// Copyright 2026 Ayesh Almeida
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Command doclinks checks local links in Markdown documents.
package main

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var markdownLink = regexp.MustCompile(`!?\[[^]]*\]\(([^)]+)\)`)

func main() {
	if err := run("."); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(root string) error {
	var failures []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() && (entry.Name() == ".git" || entry.Name() == "dist") {
			return filepath.SkipDir
		}
		if entry.IsDir() || filepath.Ext(path) != ".md" {
			return nil
		}
		missing, err := missingLinks(path)
		if err != nil {
			return err
		}
		failures = append(failures, missing...)
		return nil
	})
	if err != nil {
		return err
	}
	if len(failures) != 0 {
		return errors.New("broken local Markdown links:\n" + strings.Join(failures, "\n"))
	}
	fmt.Println("all local Markdown links resolve")
	return nil
}

func missingLinks(document string) ([]string, error) {
	file, err := os.Open(document)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var failures []string
	scanner := bufio.NewScanner(file)
	line := 0
	for scanner.Scan() {
		line++
		for _, match := range markdownLink.FindAllStringSubmatch(scanner.Text(), -1) {
			target := strings.TrimSpace(strings.Trim(match[1], "<>"))
			if target == "" || strings.HasPrefix(target, "#") || isRemote(target) {
				continue
			}
			if fields := strings.Fields(target); len(fields) > 0 {
				target = fields[0]
			}
			target = strings.SplitN(target, "#", 2)[0]
			decoded, err := url.PathUnescape(target)
			if err != nil {
				failures = append(failures, fmt.Sprintf("%s:%d invalid URL escape %q", document, line, target))
				continue
			}
			if filepath.IsAbs(decoded) {
				failures = append(failures, fmt.Sprintf("%s:%d absolute local path %q", document, line, decoded))
				continue
			}
			resolved := filepath.Join(filepath.Dir(document), filepath.FromSlash(decoded))
			if _, err := os.Stat(resolved); errors.Is(err, os.ErrNotExist) {
				failures = append(failures, fmt.Sprintf("%s:%d missing %q", document, line, decoded))
			} else if err != nil {
				return nil, err
			}
		}
	}
	return failures, scanner.Err()
}

func isRemote(target string) bool {
	lower := strings.ToLower(target)
	return strings.HasPrefix(lower, "http://") ||
		strings.HasPrefix(lower, "https://") ||
		strings.HasPrefix(lower, "mailto:")
}
