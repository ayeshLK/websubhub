// Command licensecheck enforces the repository's dependency license policy.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type module struct {
	Path string
	Dir  string
	Main bool
}

var allowedMarkers = map[string]string{
	"Apache-2.0":   "Apache License\n                           Version 2.0",
	"BSD-2-Clause": "Redistribution and use in source and binary forms, with or without",
	"BSD-3-Clause": "Neither the name of",
	"ISC":          "Permission to use, copy, modify, and/or distribute this software",
	"MIT":          "Permission is hereby granted, free of charge, to any person obtaining a copy",
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	cmd := exec.Command("go", "list", "-m", "-json", "all")
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("list modules: %w", err)
	}

	decoder := json.NewDecoder(bytes.NewReader(output))
	checked := 0
	for decoder.More() {
		var mod module
		if err := decoder.Decode(&mod); err != nil {
			return fmt.Errorf("decode module list: %w", err)
		}
		if mod.Main {
			continue
		}
		license, err := identifyLicense(mod.Dir)
		if err != nil {
			return fmt.Errorf("dependency %s: %w", mod.Path, err)
		}
		fmt.Printf("%s: %s\n", mod.Path, license)
		checked++
	}
	fmt.Printf("checked %d dependency licenses\n", checked)
	return nil
}

func identifyLicense(dir string) (string, error) {
	names := []string{"LICENSE", "LICENSE.txt", "LICENSE.md", "COPYING", "COPYING.txt"}
	var contents []byte
	for _, name := range names {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err == nil {
			contents = append(contents, data...)
			contents = append(contents, '\n')
			continue
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
	}
	if len(contents) == 0 {
		return "", errors.New("no root license file found")
	}

	text := string(contents)
	for license, marker := range allowedMarkers {
		if strings.Contains(text, marker) {
			return license, nil
		}
	}
	return "", errors.New("license is not in the allowlist")
}
