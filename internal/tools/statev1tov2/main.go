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

package main

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/ayeshLK/websubhub/internal/state"
)

const maxRecordBytes = 16 << 20

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(arguments []string, input io.Reader, output io.Writer) error {
	if len(arguments) != 1 || (arguments[0] != "event" && arguments[0] != "snapshot") {
		return errors.New("usage: go run ./internal/tools/statev1tov2 <event|snapshot>")
	}
	body, err := io.ReadAll(io.LimitReader(input, maxRecordBytes+1))
	if err != nil {
		return errors.New("read state record")
	}
	if len(body) > maxRecordBytes {
		return errors.New("state record exceeds 16 MiB")
	}
	var migrated []byte
	if arguments[0] == "event" {
		migrated, err = state.MigrateEventV1ToV2(body)
	} else {
		migrated, err = state.MigrateSnapshotV1ToV2(body)
	}
	if err != nil {
		return err
	}
	if _, err := output.Write(append(migrated, '\n')); err != nil {
		return errors.New("write migrated state record")
	}
	return nil
}
