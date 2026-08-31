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

package state

import "testing"

func TestNormalizeTopicContentType(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{name: "default", want: "application/json"},
		{name: "canonical media type", value: "Application/JSON", want: "application/json"},
		{name: "parameters", value: `application/json; profile="https://example.test/p"; charset=utf-8`, want: `application/json; charset=utf-8; profile="https://example.test/p"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := NormalizeTopicContentType(test.value)
			if err != nil || got != test.want {
				t.Fatalf("normalize %q = %q, %v; want %q", test.value, got, err, test.want)
			}
		})
	}
	if _, err := NormalizeTopicContentType("not a media type"); err == nil {
		t.Fatal("invalid content type accepted")
	}
}
