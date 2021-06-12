/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package progress

import (
	"regexp"
	"testing"
	"testing/fstest"
)

type progressFileFinderTest struct {
	files []string
	want  []string
}

var progressFileFinderTests = map[string]progressFileFinderTest{
	"build dir + task dir + task progress file": {
		files: []string{
			"dir/build/distributedTestTest/distributedTestTest-progress.txt",
			"dir/build/integrationTest/integrationTest-progress.txt",
			"dir/build/repeatUpgradeTest/repeatUpgradeTest-progress.txt",
		},
		want: []string{
			"dir/build/distributedTestTest/distributedTestTest-progress.txt",
			"dir/build/integrationTest/integrationTest-progress.txt",
			"dir/build/repeatUpgradeTest/repeatUpgradeTest-progress.txt",
		},
	},
	"file name does not end in -progress.txt": {
		files: []string{"dir/build/distributedTest/not-a-progress-file.txt"},
		want:  []string{},
	},
	"file name prefix does match dir name": {
		files: []string{"dir/build/distributedTest/integrationTest-progress.txt"},
		want:  []string{},
	},
	"progress file does not have build grandparent": {
		files: []string{"dir/integrationTest/integrationTest-progress.txt"},
		want:  []string{},
	},
}

func TestProgressFileFinder(t *testing.T) {
	for name, test := range progressFileFinderTests {
		t.Run(name, func(t *testing.T) {
			fsys := make(fstest.MapFS)
			for _, path := range test.files {
				fsys[path] = &fstest.MapFile{}
			}
			finder := Finder{FS: fsys, TaskMatcher: regexp.MustCompile("")}
			got, err := finder.findInDir(".")
			if err != nil {
				t.Error(err)
			}
			d := diff(test.want, got)
			if len(d.missing) > 0 {
				t.Errorf("Did not find: %s", d.missing)
			}
			if len(d.extra) > 0 {
				t.Errorf("Found extra: %s", d.extra)
			}
		})
	}
}

type differences struct {
	missing []string
	extra   []string
}

func (d differences) IsEmpty() bool {
	return len(d.missing) == 0 && len(d.extra) == 0
}

func diff(want, got []string) differences {
	var d differences
wants:
	for _, w := range want {
		for _, g := range got {
			if g == w {
				continue wants
			}
		}
		d.missing = append(d.missing, w)
	}
gots:
	for _, g := range got {
		for _, w := range want {
			if w == g {
				continue gots
			}
		}
		d.extra = append(d.extra, g)
	}
	return d
}
