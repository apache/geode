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
	"fmt"
	"io/fs"
	"os"
	"strings"
)

// A finder identifies the progress file paths represented by a list of file and directory paths.
type finder interface {
	Find(paths []string) ([]string, error)
}

// A filter applies a set of criteria to each Execution in a Build.
type filter interface {
	Filter(b Build) Build
}

// A reporter writes an encoding of a Build to an output stream.
type reporter interface {
	Report(b Build) error
}

type Cmd struct {
	FS       fs.FS
	Finder   finder
	Filter   filter
	Reporter reporter
}

func (c Cmd) Run(paths []string) error {
	filePaths, err := c.Finder.Find(paths)
	if err != nil {
		return err
	}
	build := c.parseFiles(filePaths)
	filtered := c.Filter.Filter(build)
	if err := c.Reporter.Report(filtered); err != nil {
		return err
	}
	return nil
}

func (c Cmd) parseFiles(filePaths []string) Build {
	merged := make(Build)
	for _, path := range filePaths {
		build, err := c.parseFile(path)
		if err != nil {
			fmt.Fprintf(os.Stdout, "%s: %s\n", path, err)
			continue
		}
		for path, task := range build {
			merged[path] = task
		}
	}
	return merged
}

func (c Cmd) parseFile(path string) (Build, error) {
	r, err := c.FS.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = r.Close() }()
	if strings.HasSuffix(path, ".json") {
		return parseJSONBuild(r)
	}
	task, err := parseProgressFile(r)
	if err != nil {
		return nil, err
	}
	return Build{"/" + path: task}, nil
}
