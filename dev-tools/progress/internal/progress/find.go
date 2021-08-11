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
	"errors"
	"io/fs"
	"path/filepath"
	"regexp"
)

type Finder struct {
	FS          fs.FS
	TaskMatcher *regexp.Regexp
}

func (f Finder) Find(paths []string) ([]string, error) {
	var progressPaths []string
	for _, path := range paths {
		info, err := fs.Stat(f.FS, path)
		if err != nil {
			return nil, err
		}
		if info.IsDir() {
			found, err := f.findInDir(path)
			if err != nil {
				return nil, err
			}
			progressPaths = append(progressPaths, found...)
			continue
		}
		progressPaths = append(progressPaths, path)
	}
	if len(progressPaths) == 0 {
		return nil, errors.New("no progress files")
	}
	return progressPaths, nil
}

func (f Finder) findInDir(dir string) ([]string, error) {
	var progFilePaths []string
	walkFn := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil // Not a dir, and so not a task dir
		}
		if filepath.Base(filepath.Dir(path)) != "build" {
			return nil // Not in a build dir, so not a task dir
		}
		taskName := filepath.Base(path)
		if !f.TaskMatcher.MatchString(taskName) {
			return nil // Task name doesn't pass filter
		}
		progFilePath := filepath.Join(path, taskName+"-progress.txt")
		progFile, err := f.FS.Open(progFilePath)
		if err != nil {
			return nil // No progress file for the task name
		}
		defer progFile.Close()
		info, err := progFile.Stat()
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil // Progress file is a dir?!
		}
		progFilePaths = append(progFilePaths, progFilePath)
		return fs.SkipDir // Found this dir's progress file, so no need to look further
	}
	if err := fs.WalkDir(f.FS, dir, walkFn); err != nil {
		return nil, err
	}
	return progFilePaths, nil
}
