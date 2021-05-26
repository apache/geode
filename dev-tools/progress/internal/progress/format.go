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
	"io"
	"text/template"
)

// testReport is the struct passed to the user-specified template to describe each Execution.
type testReport struct {
	File   string
	Class  string
	Method string
	Execution
}

// FormatReporter writes each test in a Build, formatted according to a user-specified template.
type FormatReporter struct {
	Format *template.Template
	Out    io.Writer
}

// Report writes each test in the Build to the stream, formatted according to the template.
func (r FormatReporter) Report(build Build) error {
	var test testReport
	for path, task := range build {
		test.File = path
		for className, class := range task {
			test.Class = className
			for methodName, method := range class {
				test.Method = methodName
				for _, execution := range method {
					test.Execution = execution
					if err := r.Format.Execute(r.Out, test); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}
