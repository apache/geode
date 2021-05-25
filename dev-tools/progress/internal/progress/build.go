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

import "time"

// A Build represents the executions of tests reported by a set of progress files, grouped by progress file path.
type Build map[string]Task

// A Task represents the executions of tests reported by a single progress file, grouped by class name.
type Task map[string]Class

// A Class represents the executions of tests in a class, grouped by method description (method name and parameters).
type Class map[string]Method

// A Method represents the executions of a test method with a given set of parameters.
type Method []Execution

// An Execution describes a single execution of a test. If a test is executed multiple times in a build,
// the Iteration distinguishes one execution from another. Note that progress assumes that each test result
// event corresponds to the earliest uncompleted start event for the same test description.
type Execution struct {
	Iteration int
	StartTime time.Time
	EndTime   time.Time
	Status    string
}

// Duration returns the execution's EndTime minus its StartTime.
func (e Execution) Duration() time.Duration {
	return e.EndTime.Sub(e.StartTime)
}
