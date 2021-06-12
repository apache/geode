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
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"
)

type event struct {
	Class  string
	Method string
	Time   time.Time
	Status string
}

func parseProgressFile(r io.Reader) (Task, error) {
	task := make(Task)
	lines := bufio.NewScanner(r)
	for lines.Scan() {
		e, err := parseEvent(lines.Text())
		if err != nil {
			return nil, err
		}
		task, err = addEvent(task, e)
		if err != nil {
			return nil, err
		}
	}
	return task, nil
}

func parseEvent(line string) (event, error) {
	if fields := startEventRE.FindStringSubmatch(line); fields != nil {
		return newEvent(fields[1], fields[2], fields[3], "started")
	}
	if fields := endEventRE.FindStringSubmatch(line); fields != nil {
		return newEvent(fields[1], fields[2], fields[3], strings.ToLower(fields[4]))
	}
	return event{}, fmt.Errorf("unrecognized event: %s", line)
}

func addEvent(task Task, e event) (Task, error) {
	switch e.Status {
	case "started":
		if _, ok := task[e.Class]; !ok {
			task[e.Class] = make(Class)
		}
		executions := task[e.Class][e.Method]
		execution := Execution{
			Iteration: len(executions) + 1,
			StartTime: e.Time,
			Status:    e.Status,
		}
		task[e.Class][e.Method] = append(executions, execution)
	default:
		executions, ok := task[e.Class][e.Method]
		if !ok {
			return task, fmt.Errorf("%s event with no start event: %s.%s", e.Status, e.Class, e.Method)
		}
		if executions[len(executions)-1].Status != "started" {
			return task, fmt.Errorf("%s event with no running execution: %s.%s", e.Status, e.Class, e.Method)
		}
		// Apply this end event to the first unfinished execution
		for i, execution := range executions {
			if execution.Status == "started" {
				execution.EndTime = e.Time
				execution.Status = e.Status
				executions[i] = execution
				break
			}
		}
	}
	return task, nil
}

func newEvent(timeStr, class, method, status string) (event, error) {
	t, err := time.Parse("2006-01-02 15:04:05.000 -0700", timeStr)
	if err != nil {
		return event{}, err
	}
	return event{
		Class:  class,
		Method: method,
		Time:   t,
		Status: status,
	}, nil
}

var (
	startEventRE = regexp.MustCompile(`^(\S+ \S+ \S+) Starting test (\S+)\s+(.+)\s*`)
	endEventRE   = regexp.MustCompile(`^(\S+ \S+ \S+) Completed test (\S+)\s+(.+)\s+with result: (\S+)`)
)
