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
	"fmt"
	"regexp"
	"strings"
	"time"
)

type TimeFilter func(time.Time) bool

type DurationFilter func(time.Duration) bool

// Filter applies a set of criteria to each Execution in a Build.
type Filter struct {
	ClassFilter      *regexp.Regexp
	MethodFilter     *regexp.Regexp
	StatusFilter     *regexp.Regexp
	StartTimeFilters []TimeFilter
	EndTimeFilters   []TimeFilter
	DurationFilters  []DurationFilter
}

// Filter returns a Build with each Execution in b that satisfies all of f's criteria.
func (f Filter) Filter(b Build) Build {
	filtered := make(Build)
	for path, task := range b {
		for className, class := range task {
			if !f.ClassFilter.MatchString(className) {
				continue
			}
			for methodName, method := range class {
				if !f.MethodFilter.MatchString(methodName) {
					continue
				}
				for _, execution := range method {
					if !f.acceptExecution(execution) {
						continue
					}
					if _, ok := filtered[path]; !ok {
						filtered[path] = make(Task)
					}
					if _, ok := filtered[path][className]; !ok {
						filtered[path][className] = make(Class)
					}
					filtered[path][className][methodName] = append(filtered[path][className][methodName], execution)
				}
			}
		}
	}
	return filtered
}

func (f Filter) acceptExecution(e Execution) bool {
	if !f.StatusFilter.MatchString(e.Status) {
		return false
	}
	for _, f := range f.StartTimeFilters {
		if !f(e.StartTime) {
			return false
		}
	}
	for _, f := range f.EndTimeFilters {
		if !f(e.EndTime) {
			return false
		}
	}
	for _, f := range f.DurationFilters {
		if !f(e.Duration()) {
			return false
		}
	}
	return true
}

var constraintRE = regexp.MustCompile(`^(\D+)(.+)`)

// TODO: Consider allowing simpler time formats:
//   Omit date (Use PROGRESS_DATE env var or infer from a date mentioned in the build).
//   Omit time zone (Use PROGRESS_TIME_ZONE env var or infer from a date mentioned in the build).
//   Omit seconds or minutes (infer 0).
// TODO: Allow RFC3339 dates to support copying/pasting dates from JSON.

const (
	TimeFormat = "2006-01-02 15:04:05.000 -0700"
	opLT       = "<"
	opLE       = "<="
	opGE       = ">="
	opGT       = ">"
)

// Each time comparison func treats its arg specially if its value is the zero time. The arg can be zero only if it
// represents an Execution's end time, and only if the progress file did not record an end event for the Execution. So
// the zero end time indicates that the Execution has not yet completed. The Execution's end time might be, so far as
// the func can tell, infinitely far into the future. To handle this case, each func considers a zero arg to be greater
// than any ref time.

// ltTime returns a func that reports whether its arg is less than ref.
func ltTime(ref time.Time) TimeFilter {
	return func(arg time.Time) bool {
		return arg.Before(ref) && !arg.IsZero()
	}
}

// leTime returns a func that reports whether its arg is less than or equal to ref.
func leTime(ref time.Time) TimeFilter {
	return func(arg time.Time) bool {
		return !arg.IsZero() && !arg.After(ref)
	}
}

// geTime returns a func that reports whether its arg is greater than or equal to ref.
func geTime(ref time.Time) TimeFilter {
	return func(arg time.Time) bool {
		return arg.IsZero() || !arg.Before(ref)
	}
}

// gtTime returns a func that reports whether its arg is greater than ref.
func gtTime(ref time.Time) TimeFilter {
	return func(arg time.Time) bool {
		return arg.IsZero() || arg.After(ref)
	}
}

var timeFilters = map[string]func(ref time.Time) TimeFilter{
	opLT: ltTime,
	opLE: leTime,
	opGE: geTime,
	opGT: gtTime,
}

func ParseTimeFilter(c string) (TimeFilter, error) {
	match := constraintRE.FindStringSubmatch(c)
	if match == nil {
		return nil, errors.New("invalid constraint")
	}
	op := strings.TrimSpace(match[1])
	filterFn, ok := timeFilters[op]
	if !ok {
		return nil, fmt.Errorf(`invalid operator: "%s"`, op)
	}
	ref, err := time.Parse(TimeFormat, strings.TrimSpace(match[2]))
	if err != nil {
		return nil, err
	}
	return filterFn(ref), err
}

var durationFilters = map[string]func(ref time.Duration) DurationFilter{
	opLT: ltDur,
	opLE: leDur,
	opGE: geDur,
	opGT: gtDur,
}

func ParseDurationFilter(c string) (DurationFilter, error) {
	match := constraintRE.FindStringSubmatch(c)
	if match == nil {
		return nil, errors.New("invalid constraint")
	}
	op := strings.TrimSpace(match[1])
	constraintFn, ok := durationFilters[op]
	if !ok {
		return nil, fmt.Errorf(`invalid operator: "%s"`, op)
	}
	ref, err := time.ParseDuration(strings.TrimSpace(match[2]))
	if err != nil {
		return nil, err
	}
	return constraintFn(ref), nil
}

func ltDur(ref time.Duration) DurationFilter {
	return func(arg time.Duration) bool {
		return arg < ref
	}
}

func leDur(ref time.Duration) DurationFilter {
	return func(arg time.Duration) bool {
		return arg <= ref
	}
}

func geDur(ref time.Duration) DurationFilter {
	return func(arg time.Duration) bool {
		return arg >= ref
	}
}

func gtDur(ref time.Duration) DurationFilter {
	return func(arg time.Duration) bool {
		return arg > ref
	}
}
