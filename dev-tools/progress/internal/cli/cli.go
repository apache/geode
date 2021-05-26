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

package cli

import (
	"flag"
	"fmt"
	"github.com/apache/geode/dev-tools/progress/internal/progress"
	"os"
	"path/filepath"
	"regexp"
	"text/template"
	"time"
)

func Execute() int {
	flags.Usage = usage
	err := flags.Parse(os.Args[1:])
	if err == flag.ErrHelp {
		extraHelp()
		os.Exit(0)
	}
	if err != nil {
		os.Exit(1)
	}

	fs := os.DirFS(fsRoot)

	finder := progress.Finder{
		FS:          fs,
		TaskMatcher: taskMatcher,
	}

	filter := progress.Filter{
		ClassFilter:      classMatcher,
		MethodFilter:     methodMatcher,
		StatusFilter:     statusMatcher,
		StartTimeFilters: startTimeFilters,
		EndTimeFilters:   endTimeFilters,
		DurationFilters:  durationFilters,
	}

	cmd := progress.Cmd{
		FS:     fs,
		Finder: finder,
		Filter: filter,
	}

	if *reportJSON {
		cmd.Reporter = progress.JSONReporter{
			Out: os.Stdout,
		}
	} else {
		cmd.Reporter = progress.FormatReporter{
			Out:    os.Stdout,
			Format: format,
		}
	}

	err = cmd.Run(fsPaths(flags.Args()))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return 0
}

var (
	name       = filepath.Base(os.Args[0])
	synopsis   = fmt.Sprintf("Usage: %s [options] [path ...]", name)
	flags      = flag.NewFlagSet(name, flag.ContinueOnError)
	reportJSON = flags.Bool("j", false, "Describe tests as JSON")
	format     = template.Must(template.New("format").Parse(defaultFormat))

	classMatcher  = regexp.MustCompile("")
	methodMatcher = regexp.MustCompile("")
	statusMatcher = regexp.MustCompile("")
	taskMatcher   = regexp.MustCompile("")

	durationFilters  []progress.DurationFilter
	endTimeFilters   []progress.TimeFilter
	startTimeFilters []progress.TimeFilter
)

func init() {
	flags.Func("b", "Filter tests by start time `constraint`", addStartTimeFilter)
	flags.Func("c", "Filter tests by class `regexp`", setClassMatcher)
	flags.Func("d", "Filter tests by duration `constraint`", addDurationFilter)
	flags.Func("e", "Filter tests by end time `constraint`", addEndTimeFilter)
	flags.Func("f", "The `format` to describe each test", setFormat)
	flags.Func("m", "Filter tests by method `regexp`", setMethodMatcher)
	flags.Func("r", "Filter tests running at `time`", addRunningAtFilter)
	flags.Func("s", "Filter tests by status `regexp`", setStatusMatcher)
	flags.Func("t", "Limit directory search by task `regexp`", setTaskMatcher)
}

func addDurationFilter(v string) error {
	f, err := progress.ParseDurationFilter(v)
	if err != nil {
		return err
	}
	durationFilters = append(durationFilters, f)
	return nil
}

func addEndTimeFilter(v string) error {
	f, err := progress.ParseTimeFilter(v)
	if err != nil {
		return err
	}
	endTimeFilters = append(endTimeFilters, f)
	return nil
}

func addRunningAtFilter(v string) error {
	if _, err := time.Parse(progress.TimeFormat, v); err != nil {
		return err
	}
	_ = addStartTimeFilter("<=" + v)
	_ = addEndTimeFilter(">=" + v)
	return nil
}

func addStartTimeFilter(v string) error {
	f, err := progress.ParseTimeFilter(v)
	if err != nil {
		return err
	}
	startTimeFilters = append(startTimeFilters, f)
	return nil
}

func setClassMatcher(v string) error {
	re, err := regexp.Compile(v)
	if err != nil {
		return err
	}
	classMatcher = re
	return nil
}

func setFormat(v string) error {
	t, err := template.New("format").Parse(v)
	if err != nil {
		return err
	}
	format = t
	return nil
}

func setMethodMatcher(v string) error {
	re, err := regexp.Compile(v)
	if err != nil {
		return err
	}
	methodMatcher = re
	return nil
}

func setStatusMatcher(v string) error {
	re, err := regexp.Compile(v)
	if err != nil {
		return err
	}
	statusMatcher = re
	return nil
}

func setTaskMatcher(v string) error {
	re, err := regexp.Compile(v)
	if err != nil {
		return err
	}
	taskMatcher = re
	return nil
}

func usage() {
	w := flags.Output()
	fmt.Fprintln(w, synopsis)
	fmt.Fprintln(w)
	fmt.Fprintln(w, description)
	fmt.Fprintln(w)
	fmt.Fprintln(w, "OPTIONS")
	flags.PrintDefaults()
}

func extraHelp() {
	w := flags.Output()
	fmt.Fprintln(w)
	fmt.Fprint(w, details)
}

const defaultFormat = `{{ .Class }}.{{ .Method }}
    Iteration: {{ .Iteration }}
    Start:     {{ .StartTime.Format "` + progress.TimeFormat + `" }}
    End:       {{ .EndTime.Format "` + progress.TimeFormat + `" }}
    Duration:  {{ .Duration }}
    Status:    {{ .Status }}
`

const description = "Filter and describe test events recorded in Geode test progress files."

const details = `PROGRESS FILES

Progress reads test information from each regular file listed on the command
line. If the file has a .json extension, the content must have the same
structure as produced by progress -j. Otherwise, the content must be formatted
as progress events written by Geode's build process.

For each directory listed on the command line, progress reads each progress
file it detects in the file tree rooted at the directory.

The default path is the current working directory.

If you add the -t option, progress reads only those progress files whose task
names satisfy the regexp. Note that the -t option affects only how progress
searches directories. It does not filter the regular files that you list.

CAVEAT: When a build executes multiple instances of a test concurrently, the
progress file does not identify which test instance produced each test event.
Lacking this information, progress associates each test result event with the
earliest uncompleted start event with the same class name and method name.

FILTERING TESTS

By default progress describes every test it reads. If you specify one or more
filter options, progress describes only those tests that satisfy every
specified filter.

You can specify multiple time and duration filters. This is useful to specify
a range of times or durations.

The -r option takes a time string argument. The format for a time string
(using "Mon Jan 2 15:04:05 -0700 MST 2006" as an example) is:

    ` + progress.TimeFormat + `

The -b and -e options take time constraint arguments. A time constraint is a
relational operator followed by a time string. For example:

    <=2021-05-10 08:57:49.000 -0700

The -d option takes a duration constraint argument. A duration constraint is
a relational operator followed by a duration string. For example:

    >=2m

A duration string is a possibly signed sequence of decimal numbers, each with
optional fraction and a unit suffix, such as "300ms", "-1.5h" or "2h45m".
Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".

The relational operators are:

    <   less than
    <=  less than or equal to
    >=  greater than or equal to
    >   greater than

FORMATTING

By default, progress describes tests using a built in format (see below).

The -j option describes tests in JSON format, grouped by progress file path,
class name, and method name.

Use the -f option to specify a custom format, using Go's template syntax
(https://golang.org/pkg/text/template/).

For each test, progress passes the following Go struct to the template:

    type Test struct {
        File       string        // progress file
        Class      string        // test class
        Method     string        // test method, including parameters
        Iteration  int           // iteration number (for repeat tests)
        Status     string        // started, success, failure, or skipped
        StartTime  time.Time     // test start time
        EndTime    time.Time     // test end time
        Duration   time.Duration // test duration
    }

By default format is:

` + defaultFormat

const fsRoot = "/"

// fsPaths maps each path to the path relative to fsRoot
func fsPaths(paths []string) []string {
	if len(paths) == 0 {
		paths = append(paths, ".")
	}
	var fsp []string
	for _, path := range paths {
		fsp = append(fsp, fsPath(path))
	}
	return fsp
}

// fsPath maps the path to the path relative to fsRoot
func fsPath(path string) string {
	absPath, err := filepath.Abs(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	p, err := filepath.Rel(fsRoot, absPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return p
}
