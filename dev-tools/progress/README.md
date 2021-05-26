# progress

The `progress` tool reads, filters, and describes test results recorded in Geode test progress files.

For help, run

    progress -h

Contents:

- [Select Progress Files](#select-progress-files) to read, filter, and describe.
- [Filter Test Results](#filter-test-results) by a variety of criteria.
- [Describe Test Results](#describe-test-results) in a variety of formats.

## Select Progress Files

You can specify any number of directories, progress files, or JSON files for `progress` to read.

By default, `progress` reads all progress files in and below the current directory.

    progress    # Reads all progress files in and below the current directory

If you specify one or more directories, `progress` searches each to find progress files.

    progress geode-core geode-wan geode-cq

If you specify one or more JSON files, `progress` reads each as a JSON file written by `progress -j`
(see [Encode Test Results as JSON](#encode-test-results-as-json)).

    progress all-tests.json

If you specify other files, `progress` reads each as a progress file written by the Geode build process.

    progress geode-core/build/distributedTest/distributedTest-progress.txt

## Filter Test Results

You can filter test results by:

- [Status](#filter-tests-by-status)
- [Class Name](#filter-tests-by-class-name)
- [Method Description](#filter-tests-by-method-description)
- [Start and End Time](#filter-tests-by-start-and-end-time)
- [Duration](#filter-tests-by-duration)

`progress` displays every test that satisfies all filters.

You may specify any number of start time, end time, and duration filters.

### Filter Tests By Status

Each test execution has a status: `started`, `success`, `failure`, and `skipped`.

The `-s` flag applies a regular expression to each test's status:

    progress -s started
    progress -s failure
    progress -s success
    progress -s skipped
    progress -s st          # matches `started`
    progress -s f           # matches `failure`
    progress -s fail        # matches `failure`
    progress -s succ        # matches `success`
    progress -s 'fail|succ' # matches `failure` and `success`
    progress -s u           # matches `failure` and `success`
    progress -s sk          # matches `skipped`
    progress -s s           # matches `started`, `success`, and `skipped`
    progress -s ed          # matches `started` and `skipped`

### Filter Tests by Class Name

The `-c` flag applies a regular expression to each test's fully qualified class name.

    progress -c 'org.apache.geode.deployment.internal.modular.ModularJarDeploymentServiceTest'
    progress -c 'org.apache.geode.deployment.internal'
    progress -c ModularJarDeploymentServiceTest

### Filter Tests by Method Description

The `-m` flag applies a regular expression to each test's method description.

    progress -m addsListenerConcurrentlyWithNotification
    progress -m 'addsListenerConcurrentlyWithNotification\(BEFORE_BOUNCE_VM\)'
    progress -m 'addsListenerConcurrentlyWithNotification.*BOUNCE_VM'
    progress -m BEFORE_BOUNCE_VM

Note that description includes the method name, and may include the parameters passed to the method. The exact format of
the description depends on the test runner used to run the test.

### Filter Tests By Start and End Time

The `-b` flag filters tests by start time.

The `-e` flag filters tests by end time.

The `-r` flag filters tests that were running at a given time.

The `-b` and `-e` flags specify _time constraints._
A time constraint is a relational operator (`<`, `<=`, `>`, and `>=`)
followed by a reference time.

You can specify any number of `-b`, `-e`, and `-r` filters.

    progress -b '>=2021-05-24 17:35:28.031 -0700'
    progress -e '<2021-05-24 17:35:28.031 -0700'
    progress -b '>=2021-05-24 17:30:00.000 -0700' -b '<=2021-05-24 17:40:00.000 -0700'
    progress -b '>=2021-05-24 17:30:00.000 -0700' -e '<2021-05-24 17:40:00.000 -0700'
    progress -r '2021-05-24 17:35:28.031 -0700'

Note that if a test has no result, `progress` considers its "end time" to be infinitely far into the future, and
therefore greater than any reference time you specify.

### Filter Tests by Duration

The `-d` flag filters tests by duration _constraint_. A duration constraint is a relational operator (`<`, `<=`, `>`,
and `>=`) followed by a reference duration.

You can specify any number of `-d` constraints.

    progress -d '>4m34s'
    progress -d '>=4m' -d '<=4m30s'

## Describe Test Results

- [Default Format](#default-format)
- [Apply a Custom Format](#apply-a-custom-format)
- [Apply a Template File](#apply-a-template-file)
- [Encode Test Results as JSON](#encode-test-results-as-json)
- [Bundle Test Results](#bundle-test-results) for storage or copying.

### Default Format

The default format displays commonly useful information about each test:

- The test class and method/description
- The iteration number (useful for repeat tests)
- The start time, end time, and duration of the test
- The test status

Example:

```
org.apache.geode.modules.session.catalina.Tomcat7CommitSessionValveTest.recycledResponseObjectDoesNotWrapAlreadyWrappedOutputBuffer
    Iteration: 1
    Start:     2021-05-20 22:16:18.699 +0000
    End:       2021-05-20 22:16:20.585 +0000
    Duration:  1.886s
    Status:    success
org.apache.geode.modules.session.catalina.Tomcat7CommitSessionValveTest.wrappedOutputBufferForwardsToDelegate
    Iteration: 1
    Start:     2021-05-20 22:16:20.585 +0000
    End:       2021-05-20 22:16:20.589 +0000
    Duration:  4ms
    Status:    success
```

### Apply a Custom Format

The `-f` flag formats each test execution according to the format you specify,
using [Go's template syntax](https://golang.org/pkg/text/template/).

For each test execution, `progress` passes the following Go struct to your template:

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

Here are some example formats that write one line per test execution:

    progress -f '{{ println .StartTime .Class .Method }}'
    progress -f '{{ println .Class .Method }}'
    progress -f '{{ println .Status .Class .Method }}'

One-liner formats like these are useful analyzing test results in myriad ways:

    progress -f '{{ println .StartTime .Class .Method }}' | sort    # sort by start time
    progress -f '{{ println .Class .Method }}' | sort               # sort by class
    progress -f '{{ println .Status .Class .Method }}' | sort       # sort by status
    progress -s fail -f '{{ println .Class }}' | sort -u | uniq -c  # count failures by class

### Apply a Template File

For templates that you reuse often, or that are too cumbersome or complex to write on the command line, you can use your
shell's redirect feature to read the template from a file:

    progress -f "$(< my-template-file.tmpl)"

### Encode Test Results as JSON

The `-j` flag encodes test results as JSON, grouping test executions by progress file path, class name, and method
description:

    progress -j
    progress -j | jq    # Use jq to pretty-print the JSON
    progress -j -c Tomcat7CommitSessionValveTest

The output (if pretty-printed) looks like this:

```json
{
    "/path/to/geode/project/extensions/geode-modules-tomcat7/build/test/test-progress.txt": {
        "org.apache.geode.modules.session.catalina.Tomcat7CommitSessionValveTest": {
            "recycledResponseObjectDoesNotWrapAlreadyWrappedOutputBuffer": [
                {
                    "Iteration": 1,
                    "StartTime": "2021-05-20T22:16:18.699Z",
                    "EndTime": "2021-05-20T22:16:20.585Z",
                    "Status": "success"
                }
            ],
            "wrappedOutputBufferForwardsToDelegate": [
                {
                    "Iteration": 1,
                    "StartTime": "2021-05-20T22:16:20.585Z",
                    "EndTime": "2021-05-20T22:16:20.589Z",
                    "Status": "success"
                }
            ]
        }
    }
}
  ```

### Bundle Test Results

You can use `progress` to bundle a set of test results for easy storage or copying, and use `progress` to analyze the
file at a later time or on another computer:

    progress -j > all-tests.json    # Bundle all test results into a single JSON file
    ...
    progress -s fail all-tests.json # Reads the JSON file written by progress -j
