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
	"testing"
	"time"
)

var (
	zeroTime  = time.Time{}
	earlyTime = time.Now().Round(time.Millisecond)
	refTime   = earlyTime.Add(time.Millisecond)
	lateTime  = refTime.Add(time.Millisecond)

	shortDur = time.Second
	refDur   = shortDur + time.Millisecond
	longDur  = refDur + time.Millisecond
)

type timeConstraintTest struct {
	constraint string
	operand    time.Time
	want       bool
	wantErr    error
}

var timeConstraintTests = map[string]timeConstraintTest{
	"early lt ref": {
		operand:    earlyTime,
		constraint: opLT + refTime.Format(TimeFormat),
		want:       true,
	},
	"ref lt ref": {
		operand:    refTime,
		constraint: opLT + refTime.Format(TimeFormat),
		want:       false,
	},
	"late lt ref": {
		operand:    lateTime,
		constraint: opLT + refTime.Format(TimeFormat),
		want:       false,
	},
	"early le ref": {
		operand:    earlyTime,
		constraint: opLE + refTime.Format(TimeFormat),
		want:       true,
	},
	"ref le ref": {
		operand:    refTime,
		constraint: opLE + refTime.Format(TimeFormat),
		want:       true,
	},
	"late le ref": {
		operand:    lateTime,
		constraint: opLE + refTime.Format(TimeFormat),
		want:       false,
	},
	"early ge ref": {
		operand:    earlyTime,
		constraint: opGE + refTime.Format(TimeFormat),
		want:       false,
	},
	"ref ge ref": {
		operand:    refTime,
		constraint: opGE + refTime.Format(TimeFormat),
		want:       true,
	},
	"late ge ref": {
		operand:    lateTime,
		constraint: opGE + refTime.Format(TimeFormat),
		want:       true,
	},
	"early gt ref": {
		operand:    earlyTime,
		constraint: opGT + refTime.Format(TimeFormat),
		want:       false,
	},
	"ref gt ref": {
		operand:    refTime,
		constraint: opGT + refTime.Format(TimeFormat),
		want:       false,
	},
	"late gt ref": {
		constraint: opGT + refTime.Format(TimeFormat),
		operand:    lateTime,
		want:       true,
	},
	// Special case: An uncompleted test's end time is the zero time. To treat this zero end time as always being
	// in the future, time constraints consider the zero time to be greater than every time.
	"zero lt ref": {
		constraint: opLT + refTime.Format(TimeFormat),
		operand:    zeroTime,
		want:       false,
	},
	"zero le ref": {
		constraint: opLE + refTime.Format(TimeFormat),
		operand:    zeroTime,
		want:       false,
	},
	"zero ge ref": {
		constraint: opGE + refTime.Format(TimeFormat),
		operand:    zeroTime,
		want:       true,
	},
	"zero gt ref": {
		constraint: opGT + refTime.Format(TimeFormat),
		operand:    zeroTime,
		want:       true,
	},
	// The zero time is greater than the zero time.
	"zero lt zero": {
		constraint: opLT + zeroTime.Format(TimeFormat),
		operand:    zeroTime,
		want:       false,
	},
	"zero le zero": {
		constraint: opLE + zeroTime.Format(TimeFormat),
		operand:    zeroTime,
		want:       false,
	},
	"zero ge zero": {
		constraint: opGE + zeroTime.Format(TimeFormat),
		operand:    zeroTime,
		want:       true,
	},
	"zero gt zero": {
		constraint: opGT + zeroTime.Format(TimeFormat),
		operand:    zeroTime,
		want:       true,
	},
}

func TestTimeConstraint(t *testing.T) {
	for name, test := range timeConstraintTests {
		t.Run(name, func(t *testing.T) {
			constraintFn, err := ParseTimeFilter(test.constraint)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("Got error %q, want %q", err, test.wantErr)
			}
			got := constraintFn(test.operand)
			if got != test.want {
				t.Errorf("Got %t for %q %q", got, test.operand, test.constraint)
			}
		})
	}
}

type durationConstraintTest struct {
	constraint string
	operand    time.Duration
	want       bool
	wantErr    error
}

var durationConstraintTests = map[string]durationConstraintTest{
	"short lt ref": {
		operand:    shortDur,
		constraint: opLT + refDur.String(),
		want:       true,
	},
	"ref lt ref": {
		operand:    refDur,
		constraint: opLT + refDur.String(),
		want:       false,
	},
	"long lt ref": {
		operand:    longDur,
		constraint: opLT + refDur.String(),
		want:       false,
	},
	"short le ref": {
		operand:    shortDur,
		constraint: opLE + refDur.String(),
		want:       true,
	},
	"ref le ref": {
		operand:    refDur,
		constraint: opLE + refDur.String(),
		want:       true,
	},
	"long le ref": {
		operand:    longDur,
		constraint: opLE + refDur.String(),
		want:       false,
	},
	"short ge ref": {
		operand:    shortDur,
		constraint: opGE + refDur.String(),
		want:       false,
	},
	"ref ge ref": {
		operand:    refDur,
		constraint: opGE + refDur.String(),
		want:       true,
	},
	"long ge ref": {
		operand:    longDur,
		constraint: opGE + refDur.String(),
		want:       true,
	},
	"short gt ref": {
		operand:    shortDur,
		constraint: opGT + refDur.String(),
		want:       false,
	},
	"ref gt ref": {
		operand:    refDur,
		constraint: opGT + refDur.String(),
		want:       false,
	},
	"long gt ref": {
		constraint: opGT + refDur.String(),
		operand:    longDur,
		want:       true,
	},
}

func TestDurationConstraint(t *testing.T) {
	for name, test := range durationConstraintTests {
		t.Run(name, func(t *testing.T) {
			constraintFn, err := ParseDurationFilter(test.constraint)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("Got error %q, want %q", err, test.wantErr)
			}
			got := constraintFn(test.operand)
			if got != test.want {
				t.Errorf("Got %t for %q %q", got, test.operand, test.constraint)
			}
		})
	}
}
