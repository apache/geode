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
	"testing"
	"time"
)

func TestExecution_Duration(t *testing.T) {
	now := time.Now()
	shortDuration := 1 * time.Millisecond
	longDuration := 27 * time.Hour

	tests := map[string]struct {
		start time.Time
		end   time.Time
		want  time.Duration
	}{
		"zero duration": {
			start: now,
			end:   now,
			want:  0,
		},
		"short duration": {
			start: now,
			end:   now.Add(shortDuration),
			want:  shortDuration,
		},
		"long duration": {
			start: now,
			end:   now.Add(longDuration),
			want:  longDuration,
		},
		"duration is zero if end time is zero": {
			start: now,
			end:   time.Time{},
			want:  0,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			execution := Execution{
				StartTime: test.start,
				EndTime:   test.end,
			}
			if got := execution.Duration(); got != test.want {
				t.Errorf("Got duration %s, want %s", got, test.want)
			}
		})
	}
}
