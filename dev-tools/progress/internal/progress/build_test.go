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
