include $JTESTS/smoketest/perf/common.spec
;
include $JTESTS/cacheperf/specs/updateLatency.spec
;
statspec putsPerSecond * cacheperf.CachePerfStats * puts
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=puts
;
statspec updateEventsPerSecond * cacheperf.CachePerfStats * updateEvents
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=puts
;
