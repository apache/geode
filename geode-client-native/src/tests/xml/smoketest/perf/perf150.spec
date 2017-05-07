include $JTESTS/smoketest/perf/common.spec
;
include $JTESTS/cacheperf/specs/updateLatency.spec
;
statspec updatesPerSecond * cacheperf.CachePerfStats * updates
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=updates
;
statspec updateEventsPerSecond * cacheperf.CachePerfStats * updateEvents
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=puts
;
