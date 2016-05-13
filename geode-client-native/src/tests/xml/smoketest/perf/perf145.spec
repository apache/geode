include $JTESTS/smoketest/perf/common.spec
;
statspec updatesPerSecond * cacheperf.CachePerfStats * updates
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=updates
;
