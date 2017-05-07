include $JTESTS/smoketest/perf/common.spec
;
statspec getsPerSecond * cacheperf.CachePerfStats * gets
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=gets
;
