include $JTESTS/smoketest/perf/common.spec
;
statspec queriesPerSecond * cacheperf.CachePerfStats * queries
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=queries
;
