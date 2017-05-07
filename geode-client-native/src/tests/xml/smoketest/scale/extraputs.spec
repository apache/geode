statspec extraPutsPerSecond * cacheperf.CachePerfStats * puts
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=extraPuts
;
statspec totalExtraPuts * cacheperf.CachePerfStats * puts
filter=none combine=combineAcrossArchives ops=max-min! trimspec=extraPuts
;
statspec totalExtraPutTime * cacheperf.CachePerfStats * putTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=extraPuts
;
expr extraPutResponseTime = totalExtraPutTime / totalExtraPuts ops=max-min?
;
