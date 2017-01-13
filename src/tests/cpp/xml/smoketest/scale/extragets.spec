statspec extraGetsPerSecond * cacheperf.CachePerfStats * gets
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=extraGets
;
statspec totalExtraGets * cacheperf.CachePerfStats * gets
filter=none combine=combineAcrossArchives ops=max-min! trimspec=extraGets
;
statspec totalExtraGetTime * cacheperf.CachePerfStats * getTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=extraGets
;
expr extraGetResponseTime = totalExtraGetTime / totalExtraGets ops=max-min?
;
