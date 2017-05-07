//------------------------------------------------------------------------------
// Statistics Specifications for CreateKeys
//------------------------------------------------------------------------------

statspec createKeysPerSecond * cacheperf.CachePerfStats * createKeys
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=createKeys
;
statspec totalCreateKeys * cacheperf.CachePerfStats * createKeys
filter=none combine=combineAcrossArchives ops=max-min! trimspec=createKeys
;
statspec totalCreateKeyTime * cacheperf.CachePerfStats * createKeyTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=createKeys
;
expr createKeyResponseTime = totalCreateKeyTime / totalCreateKeys ops=max-min?
;
