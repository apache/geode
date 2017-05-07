//------------------------------------------------------------------------------
// Statistics Specifications for Cache Opens
//------------------------------------------------------------------------------

statspec cacheOpensPerSecond * cacheperf.CachePerfStats * cacheOpens
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=cacheOpens
;
statspec totalCacheOpens * cacheperf.CachePerfStats * cacheOpens
filter=none combine=combineAcrossArchives ops=max-min! trimspec=cacheOpens
;
statspec totalCacheOpenTime * cacheperf.CachePerfStats * cacheOpenTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=cacheOpens
;
statspec localRecoveryTime * cacheperf.CachePerfStats * localRecoveryTime
filter=none combine=raw ops=max trimspec=none
;
statspec remoteRecoveryTime * cacheperf.CachePerfStats * remoteRecoveryTime
filter=none combine=raw ops=max trimspec=none
;
expr cacheOpenResponseTime = totalCacheOpenTime / totalCacheOpens ops=max-min?
;
