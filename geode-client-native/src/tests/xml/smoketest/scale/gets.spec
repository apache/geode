//------------------------------------------------------------------------------
// Statistics Specifications for Gets
//------------------------------------------------------------------------------

statspec getsPerSecond * cacheperf.CachePerfStats * gets
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=gets
;
statspec totalGets * cacheperf.CachePerfStats * gets
filter=none combine=combineAcrossArchives ops=max-min! trimspec=gets
;
statspec totalGetTime * cacheperf.CachePerfStats * getTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=gets
;
expr getResponseTime = totalGetTime / totalGets ops=max-min?
;
