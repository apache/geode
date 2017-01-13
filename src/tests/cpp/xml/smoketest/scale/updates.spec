//------------------------------------------------------------------------------
// Statistics Specifications for Updates
//------------------------------------------------------------------------------

statspec updatesPerSecond * cacheperf.CachePerfStats * updates
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=updates
;
statspec totalUpdates * cacheperf.CachePerfStats * updates
filter=none combine=combineAcrossArchives ops=max-min! trimspec=updates
;
statspec totalUpdateTime * cacheperf.CachePerfStats * updateTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=updates
;
expr updateResponseTime = totalUpdateTime / totalUpdates ops=max-min?
;
