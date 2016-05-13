//------------------------------------------------------------------------------
// Statistics Specifications for Updates and UpdateEvents
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

statspec updateEventsPerSecond * cacheperf.CachePerfStats * updateEvents
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=updates
;
statspec totalUpdateEvents * cacheperf.CachePerfStats * updateEvents
filter=none combine=combineAcrossArchives ops=max-min! trimspec=updates
;
statspec totalUpdateLatency * cacheperf.CachePerfStats * updateLatency
filter=none combine=combineAcrossArchives ops=max-min! trimspec=updates
;
expr updateLatency = totalUpdateLatency / totalUpdateEvents ops=max-min?
;
