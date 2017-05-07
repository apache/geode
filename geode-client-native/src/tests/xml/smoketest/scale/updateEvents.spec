statspec updateEventsPerSecond * cacheperf.CachePerfStats * updateEvents
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=puts
;
statspec totalUpdateEvents * cacheperf.CachePerfStats * updateEvents
filter=none combine=combineAcrossArchives ops=max-min! trimspec=puts
;
statspec totalUpdateLatency * cacheperf.CachePerfStats * updateLatency
filter=none combine=combineAcrossArchives ops=max-min! trimspec=puts
;
expr updateLatency = totalUpdateLatency / totalUpdateEvents ops=max-min?
;
