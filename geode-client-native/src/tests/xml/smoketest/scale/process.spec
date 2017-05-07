//------------------------------------------------------------------------------
// Statistics Specifications for Solaris Processes
//------------------------------------------------------------------------------

statspec memory * VMStats * totalMemory
filter=none combine=combineAcrossArchives ops=min,max,max-min? trimspec=untrimmed
;
statspec cpu * SolarisProcessStats * cpuUsed
filter=none combine=combineAcrossArchives ops=mean? trimspec=untrimmed
;
