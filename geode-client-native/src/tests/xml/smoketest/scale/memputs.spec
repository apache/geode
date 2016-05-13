//------------------------------------------------------------------------------
// Statistics Specifications for Memory
//------------------------------------------------------------------------------

statspec memoryLinux * LinuxProcessStats * rssSize
filter=none combine=combineAcrossArchives ops=max? trimspec=puts
;

statspec memorySolaris * SolarisProcessStats * rssSize
filter=none combine=combineAcrossArchives ops=max? trimspec=puts
;

statspec memoryWindows * WindowsProcessStats * rssSize
filter=none combine=combineAcrossArchives ops=max? trimspec=puts
;
