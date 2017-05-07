statspec memory * VMStats * totalMemory
filter=none combine=raw ops=max trimspec=sleep
;
statspec rssSize * LinuxProcessStats * rssSize
filter=none combine=raw ops=max trimspec=sleep
;
statspec cpu * LinuxSystemStats * cpuActive
filter=none combine=raw ops=mean trimspec=sleep
;
statspec cpuMax * LinuxSystemStats * cpuActive
filter=none combine=raw ops=max trimspec=sleep
;
statspec processCpu * vmStats * processCpuTime
filter=none combine=raw ops=mean trimspec=sleep
;
statspec maxProcessCpu * vmStats * processCpuTime
filter=none combine=raw ops=max trimspec=sleep
;


