/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.management.internal.beans;

import static org.apache.geode.internal.lang.SystemUtils.getLineSeparator;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.JMRuntimeException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.logging.log4j.Logger;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.internal.CommandProcessor;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.distributed.internal.locks.DLockStats;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.DirectoryHolder;
import org.apache.geode.internal.cache.DiskDirectoryStats;
import org.apache.geode.internal.cache.DiskRegion;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.DiskStoreStats;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionStats;
import org.apache.geode.internal.cache.control.ResourceManagerStats;
import org.apache.geode.internal.cache.execute.metrics.FunctionServiceStats;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.offheap.MemoryAllocator;
import org.apache.geode.internal.offheap.OffHeapMemoryStats;
import org.apache.geode.internal.process.PidUnavailableException;
import org.apache.geode.internal.process.ProcessUtils;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.internal.statistics.OsStatisticsProvider;
import org.apache.geode.internal.statistics.StatSamplerStats;
import org.apache.geode.internal.statistics.StatisticsManager;
import org.apache.geode.internal.statistics.VMStatsContract;
import org.apache.geode.internal.statistics.platform.LinuxSystemStats;
import org.apache.geode.internal.statistics.platform.ProcessStats;
import org.apache.geode.internal.stats50.VMStats50;
import org.apache.geode.internal.tcp.ConnectionTable;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.logging.internal.spi.LogFile;
import org.apache.geode.management.GemFireProperties;
import org.apache.geode.management.JVMMetrics;
import org.apache.geode.management.OSMetrics;
import org.apache.geode.management.internal.ManagementConstants;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.beans.stats.AggregateRegionStatsMonitor;
import org.apache.geode.management.internal.beans.stats.GCStatsMonitor;
import org.apache.geode.management.internal.beans.stats.MBeanStatsMonitor;
import org.apache.geode.management.internal.beans.stats.MemberLevelDiskMonitor;
import org.apache.geode.management.internal.beans.stats.StatType;
import org.apache.geode.management.internal.beans.stats.StatsAverageLatency;
import org.apache.geode.management.internal.beans.stats.StatsKey;
import org.apache.geode.management.internal.beans.stats.StatsLatency;
import org.apache.geode.management.internal.beans.stats.StatsRate;
import org.apache.geode.management.internal.beans.stats.VMStatsMonitor;

/**
 * This class acts as an Bridge between MemberMBean and GemFire Cache and Distributed System
 */
public class MemberMBeanBridge {

  private static final Logger logger = LogService.getLogger();
  private static final String MEMBER_LEVEL_DISK_MONITOR = "MemberLevelDiskMonitor";
  private static final String MEMBER_LEVEL_REGION_MONITOR = "MemberLevelRegionMonitor";
  private static final long MBFactor = 1024 * 1024;

  private final OsStatisticsProvider osStatisticsProvider = OsStatisticsProvider.build();

  private InternalCache cache;
  private DistributionConfig config;

  private GemFireProperties gemFirePropertyData;
  private final InternalDistributedSystem system;
  private final StatisticsManager statisticsManager;
  private CommandProcessor commandProcessor;

  private String commandServiceInitError;
  private MemoryMXBean memoryMXBean;
  private ThreadMXBean threadMXBean;
  private OperatingSystemMXBean osBean;
  private String hostname;
  private int processId;
  private ObjectName osObjectName;
  private final MBeanStatsMonitor monitor;
  private SystemManagementService service;
  private final MemberLevelDiskMonitor diskMonitor;
  private final AggregateRegionStatsMonitor regionMonitor;
  private StatsRate createsRate;
  private StatsRate bytesReceivedRate;
  private StatsRate bytesSentRate;
  private StatsRate destroysRate;
  private StatsRate functionExecutionRate;
  private StatsRate getsRate;
  private StatsRate putAllRate;
  private StatsRate putsRate;
  private StatsRate transactionCommitsRate;
  private StatsRate diskReadsRate;
  private StatsRate diskWritesRate;
  private StatsAverageLatency listenerCallsAvgLatency;
  private StatsAverageLatency writerCallsAvgLatency;
  private StatsAverageLatency putsAvgLatency;
  private StatsAverageLatency getsAvgLatency;
  private StatsAverageLatency putAllAvgLatency;
  private StatsAverageLatency loadsAverageLatency;
  private StatsAverageLatency netLoadsAverageLatency;
  private StatsAverageLatency netSearchAverageLatency;
  private StatsAverageLatency transactionCommitsAvgLatency;
  private StatsAverageLatency diskFlushAvgLatency;
  private StatsAverageLatency deserializationAvgLatency;
  private StatsLatency deserializationLatency;
  private StatsRate deserializationRate;
  private StatsAverageLatency serializationAvgLatency;
  private StatsLatency serializationLatency;
  private StatsRate serializationRate;
  private StatsAverageLatency pdxDeserializationAvgLatency;
  private StatsRate pdxDeserializationRate;
  private StatsRate lruDestroyRate;
  private StatsRate lruEvictionRate;
  private String gemFireVersion;
  private String classPath;
  private String name;
  private String id;
  private final String osName = System.getProperty("os.name", "unknown");
  private final GCStatsMonitor gcMonitor;
  private final VMStatsMonitor vmStatsMonitor;
  private final MBeanStatsMonitor systemStatsMonitor;
  private float instCreatesRate;
  private float instGetsRate;
  private float instPutsRate;
  private float instPutAllRate;
  private Statistics systemStat;
  private boolean cacheServer;
  private String redundancyZone = "";
  private ResourceManagerStats resourceManagerStats;

  private volatile boolean lockServicesStatsAdded;

  MemberMBeanBridge(InternalCache cache, SystemManagementService service) {
    this.cache = cache;
    this.service = service;

    system = (InternalDistributedSystem) cache.getDistributedSystem();
    statisticsManager = system.getStatisticsManager();

    redundancyZone = system.getDistributionManager()
        .getRedundancyZone(cache.getInternalDistributedSystem().getDistributedMember());

    config = system.getConfig();
    try {
      commandProcessor = cache.getService(CommandProcessor.class);
    } catch (Exception e) {
      commandServiceInitError = e.getMessage();
      logger.info(LogMarker.CONFIG_MARKER, "Command processor could not be initialized. {}",
          e.getMessage());
    }

    initGemfireProperties();

    try {
      hostname = LocalHostUtil.getLocalHostName();
    } catch (UnknownHostException ignore) {
      hostname = ManagementConstants.DEFAULT_HOST_NAME;
    }

    try {
      osObjectName = new ObjectName("java.lang:type=OperatingSystem");
    } catch (MalformedObjectNameException | NullPointerException ex) {
      if (logger.isDebugEnabled()) {
        logger.debug(ex.getMessage(), ex);
      }
    }

    memoryMXBean = ManagementFactory.getMemoryMXBean();

    threadMXBean = ManagementFactory.getThreadMXBean();

    osBean = ManagementFactory.getOperatingSystemMXBean();

    // Initialize all the Stats Monitors
    monitor = new MBeanStatsMonitor("MemberMXBeanMonitor");
    diskMonitor = new MemberLevelDiskMonitor(MEMBER_LEVEL_DISK_MONITOR);
    regionMonitor = new AggregateRegionStatsMonitor(MEMBER_LEVEL_REGION_MONITOR);
    gcMonitor = new GCStatsMonitor("GCStatsMonitor");
    vmStatsMonitor = new VMStatsMonitor("VMStatsMonitor");

    systemStatsMonitor = new MBeanStatsMonitor("SystemStatsManager");

    // Initialize Process related information

    gemFireVersion = GemFireVersion.asString();
    classPath = ManagementFactory.getRuntimeMXBean().getClassPath();
    name = cache.getDistributedSystem().getDistributedMember().getName();
    id = cache.getDistributedSystem().getDistributedMember().getId();

    try {
      processId = ProcessUtils.identifyPid();
    } catch (PidUnavailableException ex) {
      if (logger.isDebugEnabled()) {
        logger.debug(ex.getMessage(), ex);
      }
    }

    FunctionService.registerFunction(new QueryDataFunction());

    resourceManagerStats = cache.getInternalResourceManager().getStats();
  }

  @VisibleForTesting
  public MemberMBeanBridge(InternalDistributedSystem system, StatisticsManager statisticsManager) {
    monitor = new MBeanStatsMonitor("MemberMXBeanMonitor");
    diskMonitor = new MemberLevelDiskMonitor(MEMBER_LEVEL_DISK_MONITOR);
    regionMonitor = new AggregateRegionStatsMonitor(MEMBER_LEVEL_REGION_MONITOR);
    gcMonitor = new GCStatsMonitor("GCStatsMonitor");
    vmStatsMonitor = new VMStatsMonitor("VMStatsMonitor");
    systemStatsMonitor = new MBeanStatsMonitor("SystemStatsManager");

    this.system = system;
    this.statisticsManager = statisticsManager;

    initializeStats();
  }

  MemberMBeanBridge init() {
    CachePerfStats cachePerfStats = cache.getCachePerfStats();
    addCacheStats(cachePerfStats);
    addFunctionStats(system.getFunctionStatsManager().getFunctionServiceStats());

    if (system.getDistributionManager().getStats() instanceof DistributionStats) {
      DistributionStats distributionStats =
          (DistributionStats) system.getDistributionManager().getStats();
      addDistributionStats(distributionStats);
    }

    systemStat = fetchSystemStats();

    MemoryAllocator allocator = cache.getOffHeapStore();
    if (null != allocator) {
      OffHeapMemoryStats offHeapStats = allocator.getStats();

      if (null != offHeapStats) {
        addOffHeapStats(offHeapStats);
      }
    }

    addProcessStats(fetchProcessStats());
    addStatSamplerStats(fetchStatSamplerStats());
    addVMStats(fetchVMStats());
    initializeStats();

    return this;
  }

  private Statistics fetchSystemStats() {
    if (osStatisticsProvider.osStatsSupported()) {
      Statistics[] systemStats = system.findStatisticsByType(LinuxSystemStats.getType());

      if (systemStats != null) {
        return systemStats[0];
      }
    }
    return null;
  }

  private void addOffHeapStats(OffHeapMemoryStats offHeapStats) {
    Statistics offHeapMemoryStatistics = offHeapStats.getStats();
    monitor.addStatisticsToMonitor(offHeapMemoryStatistics);
  }

  @VisibleForTesting
  public void addCacheStats(CachePerfStats cachePerfStats) {
    Statistics cachePerfStatistics = cachePerfStats.getStats();
    monitor.addStatisticsToMonitor(cachePerfStatistics);
  }

  @VisibleForTesting
  public void addFunctionStats(FunctionServiceStats functionServiceStats) {
    Statistics functionStatistics = functionServiceStats.getStats();
    monitor.addStatisticsToMonitor(functionStatistics);
  }

  @VisibleForTesting
  public void addDistributionStats(DistributionStats distributionStats) {
    Statistics dsStats = distributionStats.getStats();
    monitor.addStatisticsToMonitor(dsStats);
  }

  void addDiskStore(DiskStore dsi) {
    DiskStoreImpl impl = (DiskStoreImpl) dsi;
    addDiskStoreStats(impl.getStats());
  }

  @VisibleForTesting
  public void addDiskStoreStats(DiskStoreStats stats) {
    diskMonitor.addStatisticsToMonitor(stats.getStats());
  }

  void removeDiskStore(DiskStore dsi) {
    DiskStoreImpl impl = (DiskStoreImpl) dsi;
    removeDiskStoreStats(impl.getStats());
  }

  private void removeDiskStoreStats(DiskStoreStats stats) {
    diskMonitor.removeStatisticsFromMonitor(stats.getStats());
  }

  void addRegion(Region region) {
    if (region.getAttributes().getPartitionAttributes() != null) {
      addPartitionedRegionStats(((PartitionedRegion) region).getPrStats());
    }

    InternalRegion internalRegion = (InternalRegion) region;
    addLRUStats(internalRegion.getEvictionStatistics());
    DiskRegion dr = internalRegion.getDiskRegion();
    if (dr != null) {
      for (DirectoryHolder dh : dr.getDirectories()) {
        addDirectoryStats(dh.getDiskDirectoryStats());
      }
    }
  }

  @VisibleForTesting
  public void addPartitionedRegionStats(PartitionedRegionStats parStats) {
    regionMonitor.addStatisticsToMonitor(parStats.getStats());
  }

  private void addLRUStats(Statistics lruStats) {
    if (lruStats != null) {
      regionMonitor.addStatisticsToMonitor(lruStats);
    }
  }

  private void addDirectoryStats(DiskDirectoryStats diskDirStats) {
    regionMonitor.addStatisticsToMonitor(diskDirStats.getStats());
  }

  void removeRegion(Region region) {
    if (region.getAttributes().getPartitionAttributes() != null) {
      removePartitionedRegionStats(((PartitionedRegion) region).getPrStats());
    }

    InternalRegion internalRegion = (InternalRegion) region;
    removeLRUStats(internalRegion.getEvictionStatistics());

    DiskRegion diskRegion = internalRegion.getDiskRegion();
    if (diskRegion != null) {
      for (DirectoryHolder directoryHolder : diskRegion.getDirectories()) {
        removeDirectoryStats(directoryHolder.getDiskDirectoryStats());
      }
    }
  }

  @VisibleForTesting
  public void removePartitionedRegionStats(PartitionedRegionStats parStats) {
    regionMonitor.removePartitionStatistics(parStats.getStats());
  }

  private void removeLRUStats(Statistics statistics) {
    if (statistics != null) {
      regionMonitor.removeLRUStatistics(statistics);
    }
  }

  private void removeDirectoryStats(DiskDirectoryStats diskDirStats) {
    regionMonitor.removeDirectoryStatistics(diskDirStats.getStats());
  }

  void addLockServiceStats(DLockService lock) {
    if (!lockServicesStatsAdded) {
      DLockStats stats = (DLockStats) lock.getStats();
      addLockServiceStats(stats);
      lockServicesStatsAdded = true;
    }
  }

  @VisibleForTesting
  public void addLockServiceStats(DLockStats stats) {
    monitor.addStatisticsToMonitor(stats.getStats());
  }

  private ProcessStats fetchProcessStats() {
    return system.getStatSampler().getProcessStats();
  }

  private StatSamplerStats fetchStatSamplerStats() {
    return system.getStatSampler().getStatSamplerStats();
  }

  @VisibleForTesting
  public void addProcessStats(ProcessStats processStats) {
    if (processStats != null) {
      systemStatsMonitor.addStatisticsToMonitor(processStats.getStatistics());
    }
  }

  @VisibleForTesting
  public void addStatSamplerStats(StatSamplerStats statSamplerStats) {
    if (statSamplerStats != null) {
      systemStatsMonitor.addStatisticsToMonitor(statSamplerStats.getStats());
    }
  }

  private VMStatsContract fetchVMStats() {
    return system.getStatSampler().getVMStats();
  }

  @VisibleForTesting
  public void addVMStats(VMStatsContract vmStatsContract) {
    if (vmStatsContract instanceof VMStats50) {
      VMStats50 vmStats50 = (VMStats50) vmStatsContract;
      Statistics vmStats = vmStats50.getVMStats();
      if (vmStats != null) {
        vmStatsMonitor.addStatisticsToMonitor(vmStats);
      }

      Statistics vmHeapStats = vmStats50.getVMHeapStats();
      if (vmHeapStats != null) {
        vmStatsMonitor.addStatisticsToMonitor(vmHeapStats);
      }

      StatisticsType gcType = VMStats50.getGCType();
      if (gcType != null) {
        Statistics[] gcStats = statisticsManager.findStatisticsByType(gcType);
        if (gcStats != null && gcStats.length > 0) {
          for (Statistics gcStat : gcStats) {
            if (gcStat != null) {
              gcMonitor.addStatisticsToMonitor(gcStat);
            }
          }
        }
      }
    }
  }

  private Number getMemberLevelStatistic(String statName) {
    return monitor.getStatistic(statName);
  }

  private Number getVMStatistic(String statName) {
    return vmStatsMonitor.getStatistic(statName);
  }

  private Number getGCStatistic(String statName) {
    return gcMonitor.getStatistic(statName);
  }

  private Number getSystemStatistic(String statName) {
    return systemStatsMonitor.getStatistic(statName);
  }

  void stopMonitor() {
    monitor.stopListener();
    regionMonitor.stopListener();
    gcMonitor.stopListener();
    systemStatsMonitor.stopListener();
    vmStatsMonitor.stopListener();
  }

  private void initializeStats() {
    createsRate = new StatsRate(StatsKey.CREATES, StatType.INT_TYPE, monitor);
    bytesReceivedRate = new StatsRate(StatsKey.RECEIVED_BYTES, StatType.LONG_TYPE, monitor);
    bytesSentRate = new StatsRate(StatsKey.SENT_BYTES, StatType.LONG_TYPE, monitor);
    destroysRate = new StatsRate(StatsKey.DESTROYS, StatType.INT_TYPE, monitor);

    functionExecutionRate =
        new StatsRate(StatsKey.FUNCTION_EXECUTIONS_COMPLETED, StatType.INT_TYPE, monitor);

    getsRate = new StatsRate(StatsKey.GETS, StatType.INT_TYPE, monitor);

    putAllRate = new StatsRate(StatsKey.PUT_ALLS, StatType.INT_TYPE, monitor);

    putsRate = new StatsRate(StatsKey.PUTS, StatType.INT_TYPE, monitor);

    transactionCommitsRate =
        new StatsRate(StatsKey.TRANSACTION_COMMITS, StatType.INT_TYPE, monitor);

    diskReadsRate = new StatsRate(StatsKey.DISK_READ_BYTES, StatType.LONG_TYPE, diskMonitor);

    diskWritesRate = new StatsRate(StatsKey.DISK_WRITEN_BYTES, StatType.LONG_TYPE, diskMonitor);

    listenerCallsAvgLatency = new StatsAverageLatency(StatsKey.CACHE_LISTENER_CALLS_COMPLETED,
        StatType.INT_TYPE, StatsKey.CACHE_LISTENR_CALL_TIME, monitor);

    writerCallsAvgLatency = new StatsAverageLatency(StatsKey.CACHE_WRITER_CALLS_COMPLETED,
        StatType.INT_TYPE, StatsKey.CACHE_WRITER_CALL_TIME, monitor);

    getsAvgLatency =
        new StatsAverageLatency(StatsKey.GETS, StatType.INT_TYPE, StatsKey.GET_TIME, monitor);

    putAllAvgLatency = new StatsAverageLatency(StatsKey.PUT_ALLS, StatType.INT_TYPE,
        StatsKey.PUT_ALL_TIME, monitor);

    putsAvgLatency =
        new StatsAverageLatency(StatsKey.PUTS, StatType.INT_TYPE, StatsKey.PUT_TIME, monitor);

    loadsAverageLatency = new StatsAverageLatency(StatsKey.LOADS_COMPLETED, StatType.INT_TYPE,
        StatsKey.LOADS_TIME, monitor);

    netLoadsAverageLatency = new StatsAverageLatency(StatsKey.NET_LOADS_COMPLETED,
        StatType.INT_TYPE, StatsKey.NET_LOADS_TIME, monitor);

    netSearchAverageLatency = new StatsAverageLatency(StatsKey.NET_SEARCH_COMPLETED,
        StatType.INT_TYPE, StatsKey.NET_SEARCH_TIME, monitor);

    transactionCommitsAvgLatency = new StatsAverageLatency(StatsKey.TRANSACTION_COMMITS,
        StatType.INT_TYPE, StatsKey.TRANSACTION_COMMIT_TIME, monitor);

    diskFlushAvgLatency = new StatsAverageLatency(StatsKey.NUM_FLUSHES, StatType.INT_TYPE,
        StatsKey.TOTAL_FLUSH_TIME, diskMonitor);

    deserializationAvgLatency = new StatsAverageLatency(StatsKey.DESERIALIZATIONS,
        StatType.INT_TYPE, StatsKey.DESERIALIZATION_TIME, monitor);

    deserializationLatency = new StatsLatency(StatsKey.DESERIALIZATIONS, StatType.INT_TYPE,
        StatsKey.DESERIALIZATION_TIME, monitor);

    deserializationRate = new StatsRate(StatsKey.DESERIALIZATIONS, StatType.INT_TYPE, monitor);

    serializationAvgLatency = new StatsAverageLatency(StatsKey.SERIALIZATIONS, StatType.INT_TYPE,
        StatsKey.SERIALIZATION_TIME, monitor);

    serializationLatency = new StatsLatency(StatsKey.SERIALIZATIONS, StatType.INT_TYPE,
        StatsKey.SERIALIZATION_TIME, monitor);

    serializationRate = new StatsRate(StatsKey.SERIALIZATIONS, StatType.INT_TYPE, monitor);

    pdxDeserializationAvgLatency = new StatsAverageLatency(StatsKey.PDX_INSTANCE_DESERIALIZATIONS,
        StatType.INT_TYPE, StatsKey.PDX_INSTANCE_DESERIALIZATION_TIME, monitor);

    pdxDeserializationRate =
        new StatsRate(StatsKey.PDX_INSTANCE_DESERIALIZATIONS, StatType.INT_TYPE, monitor);

    lruDestroyRate = new StatsRate(StatsKey.LRU_DESTROYS, StatType.LONG_TYPE, regionMonitor);

    lruEvictionRate = new StatsRate(StatsKey.LRU_EVICTIONS, StatType.LONG_TYPE, regionMonitor);
  }

  private void initGemfireProperties() {
    if (gemFirePropertyData == null) {
      gemFirePropertyData = BeanUtilFuncs.initGemfireProperties(config);
    }
  }

  JVMMetrics fetchJVMMetrics() {
    long gcCount = getGCStatistic(StatsKey.VM_GC_STATS_COLLECTIONS).longValue();
    long gcTimeMillis = getGCStatistic(StatsKey.VM_GC_STATS_COLLECTION_TIME).longValue();

    long initMemory = memoryMXBean.getHeapMemoryUsage().getInit();
    long committedMemory = memoryMXBean.getHeapMemoryUsage().getCommitted();
    long usedMemory = getVMStatistic(StatsKey.VM_USED_MEMORY).longValue();
    long maxMemory = memoryMXBean.getHeapMemoryUsage().getMax();

    int totalThreads = getVMStatistic(StatsKey.VM_STATS_NUM_THREADS).intValue();

    return new JVMMetrics(gcCount, gcTimeMillis, initMemory, committedMemory, usedMemory, maxMemory,
        totalThreads);
  }

  /**
   * All OS metrics are not present in java.lang.management.OperatingSystemMXBean It has to be cast
   * to com.sun.management.OperatingSystemMXBean. To avoid the cast using dynamic call so that Java
   * platform will take care of the details in a native manner;
   */
  OSMetrics fetchOSMetrics() {
    try {
      String name = osBean.getName();
      String version = osBean.getVersion();
      String arch = osBean.getArch();
      int availableProcessors = osBean.getAvailableProcessors();
      double systemLoadAverage = osBean.getSystemLoadAverage();

      long openFileDescriptorCount = getVMStatistic(StatsKey.VM_STATS_OPEN_FDS).longValue();
      long processCpuTime = getVMStatistic(StatsKey.VM_PROCESS_CPU_TIME).longValue();

      long maxFileDescriptorCount;
      try {
        maxFileDescriptorCount =
            (Long) ManagementFactory.getPlatformMBeanServer()
                .getAttribute(osObjectName, "MaxFileDescriptorCount");
      } catch (Exception ignore) {
        maxFileDescriptorCount = -1;
      }

      long committedVirtualMemorySize;
      try {
        committedVirtualMemorySize =
            (Long) ManagementFactory.getPlatformMBeanServer()
                .getAttribute(osObjectName, "CommittedVirtualMemorySize");
      } catch (Exception ignore) {
        committedVirtualMemorySize = -1;
      }

      // If Linux System type exists
      long totalPhysicalMemorySize;
      long freePhysicalMemorySize;
      long totalSwapSpaceSize;
      long freeSwapSpaceSize;
      if (osStatisticsProvider.osStatsSupported() && systemStat != null) {

        try {
          totalPhysicalMemorySize =
              systemStat.get(StatsKey.LINUX_SYSTEM_PHYSICAL_MEMORY).longValue();
        } catch (Exception ignore) {
          totalPhysicalMemorySize = -1;
        }
        try {
          freePhysicalMemorySize = systemStat.get(StatsKey.LINUX_SYSTEM_FREE_MEMORY).longValue();
        } catch (Exception ignore) {
          freePhysicalMemorySize = -1;
        }
        try {
          totalSwapSpaceSize = systemStat.get(StatsKey.LINUX_SYSTEM_TOTAL_SWAP_SIZE).longValue();
        } catch (Exception ignore) {
          totalSwapSpaceSize = -1;
        }

        try {
          freeSwapSpaceSize = systemStat.get(StatsKey.LINUX_SYSTEM_FREE_SWAP_SIZE).longValue();
        } catch (Exception ignore) {
          freeSwapSpaceSize = -1;
        }

      } else {
        totalPhysicalMemorySize = -1;
        freePhysicalMemorySize = -1;
        totalSwapSpaceSize = -1;
        freeSwapSpaceSize = -1;
      }

      return new OSMetrics(maxFileDescriptorCount, openFileDescriptorCount, processCpuTime,
          committedVirtualMemorySize, totalPhysicalMemorySize, freePhysicalMemorySize,
          totalSwapSpaceSize, freeSwapSpaceSize, name, version, arch, availableProcessors,
          systemLoadAverage);

    } catch (Exception ex) {
      if (logger.isTraceEnabled()) {
        logger.trace(ex.getMessage(), ex);
      }
    }
    return null;
  }

  GemFireProperties getGemFireProperty() {
    return gemFirePropertyData;
  }

  boolean createManager() {
    if (service.isManager()) {
      return false;
    }
    return service.createManager();
  }

  String[] compactAllDiskStores() {
    List<String> compactedStores = new ArrayList<>();

    if (cache != null && !cache.isClosed()) {
      for (DiskStore store : cache.listDiskStoresIncludingRegionOwned()) {
        if (store.forceCompaction()) {
          compactedStores.add(((DiskStoreImpl) store).getPersistentID().getDirectory());
        }
      }
    }
    String[] compactedStoresAr = new String[compactedStores.size()];
    return compactedStores.toArray(compactedStoresAr);
  }

  String[] listDiskStores(boolean includeRegionOwned) {
    Collection<DiskStore> diskCollection;
    if (includeRegionOwned) {
      diskCollection = cache.listDiskStoresIncludingRegionOwned();
    } else {
      diskCollection = cache.listDiskStores();
    }

    String[] returnString = null;
    if (diskCollection != null && !diskCollection.isEmpty()) {
      returnString = new String[diskCollection.size()];
      Iterator<DiskStore> it = diskCollection.iterator();
      int i = 0;
      while (it.hasNext()) {
        returnString[i] = it.next().getName();
        i++;
      }
    }
    return returnString;
  }

  String[] getDiskStores() {
    return listDiskStores(true);
  }

  String fetchLog(int numLines) {
    if (numLines > ManagementConstants.MAX_SHOW_LOG_LINES) {
      numLines = ManagementConstants.MAX_SHOW_LOG_LINES;
    }
    if (numLines == 0 || numLines < 0) {
      numLines = ManagementConstants.DEFAULT_SHOW_LOG_LINES;
    }
    String childTail = null;
    String mainTail;
    try {
      InternalDistributedSystem sys = system;

      if (sys.getLogFile().isPresent()) {
        LogFile logFile = sys.getLogFile().get();
        childTail = BeanUtilFuncs.tailSystemLog(logFile.getChildLogFile(), numLines);
        mainTail = BeanUtilFuncs.tailSystemLog(sys.getConfig(), numLines);
        if (mainTail == null) {
          mainTail =
              "No log file was specified in the configuration, messages will be directed to stdout.";
        }
      } else {
        throw new IllegalStateException(
            "TailLogRequest/Response processed in application vm with shared logging. This would occur if there is no 'log-file' defined.");
      }
    } catch (IOException e) {
      logger.warn("Error occurred while reading system log:", e);
      mainTail = "";
    }

    StringBuilder result = new StringBuilder();
    result.append(mainTail);
    if (childTail != null) {
      result.append(getLineSeparator())
          .append("-------------------- tail of child log --------------------")
          .append(getLineSeparator());
      result.append(childTail);
    }
    return result.toString();
  }

  /**
   * Using async thread. As remote operation will be executed by FunctionService. Might cause
   * problems in cleaning up function related resources. Aggregate bean DistributedSystemMBean will
   * have to depend on GemFire messages to decide whether all the members have been shutdown or not
   * before deciding to shut itself down
   */
  void shutDownMember() {
    if (system.isConnected()) {
      Thread t = new LoggingThread("Shutdown member", false, () -> {
        try {
          // Allow the Function call to exit
          Thread.sleep(1000);
        } catch (InterruptedException ignore) {
        }
        ConnectionTable.threadWantsSharedResources();
        if (system.isConnected()) {
          system.disconnect();
        }
      });
      t.start();
    }
  }

  String getName() {
    return name;
  }

  String getId() {
    return id;
  }

  String getMember() {
    if (name != null && !name.isEmpty()) {
      return name;
    }
    return id;
  }

  String[] getGroups() {
    List<String> groups = cache.getDistributedSystem().getDistributedMember().getGroups();
    String[] groupsArray = new String[groups.size()];
    groupsArray = groups.toArray(groupsArray);
    return groupsArray;
  }

  String getClassPath() {
    return classPath;
  }

  String[] listConnectedGatewayReceivers() {
    if (cache != null && !cache.getGatewayReceivers().isEmpty()) {
      Set<GatewayReceiver> receivers = cache.getGatewayReceivers();
      String[] receiverArray = new String[receivers.size()];
      int j = 0;
      for (GatewayReceiver recv : receivers) {
        receiverArray[j] = recv.getBindAddress();
        j++;
      }
      return receiverArray;
    }
    return ManagementConstants.NO_DATA_STRING;
  }

  String[] listConnectedGatewaySenders() {
    if (cache != null && !cache.getGatewaySenders().isEmpty()) {
      Set<GatewaySender> senders = cache.getGatewaySenders();
      String[] senderArray = new String[senders.size()];
      int j = 0;
      for (GatewaySender sender : senders) {
        senderArray[j] = sender.getId();
        j++;
      }
      return senderArray;
    }
    return ManagementConstants.NO_DATA_STRING;
  }

  float getCpuUsage() {
    return vmStatsMonitor.getCpuUsage();
  }

  long getCurrentTime() {
    return System.currentTimeMillis();
  }

  String getHost() {
    return hostname;
  }

  int getProcessId() {
    return processId;
  }

  /**
   * Gets a String describing the GemFire member's status. A GemFire member includes, but is not
   * limited to: Locators, Managers, Cache Servers and so on.
   *
   * @return String description of the GemFire member's status.
   * @see #isLocator()
   * @see #isServer()
   */
  String status() {
    if (LocatorLauncher.getInstance() != null) {
      return LocatorLauncher.getLocatorState().toJson();
    }
    if (ServerLauncher.getInstance() != null) {
      return ServerLauncher.getServerState().toJson();
    }

    return null;
  }

  String[] fetchJvmThreads() {
    long[] threadIds = threadMXBean.getAllThreadIds();
    ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadIds, 0);
    if (threadInfos == null || threadInfos.length < 1) {
      return ManagementConstants.NO_DATA_STRING;
    }
    List<String> threadList = new ArrayList<>(threadInfos.length);
    for (ThreadInfo threadInfo : threadInfos) {
      if (threadInfo != null) {
        threadList.add(threadInfo.getThreadName());
      }
    }
    String[] result = new String[threadList.size()];
    return threadList.toArray(result);
  }

  String[] getListOfRegions() {
    Set<InternalRegion> listOfAppRegions = cache.getApplicationRegions();
    if (listOfAppRegions != null && !listOfAppRegions.isEmpty()) {
      String[] regions = new String[listOfAppRegions.size()];
      int j = 0;
      for (Region region : listOfAppRegions) {
        regions[j] = region.getFullPath();
        j++;
      }
      return regions;
    }
    return ManagementConstants.NO_DATA_STRING;
  }

  long getLockLease() {
    return cache.getLockLease();
  }

  long getLockTimeout() {
    return cache.getLockTimeout();
  }

  long getMemberUpTime() {
    return cache.getUpTime();
  }

  String[] getRootRegionNames() {
    Set<Region<?, ?>> listOfRootRegions = cache.rootRegions();
    if (listOfRootRegions != null && !listOfRootRegions.isEmpty()) {
      String[] regionNames = new String[listOfRootRegions.size()];
      int j = 0;
      for (Region region : listOfRootRegions) {
        regionNames[j] = region.getFullPath();
        j++;
      }
      return regionNames;
    }
    return ManagementConstants.NO_DATA_STRING;
  }

  String getVersion() {
    return gemFireVersion;
  }

  boolean hasGatewayReceiver() {
    return cache != null && !cache.getGatewayReceivers().isEmpty();
  }

  boolean hasGatewaySender() {
    return cache != null && !cache.getAllGatewaySenders().isEmpty();
  }

  boolean isLocator() {
    return Locator.hasLocator();
  }

  boolean isManager() {
    if (cache == null || cache.isClosed()) {
      return false;
    }
    try {
      return service.isManager();
    } catch (Exception ignore) {
      return false;
    }
  }

  boolean isManagerCreated() {
    if (cache == null || cache.isClosed()) {
      return false;
    }
    try {
      return service.isManagerCreated();
    } catch (Exception ignore) {
      return false;
    }
  }

  boolean isServer() {
    return cache.isServer();
  }

  public int getInitialImageKeysReceived() {
    return getMemberLevelStatistic(StatsKey.GET_INITIAL_IMAGE_KEYS_RECEIVED).intValue();
  }

  public long getInitialImageTime() {
    return getMemberLevelStatistic(StatsKey.GET_INITIAL_IMAGE_TIME).longValue();
  }

  public int getInitialImagesInProgress() {
    return getMemberLevelStatistic(StatsKey.GET_INITIAL_IMAGES_INPROGRESS).intValue();
  }

  long getTotalIndexMaintenanceTime() {
    return getMemberLevelStatistic(StatsKey.TOTAL_INDEX_UPDATE_TIME).longValue();
  }

  public float getBytesReceivedRate() {
    return bytesReceivedRate.getRate();
  }

  public float getBytesSentRate() {
    return bytesSentRate.getRate();
  }

  public long getCacheListenerCallsAvgLatency() {
    return listenerCallsAvgLatency.getAverageLatency();
  }

  public long getCacheWriterCallsAvgLatency() {
    return writerCallsAvgLatency.getAverageLatency();
  }

  public float getCreatesRate() {
    instCreatesRate = createsRate.getRate();
    return instCreatesRate;
  }

  public float getDestroysRate() {
    return destroysRate.getRate();
  }

  public float getDiskReadsRate() {
    return diskReadsRate.getRate();
  }

  public float getDiskWritesRate() {
    return diskWritesRate.getRate();
  }

  public int getTotalBackupInProgress() {
    return diskMonitor.getBackupsInProgress();
  }

  public int getTotalBackupCompleted() {
    return diskMonitor.getBackupsCompleted();
  }

  public long getDiskFlushAvgLatency() {
    return diskFlushAvgLatency.getAverageLatency();
  }

  public float getFunctionExecutionRate() {
    return functionExecutionRate.getRate();
  }

  public long getGetsAvgLatency() {
    return getsAvgLatency.getAverageLatency();
  }

  public float getGetsRate() {
    instGetsRate = getsRate.getRate();
    return instGetsRate;
  }

  public int getLockWaitsInProgress() {
    return getMemberLevelStatistic(StatsKey.LOCK_WAITS_IN_PROGRESS).intValue();
  }

  public int getNumRunningFunctions() {
    return getMemberLevelStatistic(StatsKey.FUNCTION_EXECUTIONS_RUNNING).intValue();
  }

  public int getNumRunningFunctionsHavingResults() {
    return getMemberLevelStatistic(StatsKey.FUNCTION_EXECUTIONS_HASRESULT_RUNNING).intValue();
  }

  public long getPutAllAvgLatency() {
    return putAllAvgLatency.getAverageLatency();
  }

  public float getPutAllRate() {
    instPutAllRate = putAllRate.getRate();
    return instPutAllRate;
  }

  long getPutsAvgLatency() {
    return putsAvgLatency.getAverageLatency();
  }

  public float getPutsRate() {
    instPutsRate = putsRate.getRate();
    return instPutsRate;
  }

  public int getLockRequestQueues() {
    return getMemberLevelStatistic(StatsKey.LOCK_REQUEST_QUEUE).intValue();
  }

  int getPartitionRegionCount() {
    return getMemberLevelStatistic(StatsKey.PARTITIONED_REGIONS).intValue();
  }

  public int getTotalPrimaryBucketCount() {
    return regionMonitor.getTotalPrimaryBucketCount();
  }

  public int getTotalBucketCount() {
    return regionMonitor.getTotalBucketCount();
  }

  public int getTotalBucketSize() {
    return regionMonitor.getTotalBucketSize();
  }

  public int getTotalHitCount() {
    return getMemberLevelStatistic(StatsKey.GETS).intValue() - getTotalMissCount();
  }

  public float getLruDestroyRate() {
    return lruDestroyRate.getRate();
  }

  public float getLruEvictionRate() {
    return lruEvictionRate.getRate();
  }

  public int getTotalLoadsCompleted() {
    return getMemberLevelStatistic(StatsKey.LOADS_COMPLETED).intValue();
  }

  public long getLoadsAverageLatency() {
    return loadsAverageLatency.getAverageLatency();
  }

  public int getTotalNetLoadsCompleted() {
    return getMemberLevelStatistic(StatsKey.NET_LOADS_COMPLETED).intValue();
  }

  public long getNetLoadsAverageLatency() {
    return netLoadsAverageLatency.getAverageLatency();
  }

  public int getTotalNetSearchCompleted() {
    return getMemberLevelStatistic(StatsKey.NET_SEARCH_COMPLETED).intValue();
  }

  public long getNetSearchAverageLatency() {
    return netSearchAverageLatency.getAverageLatency();
  }

  public long getTotalLockWaitTime() {
    return getMemberLevelStatistic(StatsKey.LOCK_WAIT_TIME).intValue();
  }

  public int getTotalMissCount() {
    return getMemberLevelStatistic(StatsKey.MISSES).intValue();
  }

  public int getTotalNumberOfLockService() {
    return getMemberLevelStatistic(StatsKey.LOCK_SERVICES).intValue();
  }

  public int getTotalNumberOfGrantors() {
    return getMemberLevelStatistic(StatsKey.LOCK_GRANTORS).intValue();
  }

  public int getTotalDiskTasksWaiting() {
    return getMemberLevelStatistic(StatsKey.TOTAL_DISK_TASK_WAITING).intValue();
  }

  public int getTotalRegionCount() {
    return getMemberLevelStatistic(StatsKey.REGIONS).intValue();
  }

  public int getTotalRegionEntryCount() {
    return getMemberLevelStatistic(StatsKey.ENTRIES).intValue();
  }

  public int getTotalTransactionsCount() {
    return getMemberLevelStatistic(StatsKey.TRANSACTION_COMMITS).intValue()
        + getMemberLevelStatistic(StatsKey.TRANSACTION_ROLLBACKS).intValue();
  }

  public long getTransactionCommitsAvgLatency() {
    return transactionCommitsAvgLatency.getAverageLatency();
  }

  public float getTransactionCommitsRate() {
    return transactionCommitsRate.getRate();
  }

  public int getTransactionCommittedTotalCount() {
    return getMemberLevelStatistic(StatsKey.TRANSACTION_COMMITS).intValue();
  }

  public int getTransactionRolledBackTotalCount() {
    return getMemberLevelStatistic(StatsKey.TRANSACTION_ROLLBACKS).intValue();
  }

  long getDeserializationAvgLatency() {
    return deserializationAvgLatency.getAverageLatency();
  }

  long getDeserializationLatency() {
    return deserializationLatency.getLatency();
  }

  float getDeserializationRate() {
    return deserializationRate.getRate();
  }

  long getSerializationAvgLatency() {
    return serializationAvgLatency.getAverageLatency();
  }

  long getSerializationLatency() {
    return serializationLatency.getLatency();
  }

  float getSerializationRate() {
    return serializationRate.getRate();
  }

  long getPDXDeserializationAvgLatency() {
    return pdxDeserializationAvgLatency.getAverageLatency();
  }

  float getPDXDeserializationRate() {
    return pdxDeserializationRate.getRate();
  }

  public String processCommand(String commandString, Map<String, String> environment,
      List<String> stagedFilePaths) {
    if (commandProcessor == null) {
      throw new JMRuntimeException(
          "Command can not be processed as Command Service did not get initialized. Reason: "
              + commandServiceInitError);
    }

    return commandProcessor.executeCommandReturningJson(commandString, environment,
        stagedFilePaths);
  }

  public long getTotalDiskUsage() {
    return regionMonitor.getDiskSpace();
  }

  public float getAverageReads() {
    return instGetsRate;
  }

  public float getAverageWrites() {
    return instCreatesRate + instPutsRate + instPutAllRate;
  }

  long getGarbageCollectionTime() {
    return getGCStatistic(StatsKey.VM_GC_STATS_COLLECTION_TIME).longValue();
  }

  long getGarbageCollectionCount() {
    return getGCStatistic(StatsKey.VM_GC_STATS_COLLECTIONS).longValue();
  }

  public long getJVMPauses() {
    return getSystemStatistic(StatsKey.JVM_PAUSES).intValue();
  }

  double getLoadAverage() {
    return osBean.getSystemLoadAverage();
  }

  public int getNumThreads() {
    return getVMStatistic(StatsKey.VM_STATS_NUM_THREADS).intValue();
  }

  long getFileDescriptorLimit() {
    if (!osName.startsWith(ManagementConstants.LINUX_SYSTEM)) {
      return -1;
    }

    try {
      return (Long) ManagementFactory.getPlatformMBeanServer()
          .getAttribute(osObjectName, "MaxFileDescriptorCount");
    } catch (Exception ignore) {
      // ignore
    }
    return -1;
  }

  long getTotalFileDescriptorOpen() {
    if (!osName.startsWith(ManagementConstants.LINUX_SYSTEM)) {
      return -1;
    }
    return getVMStatistic(StatsKey.VM_STATS_OPEN_FDS).longValue();
  }

  int getOffHeapObjects() {
    int objects = 0;
    OffHeapMemoryStats stats = getOffHeapStats();

    if (null != stats) {
      objects = stats.getObjects();
    }

    return objects;
  }

  @Deprecated
  public long getOffHeapFreeSize() {
    return getOffHeapFreeMemory();
  }

  @Deprecated
  public long getOffHeapUsedSize() {
    return getOffHeapUsedMemory();
  }

  long getOffHeapMaxMemory() {
    OffHeapMemoryStats stats = getOffHeapStats();

    if (null != stats) {
      return stats.getMaxMemory();
    }

    return 0;
  }

  long getOffHeapFreeMemory() {
    OffHeapMemoryStats stats = getOffHeapStats();

    if (null != stats) {
      return stats.getFreeMemory();
    }

    return 0;
  }

  long getOffHeapUsedMemory() {
    OffHeapMemoryStats stats = getOffHeapStats();

    if (null != stats) {
      return stats.getUsedMemory();
    }

    return 0;
  }

  int getOffHeapFragmentation() {
    OffHeapMemoryStats stats = getOffHeapStats();

    if (null != stats) {
      return stats.getFragmentation();
    }

    return 0;
  }

  long getOffHeapCompactionTime() {
    OffHeapMemoryStats stats = getOffHeapStats();

    if (null != stats) {
      return stats.getDefragmentationTime();
    }

    return 0;
  }

  private OffHeapMemoryStats getOffHeapStats() {
    MemoryAllocator offHeap = cache.getOffHeapStore();

    if (null != offHeap) {
      return offHeap.getStats();
    }

    return null;
  }

  int getHostCpuUsage() {
    if (systemStat != null) {
      return systemStat.get(StatsKey.SYSTEM_CPU_ACTIVE).intValue();
    }
    return ManagementConstants.NOT_AVAILABLE_INT;
  }

  public boolean isCacheServer() {
    return cacheServer;
  }

  public void setCacheServer(boolean cacheServer) {
    this.cacheServer = cacheServer;
  }

  public String getRedundancyZone() {
    return redundancyZone;
  }

  int getRebalancesInProgress() {
    return resourceManagerStats.getRebalancesInProgress();
  }

  public int getReplyWaitsInProgress() {
    return getMemberLevelStatistic(StatsKey.REPLY_WAITS_IN_PROGRESS).intValue();
  }

  public int getReplyWaitsCompleted() {
    return getMemberLevelStatistic(StatsKey.REPLY_WAITS_COMPLETED).intValue();
  }

  public int getVisibleNodes() {
    return getMemberLevelStatistic(StatsKey.NODES).intValue();
  }

  public long getMaxMemory() {
    return Runtime.getRuntime().maxMemory() / MBFactor;
  }

  public long getFreeMemory() {
    return Runtime.getRuntime().freeMemory() / MBFactor;
  }

  public long getUsedMemory() {
    return getVMStatistic(StatsKey.VM_USED_MEMORY).longValue() / MBFactor;
  }

  String getReleaseVersion() {
    return GemFireVersion.getGemFireVersion();
  }

  String getGeodeReleaseVersion() {
    return Version.CURRENT.getName();
  }
}
