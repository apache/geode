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
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.management.JMRuntimeException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.logging.log4j.Logger;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.distributed.internal.locks.DLockStats;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.PureJavaMode;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.DirectoryHolder;
import org.apache.geode.internal.cache.DiskDirectoryStats;
import org.apache.geode.internal.cache.DiskRegion;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.DiskStoreStats;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionStats;
import org.apache.geode.internal.cache.control.ResourceManagerStats;
import org.apache.geode.internal.cache.execute.FunctionServiceStats;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.logging.log4j.LogWriterAppender;
import org.apache.geode.internal.logging.log4j.LogWriterAppenders;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.offheap.MemoryAllocator;
import org.apache.geode.internal.offheap.OffHeapMemoryStats;
import org.apache.geode.internal.process.PidUnavailableException;
import org.apache.geode.internal.process.ProcessUtils;
import org.apache.geode.internal.statistics.GemFireStatSampler;
import org.apache.geode.internal.statistics.HostStatHelper;
import org.apache.geode.internal.statistics.StatSamplerStats;
import org.apache.geode.internal.statistics.VMStatsContract;
import org.apache.geode.internal.statistics.platform.LinuxSystemStats;
import org.apache.geode.internal.statistics.platform.ProcessStats;
import org.apache.geode.internal.statistics.platform.SolarisSystemStats;
import org.apache.geode.internal.statistics.platform.WindowsSystemStats;
import org.apache.geode.internal.stats50.VMStats50;
import org.apache.geode.internal.tcp.ConnectionTable;
import org.apache.geode.management.GemFireProperties;
import org.apache.geode.management.JVMMetrics;
import org.apache.geode.management.OSMetrics;
import org.apache.geode.management.internal.ManagementConstants;
import org.apache.geode.management.internal.ManagementStrings;
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
import org.apache.geode.management.internal.cli.CommandResponseBuilder;
import org.apache.geode.management.internal.cli.remote.OnlineCommandProcessor;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.model.ResultModel;

/**
 * This class acts as an Bridge between MemberMBean and GemFire Cache and Distributed System
 */
public class MemberMBeanBridge {

  private static final Logger logger = LogService.getLogger();

  /**
   * Static reference to the Platform MBean server
   */
  public static MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

  /**
   * Factor converting bytes to MBØØ
   */
  private static final long MBFactor = 1024 * 1024;

  private static TimeUnit nanoSeconds = TimeUnit.NANOSECONDS;

  /** Cache Instance **/
  private InternalCache cache;

  /** Distribution Config **/
  private DistributionConfig config;

  /** Composite type **/
  private GemFireProperties gemFirePropertyData;

  /**
   * Internal distributed system
   */
  private InternalDistributedSystem system;

  /**
   * Distribution manager
   */
  private DistributionManager dm;

  /**
   * Command Service
   */
  private OnlineCommandProcessor commandProcessor;

  private String commandServiceInitError;

  /**
   * Reference to JDK bean MemoryMXBean
   */
  private MemoryMXBean memoryMXBean;

  /**
   * Reference to JDK bean ThreadMXBean
   */
  private ThreadMXBean threadMXBean;

  /**
   * Reference to JDK bean RuntimeMXBean
   */
  private RuntimeMXBean runtimeMXBean;

  /**
   * Reference to JDK bean OperatingSystemMXBean
   */
  private OperatingSystemMXBean osBean;

  /**
   * Host name of the member
   */
  private String hostname;

  /**
   * The member's process id (pid)
   */
  private int processId;

  /**
   * OS MBean Object name
   */
  private ObjectName osObjectName;

  /**
   * Last CPU usage calculation time
   */
  private long lastSystemTime = 0;

  /**
   * Last ProcessCPU time
   */
  private long lastProcessCpuTime = 0;

  private MBeanStatsMonitor monitor;

  private volatile boolean lockStatsAdded = false;

  private SystemManagementService service;

  private MemberLevelDiskMonitor diskMonitor;

  private AggregateRegionStatsMonitor regionMonitor;

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

  private String osName = System.getProperty("os.name", "unknown");

  private GCStatsMonitor gcMonitor;

  private VMStatsMonitor vmStatsMonitor;

  private MBeanStatsMonitor systemStatsMonitor;

  private float instCreatesRate = 0;

  private float instGetsRate = 0;

  private float instPutsRate = 0;

  private float instPutAllRate = 0;

  private GemFireStatSampler sampler;

  private Statistics systemStat;

  private static final String MEMBER_LEVEL_DISK_MONITOR = "MemberLevelDiskMonitor";
  private static final String MEMBER_LEVEL_REGION_MONITOR = "MemberLevelRegionMonitor";

  private boolean cacheServer = false;

  private String redundancyZone = "";

  private ResourceManagerStats resourceManagerStats;

  public MemberMBeanBridge(InternalCache cache, SystemManagementService service) {
    this.cache = cache;
    this.service = service;

    this.system = (InternalDistributedSystem) cache.getDistributedSystem();

    this.dm = system.getDistributionManager();

    if (dm instanceof ClusterDistributionManager) {
      ClusterDistributionManager distManager =
          (ClusterDistributionManager) system.getDistributionManager();
      this.redundancyZone = distManager
          .getRedundancyZone(cache.getInternalDistributedSystem().getDistributedMember());
    }

    this.sampler = system.getStatSampler();

    this.config = system.getConfig();
    try {
      this.commandProcessor =
          new OnlineCommandProcessor(system.getProperties(), cache.getSecurityService(), cache);
    } catch (Exception e) {
      commandServiceInitError = e.getMessage();
      logger.info(LogMarker.CONFIG_MARKER, "Command processor could not be initialized. {}",
          e.getMessage());
    }

    intitGemfireProperties();

    try {
      InetAddress addr = SocketCreator.getLocalHost();
      this.hostname = addr.getHostName();
    } catch (UnknownHostException ignore) {
      this.hostname = ManagementConstants.DEFAULT_HOST_NAME;
    }

    try {
      this.osObjectName = new ObjectName("java.lang:type=OperatingSystem");
    } catch (MalformedObjectNameException ex) {
      if (logger.isDebugEnabled()) {
        logger.debug(ex.getMessage(), ex);
      }
    } catch (NullPointerException ex) {
      if (logger.isDebugEnabled()) {
        logger.debug(ex.getMessage(), ex);
      }
    }

    this.memoryMXBean = ManagementFactory.getMemoryMXBean();

    this.threadMXBean = ManagementFactory.getThreadMXBean();

    this.runtimeMXBean = ManagementFactory.getRuntimeMXBean();
    this.osBean = ManagementFactory.getOperatingSystemMXBean();

    // Initialize all the Stats Monitors
    this.monitor =
        new MBeanStatsMonitor(ManagementStrings.MEMBER_CACHE_MONITOR.toLocalizedString());
    this.diskMonitor = new MemberLevelDiskMonitor(MEMBER_LEVEL_DISK_MONITOR);
    this.regionMonitor = new AggregateRegionStatsMonitor(MEMBER_LEVEL_REGION_MONITOR);
    this.gcMonitor = new GCStatsMonitor(ManagementStrings.GC_STATS_MONITOR.toLocalizedString());
    this.vmStatsMonitor =
        new VMStatsMonitor(ManagementStrings.VM_STATS_MONITOR.toLocalizedString());

    this.systemStatsMonitor =
        new MBeanStatsMonitor(ManagementStrings.SYSTEM_STATS_MONITOR.toLocalizedString());

    // Initialize Proecess related informations

    this.gemFireVersion = GemFireVersion.asString();
    this.classPath = runtimeMXBean.getClassPath();
    this.name = cache.getDistributedSystem().getDistributedMember().getName();
    this.id = cache.getDistributedSystem().getDistributedMember().getId();

    try {
      this.processId = ProcessUtils.identifyPid();
    } catch (PidUnavailableException ex) {
      if (logger.isDebugEnabled()) {
        logger.debug(ex.getMessage(), ex);
      }
    }

    QueryDataFunction qDataFunction = new QueryDataFunction();
    FunctionService.registerFunction(qDataFunction);

    this.resourceManagerStats = cache.getInternalResourceManager().getStats();
  }

  public MemberMBeanBridge() {
    this.monitor =
        new MBeanStatsMonitor(ManagementStrings.MEMBER_CACHE_MONITOR.toLocalizedString());
    this.diskMonitor = new MemberLevelDiskMonitor(MEMBER_LEVEL_DISK_MONITOR);
    this.regionMonitor = new AggregateRegionStatsMonitor(MEMBER_LEVEL_REGION_MONITOR);
    this.gcMonitor = new GCStatsMonitor(ManagementStrings.GC_STATS_MONITOR.toLocalizedString());
    this.vmStatsMonitor =
        new VMStatsMonitor(ManagementStrings.VM_STATS_MONITOR.toLocalizedString());
    this.systemStatsMonitor =
        new MBeanStatsMonitor(ManagementStrings.SYSTEM_STATS_MONITOR.toLocalizedString());

    this.system = InternalDistributedSystem.getConnectedInstance();

    initializeStats();
  }

  public MemberMBeanBridge init() {
    CachePerfStats cachePerfStats = this.cache.getCachePerfStats();
    addCacheStats(cachePerfStats);
    addFunctionStats(system.getFunctionServiceStats());

    if (system.getDistributionManager().getStats() instanceof DistributionStats) {
      DistributionStats distributionStats =
          (DistributionStats) system.getDistributionManager().getStats();
      addDistributionStats(distributionStats);
    }

    if (PureJavaMode.osStatsAreAvailable()) {
      Statistics[] systemStats = null;

      if (HostStatHelper.isSolaris()) {
        systemStats = system.findStatisticsByType(SolarisSystemStats.getType());
      } else if (HostStatHelper.isLinux()) {
        systemStats = system.findStatisticsByType(LinuxSystemStats.getType());
      } else if (HostStatHelper.isOSX()) {
        systemStats = null;// @TODO once OSX stats are implemented
      } else if (HostStatHelper.isWindows()) {
        systemStats = system.findStatisticsByType(WindowsSystemStats.getType());
      }

      if (systemStats != null) {
        systemStat = systemStats[0];
      }
    }

    MemoryAllocator allocator = this.cache.getOffHeapStore();
    if ((null != allocator)) {
      OffHeapMemoryStats offHeapStats = allocator.getStats();

      if (null != offHeapStats) {
        addOffHeapStats(offHeapStats);
      }
    }

    addSystemStats();
    addVMStats();
    initializeStats();

    return this;
  }

  public void addOffHeapStats(OffHeapMemoryStats offHeapStats) {
    Statistics offHeapMemoryStatistics = offHeapStats.getStats();
    monitor.addStatisticsToMonitor(offHeapMemoryStatistics);
  }

  public void addCacheStats(CachePerfStats cachePerfStats) {
    Statistics cachePerfStatistics = cachePerfStats.getStats();
    monitor.addStatisticsToMonitor(cachePerfStatistics);
  }

  public void addFunctionStats(FunctionServiceStats functionServiceStats) {
    Statistics functionStatistics = functionServiceStats.getStats();
    monitor.addStatisticsToMonitor(functionStatistics);
  }

  public void addDistributionStats(DistributionStats distributionStats) {
    Statistics dsStats = distributionStats.getStats();
    monitor.addStatisticsToMonitor(dsStats);
  }

  public void addDiskStore(DiskStore dsi) {
    DiskStoreImpl impl = (DiskStoreImpl) dsi;
    addDiskStoreStats(impl.getStats());
  }

  public void addDiskStoreStats(DiskStoreStats stats) {
    diskMonitor.addStatisticsToMonitor(stats.getStats());
  }

  public void removeDiskStore(DiskStore dsi) {
    DiskStoreImpl impl = (DiskStoreImpl) dsi;
    removeDiskStoreStats(impl.getStats());
  }

  public void removeDiskStoreStats(DiskStoreStats stats) {
    diskMonitor.removeStatisticsFromMonitor(stats.getStats());
  }

  public void addRegion(Region region) {
    if (region.getAttributes().getPartitionAttributes() != null) {
      addPartionRegionStats(((PartitionedRegion) region).getPrStats());
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

  public void addPartionRegionStats(PartitionedRegionStats parStats) {
    regionMonitor.addStatisticsToMonitor(parStats.getStats());
  }

  public void addLRUStats(Statistics lruStats) {
    if (lruStats != null) {
      regionMonitor.addStatisticsToMonitor(lruStats);
    }
  }

  public void addDirectoryStats(DiskDirectoryStats diskDirStats) {
    regionMonitor.addStatisticsToMonitor(diskDirStats.getStats());
  }

  public void removeRegion(Region region) {
    if (region.getAttributes().getPartitionAttributes() != null) {
      removePartionRegionStats(((PartitionedRegion) region).getPrStats());
    }

    LocalRegion l = (LocalRegion) region;
    removeLRUStats(l.getEvictionStatistics());

    DiskRegion dr = l.getDiskRegion();
    if (dr != null) {
      for (DirectoryHolder dh : dr.getDirectories()) {
        removeDirectoryStats(dh.getDiskDirectoryStats());
      }
    }
  }

  public void removePartionRegionStats(PartitionedRegionStats parStats) {
    regionMonitor.removePartitionStatistics(parStats.getStats());
  }

  public void removeLRUStats(Statistics statistics) {
    if (statistics != null) {
      regionMonitor.removeLRUStatistics(statistics);
    }
  }

  public void removeDirectoryStats(DiskDirectoryStats diskDirStats) {
    regionMonitor.removeDirectoryStatistics(diskDirStats.getStats());
  }

  public void addLockServiceStats(DLockService lock) {
    if (!lockStatsAdded) {
      DLockStats stats = (DLockStats) lock.getStats();
      addLockServiceStats(stats);
      lockStatsAdded = true;
    }
  }

  public void addLockServiceStats(DLockStats stats) {
    monitor.addStatisticsToMonitor(stats.getStats());
  }

  public void addSystemStats() {
    GemFireStatSampler sampler = system.getStatSampler();

    ProcessStats processStats = sampler.getProcessStats();

    StatSamplerStats samplerStats = sampler.getStatSamplerStats();
    if (processStats != null) {
      systemStatsMonitor.addStatisticsToMonitor(processStats.getStatistics());
    }
    if (samplerStats != null) {
      systemStatsMonitor.addStatisticsToMonitor(samplerStats.getStats());
    }
  }

  public void addVMStats() {
    VMStatsContract vmStatsContract = system.getStatSampler().getVMStats();

    if (vmStatsContract != null && vmStatsContract instanceof VMStats50) {
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
        Statistics[] gcStats = system.findStatisticsByType(gcType);
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

  public Number getMemberLevelStatistic(String statName) {
    return monitor.getStatistic(statName);
  }

  public Number getVMStatistic(String statName) {
    return vmStatsMonitor.getStatistic(statName);
  }

  public Number getGCStatistic(String statName) {
    return gcMonitor.getStatistic(statName);
  }

  public Number getSystemStatistic(String statName) {
    return systemStatsMonitor.getStatistic(statName);
  }

  public void stopMonitor() {
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

  private void intitGemfireProperties() {
    if (gemFirePropertyData == null) {
      this.gemFirePropertyData = BeanUtilFuncs.initGemfireProperties(config);
    }
  }

  /**
   * @return Some basic JVM metrics at the particular instance
   */
  public JVMMetrics fetchJVMMetrics() {
    long gcCount = getGCStatistic(StatsKey.VM_GC_STATS_COLLECTIONS).longValue();
    long gcTimeMillis = getGCStatistic(StatsKey.VM_GC_STATS_COLLECTION_TIME).longValue();

    // Fixed values might not be updated back by Stats monitor. Hence getting it directly
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
   *
   * @return Some basic OS metrics at the particular instance
   */
  public OSMetrics fetchOSMetrics() {
    OSMetrics metrics = null;
    try {
      long maxFileDescriptorCount = 0;
      long openFileDescriptorCount = 0;
      long processCpuTime = 0;
      long committedVirtualMemorySize = 0;
      long totalPhysicalMemorySize = 0;
      long freePhysicalMemorySize = 0;
      long totalSwapSpaceSize = 0;
      long freeSwapSpaceSize = 0;

      String name = osBean.getName();
      String version = osBean.getVersion();
      String arch = osBean.getArch();
      int availableProcessors = osBean.getAvailableProcessors();
      double systemLoadAverage = osBean.getSystemLoadAverage();

      openFileDescriptorCount = getVMStatistic(StatsKey.VM_STATS_OPEN_FDS).longValue();
      processCpuTime = getVMStatistic(StatsKey.VM_PROCESS_CPU_TIME).longValue();

      try {
        maxFileDescriptorCount =
            (Long) mbeanServer.getAttribute(osObjectName, "MaxFileDescriptorCount");
      } catch (Exception ignore) {
        maxFileDescriptorCount = -1;
      }
      try {
        committedVirtualMemorySize =
            (Long) mbeanServer.getAttribute(osObjectName, "CommittedVirtualMemorySize");
      } catch (Exception ignore) {
        committedVirtualMemorySize = -1;
      }

      // If Linux System type exists
      if (PureJavaMode.osStatsAreAvailable() && HostStatHelper.isLinux() && systemStat != null) {

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

      metrics = new OSMetrics(maxFileDescriptorCount, openFileDescriptorCount, processCpuTime,
          committedVirtualMemorySize, totalPhysicalMemorySize, freePhysicalMemorySize,
          totalSwapSpaceSize, freeSwapSpaceSize, name, version, arch, availableProcessors,
          systemLoadAverage);

    } catch (Exception ex) {
      if (logger.isTraceEnabled()) {
        logger.trace(ex.getMessage(), ex);
      }
    }
    return metrics;
  }

  /**
   * @return GemFire Properties
   */
  public GemFireProperties getGemFireProperty() {
    return gemFirePropertyData;
  }

  /**
   * Creates a Manager
   *
   * @return successful or not
   */
  public boolean createManager() {
    if (service.isManager()) {
      return false;
    }
    return service.createManager();
  }

  /**
   * An instruction to members with cache that they should compact their disk stores.
   *
   * @return a list of compacted Disk stores
   */
  public String[] compactAllDiskStores() {
    List<String> compactedStores = new ArrayList<String>();

    if (cache != null && !cache.isClosed()) {
      for (DiskStore store : this.cache.listDiskStoresIncludingRegionOwned()) {
        if (store.forceCompaction()) {
          compactedStores.add(((DiskStoreImpl) store).getPersistentID().getDirectory());
        }
      }
    }
    String[] compactedStoresAr = new String[compactedStores.size()];
    return compactedStores.toArray(compactedStoresAr);
  }

  /**
   * List all the disk Stores at member level
   *
   * @param includeRegionOwned indicates whether to show the disk belonging to any particular region
   * @return list all the disk Stores name at cache level
   */
  public String[] listDiskStores(boolean includeRegionOwned) {
    String[] retStr = null;
    Collection<DiskStore> diskCollection = null;
    if (includeRegionOwned) {
      diskCollection = this.cache.listDiskStoresIncludingRegionOwned();
    } else {
      diskCollection = this.cache.listDiskStores();
    }
    if (diskCollection != null && diskCollection.size() > 0) {
      retStr = new String[diskCollection.size()];
      Iterator<DiskStore> it = diskCollection.iterator();
      int i = 0;
      while (it.hasNext()) {
        retStr[i] = it.next().getName();
        i++;
      }
    }
    return retStr;
  }

  /**
   * @return list of disk stores which defaults includeRegionOwned = true;
   */
  public String[] getDiskStores() {
    return listDiskStores(true);
  }

  /**
   * @return log of the member.
   */
  public String fetchLog(int numLines) {
    if (numLines > ManagementConstants.MAX_SHOW_LOG_LINES) {
      numLines = ManagementConstants.MAX_SHOW_LOG_LINES;
    }
    if (numLines == 0 || numLines < 0) {
      numLines = ManagementConstants.DEFAULT_SHOW_LOG_LINES;
    }
    String childTail = null;
    String mainTail = null;
    try {
      InternalDistributedSystem sys = system;

      LogWriterAppender lwa = LogWriterAppenders.getAppender(LogWriterAppenders.Identifier.MAIN);
      if (lwa != null) {
        childTail = BeanUtilFuncs.tailSystemLog(lwa.getChildLogFile(), numLines);
        mainTail = BeanUtilFuncs.tailSystemLog(sys.getConfig(), numLines);
        if (mainTail == null) {
          mainTail =
              LocalizedStrings.TailLogResponse_NO_LOG_FILE_WAS_SPECIFIED_IN_THE_CONFIGURATION_MESSAGES_WILL_BE_DIRECTED_TO_STDOUT
                  .toLocalizedString();
        }
      } else {
        throw new IllegalStateException(
            "TailLogRequest/Response processed in application vm with shared logging. This would occur if there is no 'log-file' defined.");
      }
    } catch (IOException e) {
      logger.warn(LocalizedMessage
          .create(LocalizedStrings.TailLogResponse_ERROR_OCCURRED_WHILE_READING_SYSTEM_LOG__0, e));
      mainTail = "";
    }

    if (childTail == null && mainTail == null) {
      return LocalizedStrings.SystemMemberImpl_NO_LOG_FILE_CONFIGURED_LOG_MESSAGES_WILL_BE_DIRECTED_TO_STDOUT
          .toLocalizedString();
    } else {
      StringBuilder result = new StringBuilder();
      if (mainTail != null) {
        result.append(mainTail);
      }
      if (childTail != null) {
        result.append(getLineSeparator())
            .append(LocalizedStrings.SystemMemberImpl_TAIL_OF_CHILD_LOG.toLocalizedString())
            .append(getLineSeparator());
        result.append(childTail);
      }
      return result.toString();
    }
  }

  /**
   * Using async thread. As remote operation will be executed by FunctionService. Might cause
   * problems in cleaning up function related resources. Aggregate bean DistributedSystemMBean will
   * have to depend on GemFire messages to decide whether all the members have been shutdown or not
   * before deciding to shut itself down
   */
  public void shutDownMember() {
    final InternalDistributedSystem ids = dm.getSystem();
    if (ids.isConnected()) {
      Thread t = new Thread(new Runnable() {
        public void run() {
          try {
            // Allow the Function call to exit
            Thread.sleep(1000);
          } catch (InterruptedException ignore) {
          }
          ConnectionTable.threadWantsSharedResources();
          if (ids.isConnected()) {
            ids.disconnect();
          }
        }
      });
      t.setDaemon(false);
      t.start();
    }
  }

  /**
   * @return The name for this member.
   */
  public String getName() {
    return name;
  }

  /**
   * @return The ID for this member.
   */
  public String getId() {
    return id;
  }

  /**
   * @return The name of the member if it's been set, otherwise the ID of the member
   */
  public String getMember() {
    if (name != null && !name.isEmpty()) {
      return name;
    }
    return id;
  }

  public String[] getGroups() {
    List<String> groups = cache.getDistributedSystem().getDistributedMember().getGroups();
    String[] groupsArray = new String[groups.size()];
    groupsArray = groups.toArray(groupsArray);
    return groupsArray;
  }

  /**
   * @return classPath of the VM
   */
  public String getClassPath() {
    return classPath;
  }

  /**
   * @return Connected gateway receivers
   */
  public String[] listConnectedGatewayReceivers() {
    if ((cache != null && cache.getGatewayReceivers().size() > 0)) {
      Set<GatewayReceiver> receivers = cache.getGatewayReceivers();
      String[] arr = new String[receivers.size()];
      int j = 0;
      for (GatewayReceiver recv : receivers) {
        arr[j] = recv.getBindAddress();
        j++;
      }
      return arr;
    }
    return ManagementConstants.NO_DATA_STRING;
  }

  /**
   * @return Connected gateway senders
   */
  public String[] listConnectedGatewaySenders() {
    if ((cache != null && cache.getGatewaySenders().size() > 0)) {
      Set<GatewaySender> senders = cache.getGatewaySenders();
      String[] arr = new String[senders.size()];
      int j = 0;
      for (GatewaySender sender : senders) {
        arr[j] = sender.getId();
        j++;
      }
      return arr;
    }
    return ManagementConstants.NO_DATA_STRING;
  }

  /**
   * @return approximate usage of CPUs
   */
  public float getCpuUsage() {
    return vmStatsMonitor.getCpuUsage();
  }

  /**
   * @return current time of the system
   */
  public long getCurrentTime() {
    return System.currentTimeMillis();
  }

  public String getHost() {
    return hostname;
  }

  /**
   * @return the member's process id (pid)
   */
  public int getProcessId() {
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
  public String status() {
    if (LocatorLauncher.getInstance() != null) {
      return LocatorLauncher.getLocatorState().toJson();
    } else if (ServerLauncher.getInstance() != null) {
      return ServerLauncher.getServerState().toJson();
    }

    // TODO implement for non-launcher processes and other GemFire processes (Managers, etc)...
    return null;
  }

  /**
   * @return total heap usage in bytes
   */
  public long getTotalBytesInUse() {
    MemoryUsage memHeap = memoryMXBean.getHeapMemoryUsage();
    return memHeap.getUsed();
  }

  /**
   * @return Number of availabe CPUs
   */
  public int getAvailableCpus() {
    Runtime runtime = Runtime.getRuntime();
    return runtime.availableProcessors();
  }

  /**
   * @return JVM thread list
   */
  public String[] fetchJvmThreads() {
    long threadIds[] = threadMXBean.getAllThreadIds();
    ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadIds, 0);
    if (threadInfos == null || threadInfos.length < 1) {
      return ManagementConstants.NO_DATA_STRING;
    }
    ArrayList<String> thrdStr = new ArrayList<String>(threadInfos.length);
    for (ThreadInfo thInfo : threadInfos) {
      if (thInfo != null) {
        thrdStr.add(thInfo.getThreadName());
      }
    }
    String[] result = new String[thrdStr.size()];
    return thrdStr.toArray(result);
  }

  /**
   * @return list of regions
   */
  public String[] getListOfRegions() {
    Set<InternalRegion> listOfAppRegions = cache.getApplicationRegions();
    if (listOfAppRegions != null && listOfAppRegions.size() > 0) {
      String[] regionStr = new String[listOfAppRegions.size()];
      int j = 0;
      for (InternalRegion rg : listOfAppRegions) {
        regionStr[j] = rg.getFullPath();
        j++;
      }
      return regionStr;
    }
    return ManagementConstants.NO_DATA_STRING;
  }

  /**
   * @return configuration data lock lease
   */
  public long getLockLease() {
    return cache.getLockLease();
  }

  /**
   * @return configuration data lock time out
   */
  public long getLockTimeout() {
    return cache.getLockTimeout();
  }

  /**
   * @return the duration for which the member is up
   */
  public long getMemberUpTime() {
    return cache.getUpTime();
  }

  /**
   * @return root region names
   */
  public String[] getRootRegionNames() {
    Set<Region<?, ?>> listOfRootRegions = cache.rootRegions();
    if (listOfRootRegions != null && listOfRootRegions.size() > 0) {
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

  /**
   * @return Current GemFire version
   */
  public String getVersion() {
    return gemFireVersion;
  }

  /**
   * @return true if this members has a gateway receiver
   */
  public boolean hasGatewayReceiver() {
    return (cache != null && cache.getGatewayReceivers().size() > 0);
  }

  /**
   * @return true if member has Gateway senders
   */
  public boolean hasGatewaySender() {
    return (cache != null && cache.getAllGatewaySenders().size() > 0);
  }

  /**
   * @return true if member contains one locator. From 7.0 only locator can be hosted in a JVM
   */
  public boolean isLocator() {
    return Locator.hasLocator();
  }

  /**
   * @return true if the Federating Manager Thread is running
   */
  public boolean isManager() {
    if (this.cache == null || this.cache.isClosed()) {
      return false;
    }
    try {
      return service.isManager();
    } catch (Exception ignore) {
      return false;
    }
  }

  /**
   * Returns true if the manager has been created. Note it does not need to be running so this
   * method can return true when isManager returns false.
   *
   * @return true if the manager has been created.
   */
  public boolean isManagerCreated() {
    if (this.cache == null || this.cache.isClosed()) {
      return false;
    }
    try {
      return service.isManagerCreated();
    } catch (Exception ignore) {
      return false;
    }
  }

  /**
   * @return true if member has a server
   */
  public boolean isServer() {
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

  public long getTotalIndexMaintenanceTime() {
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
    this.instCreatesRate = createsRate.getRate();
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
    this.instGetsRate = getsRate.getRate();
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
    this.instPutAllRate = putAllRate.getRate();
    return instPutAllRate;
  }

  public long getPutsAvgLatency() {
    return putsAvgLatency.getAverageLatency();
  }

  public float getPutsRate() {
    this.instPutsRate = putsRate.getRate();
    return instPutsRate;
  }

  public int getLockRequestQueues() {
    return getMemberLevelStatistic(StatsKey.LOCK_REQUEST_QUEUE).intValue();
  }

  public int getPartitionRegionCount() {
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

  public long getDeserializationAvgLatency() {
    return deserializationAvgLatency.getAverageLatency();
  }

  public long getDeserializationLatency() {
    return deserializationLatency.getLatency();
  }

  public float getDeserializationRate() {
    return deserializationRate.getRate();
  }

  public long getSerializationAvgLatency() {
    return serializationAvgLatency.getAverageLatency();
  }

  public long getSerializationLatency() {
    return serializationLatency.getLatency();
  }

  public float getSerializationRate() {
    return serializationRate.getRate();
  }

  public long getPDXDeserializationAvgLatency() {
    return pdxDeserializationAvgLatency.getAverageLatency();
  }

  public float getPDXDeserializationRate() {
    return pdxDeserializationRate.getRate();
  }

  /**
   * Processes the given command string using the given environment information if it's non-empty.
   * Result returned is in a JSON format.
   *
   * @param commandString command string to be processed
   * @param env environment information to be used for processing the command
   * @param stagedFilePaths list of local files to be deployed
   * @return result of the processing the given command string.
   */
  public String processCommand(String commandString, Map<String, String> env,
      List<String> stagedFilePaths) {
    if (commandProcessor == null) {
      throw new JMRuntimeException(
          "Command can not be processed as Command Service did not get initialized. Reason: "
              + commandServiceInitError);
    }

    Object result = commandProcessor.executeCommand(commandString, env, stagedFilePaths);

    if (result instanceof CommandResult) {
      return CommandResponseBuilder.createCommandResponseJson(getMember(), (CommandResult) result);
    } else {
      return CommandResponseBuilder.createCommandResponseJson(getMember(), (ResultModel) result);
    }
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

  public long getGarbageCollectionTime() {
    return getGCStatistic(StatsKey.VM_GC_STATS_COLLECTION_TIME).longValue();
  }

  public long getGarbageCollectionCount() {
    return getGCStatistic(StatsKey.VM_GC_STATS_COLLECTIONS).longValue();
  }

  public long getJVMPauses() {
    return getSystemStatistic(StatsKey.JVM_PAUSES).intValue();
  }

  public double getLoadAverage() {
    return osBean.getSystemLoadAverage();
  }

  public int getNumThreads() {
    return getVMStatistic(StatsKey.VM_STATS_NUM_THREADS).intValue();
  }

  /**
   * @return max limit of FD ..Ulimit
   */
  public long getFileDescriptorLimit() {
    if (!osName.startsWith(ManagementConstants.LINUX_SYSTEM)) {
      return -1;
    }
    long maxFileDescriptorCount = 0;
    try {
      maxFileDescriptorCount =
          (Long) mbeanServer.getAttribute(osObjectName, "MaxFileDescriptorCount");
    } catch (Exception ignore) {
      maxFileDescriptorCount = -1;
    }
    return maxFileDescriptorCount;
  }

  /**
   * @return count of currently opened FDs
   */
  public long getTotalFileDescriptorOpen() {
    if (!osName.startsWith(ManagementConstants.LINUX_SYSTEM)) {
      return -1;
    }
    return getVMStatistic(StatsKey.VM_STATS_OPEN_FDS).longValue();
  }

  public int getOffHeapObjects() {
    int objects = 0;
    OffHeapMemoryStats stats = getOffHeapStats();

    if (null != stats) {
      objects = stats.getObjects();
    }

    return objects;
  }

  /**
   * @deprecated Please use {@link #getOffHeapFreeMemory()} instead.
   */
  @Deprecated
  public long getOffHeapFreeSize() {
    return getOffHeapFreeMemory();
  }

  /**
   * @deprecated Please use {@link #getOffHeapUsedMemory()} instead.
   */
  @Deprecated
  public long getOffHeapUsedSize() {
    return getOffHeapUsedMemory();
  }

  public long getOffHeapMaxMemory() {
    long usedSize = 0;
    OffHeapMemoryStats stats = getOffHeapStats();

    if (null != stats) {
      usedSize = stats.getMaxMemory();
    }

    return usedSize;
  }

  public long getOffHeapFreeMemory() {
    long freeSize = 0;
    OffHeapMemoryStats stats = getOffHeapStats();

    if (null != stats) {
      freeSize = stats.getFreeMemory();
    }

    return freeSize;
  }

  public long getOffHeapUsedMemory() {
    long usedSize = 0;
    OffHeapMemoryStats stats = getOffHeapStats();

    if (null != stats) {
      usedSize = stats.getUsedMemory();
    }

    return usedSize;
  }

  public int getOffHeapFragmentation() {
    int fragmentation = 0;
    OffHeapMemoryStats stats = getOffHeapStats();

    if (null != stats) {
      fragmentation = stats.getFragmentation();
    }

    return fragmentation;
  }

  public long getOffHeapCompactionTime() {
    long compactionTime = 0;
    OffHeapMemoryStats stats = getOffHeapStats();

    if (null != stats) {
      compactionTime = stats.getDefragmentationTime();
    }

    return compactionTime;
  }

  /**
   * Returns the OffHeapMemoryStats for this VM.
   */
  private OffHeapMemoryStats getOffHeapStats() {
    OffHeapMemoryStats stats = null;

    MemoryAllocator offHeap = this.cache.getOffHeapStore();

    if (null != offHeap) {
      stats = offHeap.getStats();
    }

    return stats;
  }

  public int getHostCpuUsage() {
    if (systemStat != null) {
      return systemStat.get(StatsKey.SYSTEM_CPU_ACTIVE).intValue();
    } else {
      return ManagementConstants.NOT_AVAILABLE_INT;
    }
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

  public int getRebalancesInProgress() {
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
    Runtime rt = Runtime.getRuntime();
    return rt.maxMemory() / MBFactor;
  }

  public long getFreeMemory() {
    Runtime rt = Runtime.getRuntime();
    return rt.freeMemory() / MBFactor;
  }

  public long getUsedMemory() {
    return getVMStatistic(StatsKey.VM_USED_MEMORY).longValue() / MBFactor;
  }

  public String getReleaseVersion() {
    return GemFireVersion.getGemFireVersion();
  }
}
