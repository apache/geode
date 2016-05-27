/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.stats50;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.StatisticsTypeFactoryImpl;
import com.gemstone.gemfire.internal.VMStatsContract;

import java.lang.management.*;
import java.lang.reflect.Method;
import java.util.*;

/**
 * Statistics related to a Java VM.
 * This version is hardcoded to use 1.5 MXBean stats from java.lang.management.
 */
public class VMStats50 implements VMStatsContract {
  private final static StatisticsType vmType;

  private final static ClassLoadingMXBean clBean;
  private final static MemoryMXBean memBean;
  private final static OperatingSystemMXBean osBean;
  /**
   * This is actually an instance of UnixOperatingSystemMXBean but this class
   * is not available on Windows so needed to make this a runtime check.
   */
  private final static Object unixBean;
  private final static Method getMaxFileDescriptorCount;
  private final static Method getOpenFileDescriptorCount;
  private final static Method getProcessCpuTime;  
  private final static ThreadMXBean threadBean;

  private final static int pendingFinalizationCountId;
  private final static int loadedClassesId;
  private final static int unloadedClassesId;

  private final static int daemonThreadsId;
  private final static int peakThreadsId;
  private final static int threadsId;
  private final static int threadStartsId;
  
  private final static int cpusId;
  private final static int freeMemoryId;
  private final static int totalMemoryId;
  private final static int maxMemoryId;

  private final static StatisticsType memoryUsageType;
  private final static int mu_initMemoryId;
  private final static int mu_maxMemoryId;
  private final static int mu_usedMemoryId;
  private final static int mu_committedMemoryId;

  private final static StatisticsType gcType;
  private final static int gc_collectionsId;
  private final static int gc_collectionTimeId;
  private final Map<GarbageCollectorMXBean,Statistics> gcMap = new HashMap<GarbageCollectorMXBean,Statistics>();

  private final static StatisticsType mpType;
  private final static int mp_l_initMemoryId;
  private final static int mp_l_maxMemoryId;
  private final static int mp_l_usedMemoryId;
  private final static int mp_l_committedMemoryId;
//   private final static int mp_gc_initMemoryId;
//   private final static int mp_gc_maxMemoryId;
  private final static int mp_gc_usedMemoryId;
//   private final static int mp_gc_committedMemoryId;
  private final static int mp_usageThresholdId;
  private final static int mp_collectionUsageThresholdId;
  private final static int mp_usageExceededId;
  private final static int mp_collectionUsageExceededId;
  private final Map<MemoryPoolMXBean,Statistics> mpMap = new HashMap<MemoryPoolMXBean,Statistics>();

  private final static int unix_fdLimitId;
  private final static int unix_fdsOpenId;
  private final static int processCpuTimeId;

  private long threadStartCount = 0;
  private long[] allThreadIds = null;
  private final static boolean THREAD_STATS_ENABLED = Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "enableThreadStats");
  private final Map<Long,ThreadStatInfo> threadMap = THREAD_STATS_ENABLED ? new HashMap<Long,ThreadStatInfo>() : null;
  private final static StatisticsType threadType;
  private final static int thread_blockedId;
  private final static int thread_lockOwnerId;
  private final static int thread_waitedId;
  private final static int thread_inNativeId;
  private final static int thread_suspendedId;
  private final static int thread_blockedTimeId;
  private final static int thread_waitedTimeId;
  private final static int thread_cpuTimeId;
  private final static int thread_userTimeId;

  static {
    clBean = ManagementFactory.getClassLoadingMXBean();
    memBean = ManagementFactory.getMemoryMXBean();
    osBean = ManagementFactory.getOperatingSystemMXBean();
    {
      Method m1 = null;
      Method m2 = null;
      Method m3 = null;
      Object bean = null;
      try {
        Class c = ClassPathLoader.getLatest().forName("com.sun.management.UnixOperatingSystemMXBean");
        if (c.isInstance(osBean)) {
          m1 = c.getMethod("getMaxFileDescriptorCount", new Class[]{});
          m2 = c.getMethod("getOpenFileDescriptorCount", new Class[]{});
          bean = osBean;
        } else {
          // leave them null
        }
        // Always set ProcessCpuTime
        m3 = osBean.getClass().getMethod("getProcessCpuTime", new Class[]{});
        if (m3!=null) {
          m3.setAccessible( true );
        }
      }
      catch (VirtualMachineError err) {
	SystemFailure.initiateFailure(err);
	// If this ever returns, rethrow the error.  We're poisoned
	// now, so don't let this thread continue.
	throw err;
      }
     catch (Throwable ex) {
          // Whenever you catch Error or Throwable, you must also
          // catch VirtualMachineError (see above).  However, there is
          // _still_ a possibility that you are dealing with a cascading
          // error condition, so you also need to check to see if the JVM
          // is still usable:
          SystemFailure.checkFailure();
        // must be on a platform that does not support unix mxbean
        bean = null;
        m1 = null;
        m2 = null;
        m3 = null;
      } finally {
        unixBean = bean;
        getMaxFileDescriptorCount = m1;
        getOpenFileDescriptorCount = m2;
        getProcessCpuTime = m3;
      }
    }
    threadBean = ManagementFactory.getThreadMXBean();
    if (THREAD_STATS_ENABLED) {
      if (threadBean.isThreadCpuTimeSupported()) {
        if (!threadBean.isThreadCpuTimeEnabled()) {
          if (Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "enableCpuTime")) {
            threadBean.setThreadCpuTimeEnabled(true);
          }
        }
      }
      if (threadBean.isThreadContentionMonitoringSupported()) {
        if (!threadBean.isThreadContentionMonitoringEnabled()) {
          if (Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "enableContentionTime")) {
            threadBean.setThreadContentionMonitoringEnabled(true);
          }
        }
      }
    }
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
    List<StatisticDescriptor> sds = new ArrayList<StatisticDescriptor>();
    sds.add(f.createIntGauge("pendingFinalization",
                             "Number of objects that are pending finalization in the java VM.",
                             "objects"));
    sds.add(f.createIntGauge("daemonThreads",
                             "Current number of live daemon threads in this VM.",
                             "threads"));
    sds.add(f.createIntGauge("threads",
                             "Current number of live threads (both daemon and non-daemon) in this VM.",
                             "threads"));
    sds.add(f.createIntGauge("peakThreads",
                             "High water mark of live threads in this VM.",
                             "threads"));
    sds.add(f.createLongCounter("threadStarts",
                                "Total number of times a thread has been started since this vm started.",
                                "threads"));
    sds.add(f.createIntGauge("cpus",
                             "Number of cpus available to the java VM on its machine.",
                             "cpus", true));
    sds.add(f.createLongCounter("loadedClasses",
                                "Total number of classes loaded since vm started.",
                                "classes"));
    sds.add(f.createLongCounter("unloadedClasses",
                                "Total number of classes unloaded since vm started.",
                                "classes", true));
    sds.add(f.createLongGauge("freeMemory",
                              "An approximation fo the total amount of memory currently available for future allocated objects, measured in bytes.",
                              "bytes", true));
    sds.add(f.createLongGauge("totalMemory",
                              "The total amount of memory currently available for current and future objects, measured in bytes.",
                              "bytes"));
    sds.add(f.createLongGauge("maxMemory",
                              "The maximum amount of memory that the VM will attempt to use, measured in bytes.",
                              "bytes", true));
   sds.add(f.createLongCounter("processCpuTime",
                              "CPU timed used by the process in nanoseconds.",
                              "nanoseconds"));
    if (unixBean != null) {
      sds.add(f.createLongGauge("fdLimit",
                                "Maximum number of file descriptors",
                                "fds", true));
      sds.add(f.createLongGauge("fdsOpen",
                                "Current number of open file descriptors",
                                "fds"));
    }
    vmType = f.createType("VMStats",
                          "Stats available on a 1.5 java virtual machine.",
                          sds.toArray(new StatisticDescriptor[sds.size()]));
    pendingFinalizationCountId = vmType.nameToId("pendingFinalization");
    loadedClassesId = vmType.nameToId("loadedClasses");
    unloadedClassesId = vmType.nameToId("unloadedClasses");
    daemonThreadsId = vmType.nameToId("daemonThreads");
    peakThreadsId = vmType.nameToId("peakThreads");
    threadsId = vmType.nameToId("threads");
    threadStartsId = vmType.nameToId("threadStarts");
    cpusId = vmType.nameToId("cpus");
    freeMemoryId = vmType.nameToId("freeMemory");
    totalMemoryId = vmType.nameToId("totalMemory");
    maxMemoryId = vmType.nameToId("maxMemory");
    processCpuTimeId = vmType.nameToId("processCpuTime");
    if (unixBean != null) {
      unix_fdLimitId = vmType.nameToId("fdLimit");
      unix_fdsOpenId = vmType.nameToId("fdsOpen");
    } else {
      unix_fdLimitId = -1;
      unix_fdsOpenId = -1;
    }

    memoryUsageType = f.createType("VMMemoryUsageStats",
                          "Stats available on a 1.5 memory usage area",
                          new StatisticDescriptor[] {
                            f.createLongGauge("initMemory",
                                              "Initial memory the vm requested from the operating system for this area",
                                              "bytes"),
                            f.createLongGauge("maxMemory",
                                              "The maximum amount of memory this area can have in bytes.",
                                              "bytes"),
                            f.createLongGauge("usedMemory",
                                              "The amount of used memory for this area, measured in bytes.",
                                              "bytes"),
                            f.createLongGauge("committedMemory",
                                              "The amount of committed memory for this area, measured in bytes.",
                                              "bytes")
                          });
    mu_initMemoryId = memoryUsageType.nameToId("initMemory");
    mu_maxMemoryId = memoryUsageType.nameToId("maxMemory");
    mu_usedMemoryId = memoryUsageType.nameToId("usedMemory");
    mu_committedMemoryId = memoryUsageType.nameToId("committedMemory");

    gcType = f.createType("VMGCStats",
                          "Stats available on a 1.5 garbage collector",
                          new StatisticDescriptor[] {
                            f.createLongCounter("collections",
                                              "Total number of collections this garbage collector has done.",
                                              "operations"),
                            f.createLongCounter("collectionTime",
                                              "Approximate elapsed time spent doing collections by this garbage collector.",
                                              "milliseconds"),
                          });
    gc_collectionsId = gcType.nameToId("collections");
    gc_collectionTimeId = gcType.nameToId("collectionTime");

    mpType = f.createType("VMMemoryPoolStats",
                          "Stats available on a 1.5 memory pool",
                          new StatisticDescriptor[] {
                            f.createLongGauge("currentInitMemory",
                                              "Initial memory the vm requested from the operating system for this pool",
                                              "bytes"),
                            f.createLongGauge("currentMaxMemory",
                                              "The maximum amount of memory this pool can have in bytes.",
                                              "bytes"),
                            f.createLongGauge("currentUsedMemory",
                                              "The estimated amount of used memory currently in use for this pool, measured in bytes.",
                                              "bytes"),
                            f.createLongGauge("currentCommittedMemory",
                                              "The amount of committed memory for this pool, measured in bytes.",
                                              "bytes"),
//                             f.createLongGauge("collectionInitMemory",
//                                               "Initial memory the vm requested from the operating system for this pool",
//                                               "bytes"),
//                             f.createLongGauge("collectionMaxMemory",
//                                               "The maximum amount of memory this pool can have in bytes.",
//                                               "bytes"),
                            f.createLongGauge("collectionUsedMemory",
                                              "The estimated amount of used memory after that last garbage collection of this pool, measured in bytes.",
                                              "bytes"),
//                             f.createLongGauge("collectionCommittedMemory",
//                                               "The amount of committed memory for this pool, measured in bytes.",
//                                               "bytes"),
                            f.createLongGauge("collectionUsageThreshold",
                                              "The collection usage threshold for this pool in bytes",
                                              "bytes"),
                            f.createLongCounter("collectionUsageExceeded",
                                                "Total number of times the garbage collector detected that memory usage in this pool exceeded the collectionUsageThreshold",
                                                "exceptions"),
                            f.createLongGauge("usageThreshold",
                                              "The usage threshold for this pool in bytes",
                                              "bytes"),
                            f.createLongCounter("usageExceeded",
                                                "Total number of times that memory usage in this pool exceeded the usageThreshold",
                                                "exceptions")
                          });
    mp_l_initMemoryId = mpType.nameToId("currentInitMemory");
    mp_l_maxMemoryId = mpType.nameToId("currentMaxMemory");
    mp_l_usedMemoryId = mpType.nameToId("currentUsedMemory");
    mp_l_committedMemoryId = mpType.nameToId("currentCommittedMemory");
    //mp_gc_initMemoryId = mpType.nameToId("collectionInitMemory");
    //mp_gc_maxMemoryId = mpType.nameToId("collectionMaxMemory");
    mp_gc_usedMemoryId = mpType.nameToId("collectionUsedMemory");
    //mp_gc_committedMemoryId = mpType.nameToId("collectionCommittedMemory");
    mp_usageThresholdId = mpType.nameToId("usageThreshold");
    mp_collectionUsageThresholdId = mpType.nameToId("collectionUsageThreshold");
    mp_usageExceededId = mpType.nameToId("usageExceeded");
    mp_collectionUsageExceededId = mpType.nameToId("collectionUsageExceeded");

    if (THREAD_STATS_ENABLED) {
      threadType = f.createType("VMThreadStats",
                                "Stats available on a 1.5 thread",
                                new StatisticDescriptor[] {
                                  f.createLongCounter("blocked",
                                                      "Total number of times this thread blocked to enter or reenter a monitor",
                                                      "operations"),
                                  f.createLongCounter("blockedTime",
                                                      "Total amount of elapsed time, approximately, that this thread has spent blocked to enter or reenter a monitor. May need to be enabled by setting -Dgemfire.enableContentionTime=true",
                                                      "milliseconds"),
                                  f.createLongGauge("lockOwner",
                                                    "The thread id that owns the lock that this thread is blocking on.",
                                                    "threadId"),
                                  f.createIntGauge("inNative",
                                                   "1 if the thread is in native code.",
                                                   "boolean"),
                                  f.createIntGauge("suspended",
                                                   "1 if this thread is suspended",
                                                   "boolean"),
                                  f.createLongCounter("waited",
                                                      "Total number of times this thread waited for notification.",
                                                      "operations"),
                                  f.createLongCounter("waitedTime",
                                                      "Total amount of elapsed time, approximately, that this thread has spent waiting for notification. May need to be enabled by setting -Dgemfire.enableContentionTime=true",
                                                      "milliseconds"),
                                  f.createLongCounter("cpuTime",
                                                      "Total cpu time for this thread.  May need to be enabled by setting -Dgemfire.enableCpuTime=true.",
                                                      "nanoseconds"),
                                  f.createLongCounter("userTime",
                                                      "Total user time for this thread. May need to be enabled by setting -Dgemfire.enableCpuTime=true.",
                                                      "nanoseconds"),
                                });
      thread_blockedId = threadType.nameToId("blocked");
      thread_waitedId = threadType.nameToId("waited");
      thread_lockOwnerId = threadType.nameToId("lockOwner");
      thread_inNativeId = threadType.nameToId("inNative");
      thread_suspendedId = threadType.nameToId("suspended");
      thread_blockedTimeId = threadType.nameToId("blockedTime");
      thread_waitedTimeId = threadType.nameToId("waitedTime");
      thread_cpuTimeId = threadType.nameToId("cpuTime");
      thread_userTimeId = threadType.nameToId("userTime");
    } else {
      threadType = null;
      thread_blockedId = -1;
      thread_waitedId = -1;
      thread_lockOwnerId = -1;
      thread_inNativeId = -1;
      thread_suspendedId = -1;
      thread_blockedTimeId = -1;
      thread_waitedTimeId = -1;
      thread_cpuTimeId = -1;
      thread_userTimeId = -1;
    }
  }
  
  private final Statistics vmStats;
  private final Statistics heapMemStats;
  private final Statistics nonHeapMemStats;

  private final StatisticsFactory f;
  private long id;

  public VMStats50(StatisticsFactory f, long id) {
    this.f = f;
    this.id = id;
    this.vmStats = f.createStatistics(vmType, "vmStats", id);
    this.heapMemStats = f.createStatistics(memoryUsageType, "vmHeapMemoryStats", id);
    this.nonHeapMemStats = f.createStatistics(memoryUsageType, "vmNonHeapMemoryStats", id);
    initMemoryPools();  //Fix for #40424
    initGC();
  }

  private boolean newThreadsStarted() {
    long curStarts = threadBean.getTotalStartedThreadCount();
    return curStarts > this.threadStartCount;
  }
  private void refreshThreads() {
    if (!THREAD_STATS_ENABLED) {
      return;
    }
    if (this.allThreadIds == null || newThreadsStarted()) {
      this.allThreadIds = threadBean.getAllThreadIds();
      this.threadStartCount = threadBean.getTotalStartedThreadCount();
    }
    ThreadInfo[] threadInfos = threadBean.getThreadInfo(this.allThreadIds, 0);
    for (int i=0; i < threadInfos.length; i++) {
      long id = this.allThreadIds[i];
      ThreadInfo item = threadInfos[i];
      if (item != null) {
        ThreadStatInfo tsi = threadMap.get(id);
        if (tsi == null) {
          threadMap.put(id, new ThreadStatInfo(item, this.f.createStatistics(threadType, item.getThreadName()+'-'+ item.getThreadId(), this.id)));
        } else {
          tsi.ti = item;
        }
      } else {
        ThreadStatInfo tsi = threadMap.remove(id);
        if (tsi != null) {
          tsi.s.close();
        }
      }
    }
    Iterator<Map.Entry<Long,ThreadStatInfo>> it = threadMap.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<Long,ThreadStatInfo> me = it.next();
      long id = me.getKey();
      ThreadStatInfo tsi = me.getValue();
      ThreadInfo ti = tsi.ti;
      Statistics s = tsi.s;
      s.setLong(thread_blockedId, ti.getBlockedCount());
      s.setLong(thread_lockOwnerId, ti.getLockOwnerId());
      s.setLong(thread_waitedId, ti.getWaitedCount());
      s.setInt(thread_inNativeId, ti.isInNative()?1:0);
      s.setInt(thread_suspendedId, ti.isSuspended()?1:0);
      if (threadBean.isThreadContentionMonitoringSupported()
          && threadBean.isThreadContentionMonitoringEnabled()) {
        s.setLong(thread_blockedTimeId, ti.getBlockedTime());
        s.setLong(thread_waitedTimeId, ti.getWaitedTime());
      }
      if (threadBean.isThreadCpuTimeSupported() && threadBean.isThreadCpuTimeEnabled()) {
        s.setLong(thread_cpuTimeId, threadBean.getThreadCpuTime(id));
        s.setLong(thread_userTimeId, threadBean.getThreadUserTime(id));
      }
    }
  }
  /**
   * This set is used to workaround a JRockit bug 36348 in which getCollectionUsage
   * throws OperationUnsupportedException instead of returning null.
   */
  private final HashSet<String> collectionUsageUnsupported = new HashSet<String>();
  /**
   * Returns true if collection usage is not supported on the given bean.
   */
  private boolean isCollectionUsageUnsupported(MemoryPoolMXBean mp) {
    return this.collectionUsageUnsupported.contains(mp.getName());
  }
  /**
   * Remember that the given bean does not support collection usage.
   */
  private void setCollectionUsageUnsupported(MemoryPoolMXBean mp) {
    this.collectionUsageUnsupported.add(mp.getName());
  }

  private void initMemoryPools() {
    List<MemoryPoolMXBean> l = ManagementFactory.getMemoryPoolMXBeans();
    for (MemoryPoolMXBean item : l) {
      if (item.isValid() && !mpMap.containsKey(item)) {
        mpMap.put(item, this.f.createStatistics(mpType, item.getName()+'-'+item.getType(), this.id));
      }
    }
  }

  private void refreshMemoryPools() {
    Iterator<Map.Entry<MemoryPoolMXBean,Statistics>> it = mpMap.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<MemoryPoolMXBean,Statistics> me = it.next();
      MemoryPoolMXBean mp = me.getKey();
      Statistics s = me.getValue();
      if (!mp.isValid()) {
        s.close();
        it.remove();
      } else {
        MemoryUsage mu = null;
        try {
          mu = mp.getUsage();
        } catch (IllegalArgumentException ex) {
          // to workaround JRockit bug 36348 just ignore this and try the next pool
          continue;
        }
        s.setLong(mp_l_initMemoryId, mu.getInit());
        s.setLong(mp_l_usedMemoryId, mu.getUsed());
        s.setLong(mp_l_committedMemoryId, mu.getCommitted());
        s.setLong(mp_l_maxMemoryId, mu.getMax());
        if (mp.isUsageThresholdSupported()) {
          s.setLong(mp_usageThresholdId, mp.getUsageThreshold());
          s.setLong(mp_usageExceededId, mp.getUsageThresholdCount());
        }
        mu = null;
        if (!this.isCollectionUsageUnsupported(mp)) {
          try {
            mu = mp.getCollectionUsage();
          } catch (UnsupportedOperationException ex) {
            // JRockit throws this exception instead of returning null
            // as the javadocs say it should. See bug 36348
            this.setCollectionUsageUnsupported(mp);
          } catch (IllegalArgumentException ex) {
            // Yet another JRockit bug in which its code catches an assertion
            // about the state of their bean stat values being inconsistent.
            //  See bug 36348.
            this.setCollectionUsageUnsupported(mp);
          }
        }
        if (mu != null) {
          //s.setLong(mp_gc_initMemoryId, mu.getInit());
          s.setLong(mp_gc_usedMemoryId, mu.getUsed());
          //s.setLong(mp_gc_committedMemoryId, mu.getCommitted());
          //s.setLong(mp_gc_maxMemoryId, mu.getMax());
          if (mp.isCollectionUsageThresholdSupported()) {
            s.setLong(mp_collectionUsageThresholdId, mp.getCollectionUsageThreshold());
            s.setLong(mp_collectionUsageExceededId, mp.getCollectionUsageThresholdCount());
          }
        }
      }
    }
  }

  private void initGC() {
    List<GarbageCollectorMXBean> l = ManagementFactory.getGarbageCollectorMXBeans();
    for (GarbageCollectorMXBean item : l) {
      if (item.isValid() && !gcMap.containsKey(item)) {
        gcMap.put(item, this.f.createStatistics(gcType, item.getName(), this.id));
      }
    }
  }

  private void refreshGC() {
    Iterator<Map.Entry<GarbageCollectorMXBean,Statistics>> it = gcMap.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<GarbageCollectorMXBean,Statistics> me = it.next();
      GarbageCollectorMXBean gc = me.getKey();
      Statistics s = me.getValue();
      if (!gc.isValid()) {
        s.close();
        it.remove();
      } else {
        s.setLong(gc_collectionsId, gc.getCollectionCount());
        s.setLong(gc_collectionTimeId, gc.getCollectionTime());
      }
    }
  }
  
  public void refresh() {
    Runtime rt = Runtime.getRuntime();
    this.vmStats.setInt(pendingFinalizationCountId, memBean.getObjectPendingFinalizationCount());
    this.vmStats.setInt(cpusId, osBean.getAvailableProcessors());
    this.vmStats.setInt(threadsId, threadBean.getThreadCount());
    this.vmStats.setInt(daemonThreadsId, threadBean.getDaemonThreadCount());
    this.vmStats.setInt(peakThreadsId, threadBean.getPeakThreadCount());
    this.vmStats.setLong(threadStartsId, threadBean.getTotalStartedThreadCount());
    this.vmStats.setLong(loadedClassesId, clBean.getTotalLoadedClassCount());
    this.vmStats.setLong(unloadedClassesId, clBean.getUnloadedClassCount());
    this.vmStats.setLong(freeMemoryId, rt.freeMemory());
    this.vmStats.setLong(totalMemoryId, rt.totalMemory());
    this.vmStats.setLong(maxMemoryId, rt.maxMemory());

    // Compute processCpuTime separately, if not accessible ignore
    try {
      if (getProcessCpuTime!=null) {
        Object v = getProcessCpuTime.invoke(osBean, new Object[]{});
        this.vmStats.setLong(processCpuTimeId, ((Long)v).longValue());
      }
    }
    catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    }
    catch (Throwable ex) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
    }

    if (unixBean != null) {
      try {
        Object v = getMaxFileDescriptorCount.invoke(unixBean, new Object[]{});
        this.vmStats.setLong(unix_fdLimitId, ((Long)v).longValue());
        v = getOpenFileDescriptorCount.invoke(unixBean, new Object[]{});
        this.vmStats.setLong(unix_fdsOpenId, ((Long)v).longValue());
      }
      catch (VirtualMachineError err) {
	SystemFailure.initiateFailure(err);
	// If this ever returns, rethrow the error.  We're poisoned
	// now, so don't let this thread continue.
	throw err;
      }
     catch (Throwable ex) {
	// Whenever you catch Error or Throwable, you must also
	// catch VirtualMachineError (see above).  However, there is
	// _still_ a possibility that you are dealing with a cascading
	// error condition, so you also need to check to see if the JVM
	// is still usable:
	SystemFailure.checkFailure();
      }
    }

    refresh(this.heapMemStats, memBean.getHeapMemoryUsage());
    refresh(this.nonHeapMemStats, memBean.getNonHeapMemoryUsage());
    refreshGC();
    refreshMemoryPools();
    refreshThreads();
  }
  private void refresh(Statistics stats, MemoryUsage mu) {
    stats.setLong(mu_initMemoryId, mu.getInit());
    stats.setLong(mu_usedMemoryId, mu.getUsed());
    stats.setLong(mu_committedMemoryId, mu.getCommitted());
    stats.setLong(mu_maxMemoryId, mu.getMax());
  }
  
  public void close() {
    this.heapMemStats.close();
    this.nonHeapMemStats.close();
    this.vmStats.close();
    closeStatsMap(this.mpMap);
    closeStatsMap(this.gcMap);
  }
  
  private void closeStatsMap(Map<?, Statistics> map) {
    for (Statistics s : map.values()) {
      s.close();
    }
  }

  private static class ThreadStatInfo {
    public ThreadInfo ti;
    public final Statistics s;
    public ThreadStatInfo(ThreadInfo ti, Statistics s) {
      this.ti = ti;
      this.s = s;
    }
  }
  public static StatisticsType getType() {
    return vmType;
  }
  
  public Statistics getVMStats() {
    return vmStats;
  }

  public Statistics getVMHeapStats() {
    return heapMemStats;
  }

  public Statistics getVMNonHeapStats() {
    return nonHeapMemStats;
  }

  public static StatisticsType getGCType() {
    return gcType;
  }

  public static StatisticsType getMemoryPoolType() {
    return mpType;
  }

  public static StatisticsType getThreadType() {
    return threadType;
  }
  
  public static StatisticsType getMemoryUsageType() {
    return memoryUsageType;
  }
}
