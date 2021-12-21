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
package org.apache.geode.internal.statistics;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.Statistics;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.admin.ListenerIdMap;
import org.apache.geode.internal.admin.remote.StatListenerMessage;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.statistics.platform.OsStatisticsFactory;
import org.apache.geode.internal.statistics.platform.ProcessStats;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.logging.internal.spi.LogFile;

/**
 * GemFireStatSampler adds listeners and rolling archives to HostStatSampler.
 * <p>
 * The StatisticsManager is implemented by DistributedSystem.
 */
public class GemFireStatSampler extends HostStatSampler {

  private static final Logger logger = LogService.getLogger();

  private final ListenerIdMap listeners = new ListenerIdMap();

  // TODO: change the listener maps to be copy-on-write

  private final Map<LocalStatListenerImpl, Boolean> localListeners = new ConcurrentHashMap<>();

  private final Map<InternalDistributedMember, List<RemoteStatListenerImpl>> recipientToListeners =
      new HashMap<>();

  private final long systemId;
  private final StatisticsConfig statisticsConfig;
  private final StatisticsManager statisticsManager;
  private final DistributionManager distributionManager;

  private int nextListenerId = 1;
  private ProcessStats processStats;

  private final OsStatisticsProvider osStatisticsProvider = OsStatisticsProvider.build();

  public GemFireStatSampler(InternalDistributedSystem internalDistributedSystem) {
    this(internalDistributedSystem, null);
  }

  public GemFireStatSampler(InternalDistributedSystem internalDistributedSystem, LogFile logFile) {
    this(internalDistributedSystem.getCancelCriterion(),
        new StatSamplerStats(internalDistributedSystem,
            internalDistributedSystem.getStatisticsManager().getPid()),
        logFile,
        internalDistributedSystem.getStatisticsConfig(),
        internalDistributedSystem.getStatisticsManager(),
        internalDistributedSystem.getDistributionManager(),
        internalDistributedSystem.getId());
  }

  @VisibleForTesting
  public GemFireStatSampler(CancelCriterion cancelCriterion,
      StatSamplerStats statSamplerStats,
      LogFile logFile,
      StatisticsConfig statisticsConfig,
      StatisticsManager statisticsManager,
      DistributionManager distributionManager,
      long systemId) {
    super(cancelCriterion, statSamplerStats, logFile);
    this.systemId = systemId;
    this.statisticsConfig = statisticsConfig;
    this.statisticsManager = statisticsManager;
    this.distributionManager = distributionManager;
  }

  /**
   * Returns the <code>ProcessStats</code> for this Java VM. Note that <code>null</code> will be
   * returned if operating statistics are disabled.
   *
   * @since GemFire 3.5
   */
  public ProcessStats getProcessStats() {
    return processStats;
  }

  @Override
  public String getProductDescription() {
    return "GemFire " + GemFireVersion.getGemFireVersion() + " #" + GemFireVersion.getBuildId()
        + " as of " + GemFireVersion.getSourceDate();
  }

  public int addListener(InternalDistributedMember recipient, long resourceId, String statName) {
    int result = getNextListenerId();
    synchronized (listeners) {
      while (listeners.get(result) != null) {
        // previous one was still being used
        result = getNextListenerId();
      }
      RemoteStatListenerImpl remoteStatListener =
          RemoteStatListenerImpl.create(result, recipient, resourceId, statName, this);
      listeners.put(result, remoteStatListener);
      List<RemoteStatListenerImpl> remoteStatListenerList =
          recipientToListeners.computeIfAbsent(recipient, k -> new ArrayList<>());
      remoteStatListenerList.add(remoteStatListener);
    }
    return result;
  }

  public boolean removeListener(int listenerId) {
    synchronized (listeners) {
      RemoteStatListenerImpl remoteStatListener =
          (RemoteStatListenerImpl) listeners.remove(listenerId);
      if (remoteStatListener != null) {
        List<RemoteStatListenerImpl> remoteStatListenerList =
            recipientToListeners.get(remoteStatListener.getRecipient());
        remoteStatListenerList.remove(remoteStatListener);
      }
      return remoteStatListener != null;
    }
  }

  public void removeListenersByRecipient(InternalDistributedMember recipient) {
    synchronized (listeners) {
      List<RemoteStatListenerImpl> remoteStatListenerList = recipientToListeners.get(recipient);
      if (remoteStatListenerList != null && remoteStatListenerList.size() != 0) {
        for (RemoteStatListenerImpl sl : remoteStatListenerList) {
          listeners.remove(sl.getListenerId());
        }
        recipientToListeners.remove(recipient);
      }
    }
  }

  public void addLocalStatListener(LocalStatListener l, Statistics stats, String statName) {
    LocalStatListenerImpl localStatListener;
    synchronized (LocalStatListenerImpl.class) {
      localStatListener = LocalStatListenerImpl.create(l, stats, statName);
    }
    localListeners.put(localStatListener, Boolean.TRUE);
  }

  public boolean removeLocalStatListener(LocalStatListener listener) {
    Iterator<Map.Entry<LocalStatListenerImpl, Boolean>> iterator =
        localListeners.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<LocalStatListenerImpl, Boolean> entry = iterator.next();
      if (listener.equals(entry.getKey().getListener())) {
        iterator.remove();
        return true;
      }
    }
    return false;
  }

  public Set<LocalStatListenerImpl> getLocalListeners() {
    return localListeners.keySet();
  }

  @Override
  public File getArchiveFileName() {
    return statisticsConfig.getStatisticArchiveFile();
  }

  @Override
  public long getArchiveFileSizeLimit() {
    if (fileSizeLimitInKB()) {
      // use KB instead of MB to speed up rolling for testing
      return (long) statisticsConfig.getArchiveFileSizeLimit() * 1024;
    } else {
      return (long) statisticsConfig.getArchiveFileSizeLimit() * (1024 * 1024);
    }
  }

  @Override
  public long getArchiveDiskSpaceLimit() {
    if (fileSizeLimitInKB()) {
      // use KB instead of MB to speed up removal for testing
      return (long) statisticsConfig.getArchiveDiskSpaceLimit() * 1024;
    } else {
      return (long) statisticsConfig.getArchiveDiskSpaceLimit() * (1024 * 1024);
    }
  }

  @Override
  public long getSystemId() {
    return systemId;
  }

  @Override
  protected void checkListeners() {
    checkLocalListeners();
    synchronized (listeners) {
      if (listeners.size() == 0) {
        return;
      }
      long timeStamp = System.currentTimeMillis();
      for (Map.Entry<InternalDistributedMember, List<RemoteStatListenerImpl>> internalDistributedMemberListEntry : recipientToListeners
          .entrySet()) {
        if (stopRequested()) {
          return;
        }
        Map.Entry<InternalDistributedMember, List<RemoteStatListenerImpl>> entry =
            internalDistributedMemberListEntry;
        List<RemoteStatListenerImpl> remoteStatListenerList = entry.getValue();
        if (remoteStatListenerList.size() > 0) {
          InternalDistributedMember recipient = entry.getKey();
          StatListenerMessage statListenerMessage =
              StatListenerMessage.create(timeStamp, remoteStatListenerList.size());
          statListenerMessage.setRecipient(recipient);
          for (RemoteStatListenerImpl statListener : remoteStatListenerList) {
            if (getStatisticsManager().statisticsExists(statListener.getStatId())) {
              statListener.checkForChange(statListenerMessage);
            } else {
              // its stale; indicate this with a negative listener id
              // fix for bug 29405
              statListenerMessage.addChange(-statListener.getListenerId(), 0);
            }
          }
          distributionManager.putOutgoing(statListenerMessage);
        }
      }
    }
  }

  @Override
  protected int getSampleRate() {
    return statisticsConfig.getStatisticSampleRate();
  }

  @Override
  public boolean isSamplingEnabled() {
    return statisticsConfig.getStatisticSamplingEnabled();
  }

  @Override
  protected StatisticsManager getStatisticsManager() {
    return statisticsManager;
  }

  @Override
  protected OsStatisticsFactory getOsStatisticsFactory() {
    return statisticsManager;
  }

  @Override
  protected void initProcessStats(long id) {
    if (osStatisticsProvider.osStatsSupported()) {
      if (osStatsDisabled()) {
        logger.info(LogMarker.STATISTICS_MARKER,
            "OS statistic collection disabled by setting the osStatsDisabled system property to true.");
      } else {
        int retVal = osStatisticsProvider.initOSStats();
        if (retVal != 0) {
          logger.error(LogMarker.STATISTICS_MARKER,
              "OS statistics failed to initialize properly, some stats may be missing. See bugnote #37160.");
        }
        osStatisticsProvider.newSystem(getOsStatisticsFactory(), id);
        String statName = getStatisticsManager().getName();
        if (statName == null || statName.length() == 0) {
          statName = "javaApp" + getSystemId();
        }
        Statistics stats =
            osStatisticsProvider.newProcess(getOsStatisticsFactory(), id, statName + "-proc");
        processStats = osStatisticsProvider.newProcessStats(stats);
      }
    }
  }

  @Override
  protected void sampleProcessStats(boolean prepareOnly) {
    if (prepareOnly || osStatsDisabled() || !osStatisticsProvider.osStatsSupported()) {
      return;
    }
    List<Statistics> statisticsList = getStatisticsManager().getStatsList();
    if (statisticsList == null) {
      return;
    }
    if (stopRequested()) {
      return;
    }
    osStatisticsProvider.readyRefreshOSStats();
    for (Statistics statistics : statisticsList) {
      if (stopRequested()) {
        return;
      }
      StatisticsImpl statisticsImpl = (StatisticsImpl) statistics;
      if (statisticsImpl.usesSystemCalls()) {
        osStatisticsProvider.refresh((LocalStatisticsImpl) statisticsImpl);
      }
    }
  }

  @Override
  protected void closeProcessStats() {
    if (osStatisticsProvider.osStatsSupported()) {
      if (!osStatsDisabled()) {
        if (processStats != null) {
          processStats.close();
        }
        osStatisticsProvider.closeOSStats();
      }
    }
  }

  private void checkLocalListeners() {
    for (LocalStatListenerImpl localStatListener : localListeners.keySet()) {
      if (getStatisticsManager().statisticsExists(localStatListener.getStatId())) {
        localStatListener.checkForChange();
      }
    }
  }

  private int getNextListenerId() {
    int result = nextListenerId++;
    if (nextListenerId < 0) {
      nextListenerId = 1;
    }
    return result;
  }

  protected abstract static class StatListenerImpl {
    protected Statistics stats;
    protected StatisticDescriptorImpl stat;
    protected boolean oldValueInitialized;
    protected long oldValue;

    public long getStatId() {
      if (stats.isClosed()) {
        return -1;
      } else {
        return stats.getUniqueId();
      }
    }

    protected abstract double getBitsAsDouble(long bits);
  }

  protected abstract static class LocalStatListenerImpl extends StatListenerImpl {
    private LocalStatListener listener;

    public LocalStatListener getListener() {
      return listener;
    }

    static LocalStatListenerImpl create(LocalStatListener l, Statistics stats, String statName) {
      LocalStatListenerImpl result = null;
      StatisticDescriptorImpl stat = (StatisticDescriptorImpl) stats.nameToDescriptor(statName);
      switch (stat.getTypeCode()) {
        case StatisticDescriptorImpl.LONG:
          result = new LocalLongStatListenerImpl();
          break;
        case StatisticDescriptorImpl.DOUBLE:
          result = new LocalDoubleStatListenerImpl();
          break;
        default:
          throw new RuntimeException("Illegal field type " + stats.getType() + " for statistic");
      }
      result.stats = stats;
      result.stat = stat;
      result.listener = l;
      return result;
    }

    /**
     * Checks to see if the value of the stat has changed. If it has then the local listener is
     * fired
     */
    public void checkForChange() {
      long currentValue = stats.getRawBits(stat);
      if (oldValueInitialized) {
        if (currentValue == oldValue) {
          return;
        }
      } else {
        oldValueInitialized = true;
      }
      oldValue = currentValue;
      listener.statValueChanged(getBitsAsDouble(currentValue));
    }
  }

  protected static class LocalLongStatListenerImpl extends LocalStatListenerImpl {
    @Override
    protected double getBitsAsDouble(long bits) {
      return bits;
    }
  }

  protected static class LocalDoubleStatListenerImpl extends LocalStatListenerImpl {
    @Override
    protected double getBitsAsDouble(long bits) {
      return Double.longBitsToDouble(bits);
    }
  }

  /**
   * Used to register a StatListener.
   */
  protected abstract static class RemoteStatListenerImpl extends StatListenerImpl {
    private int listenerId;
    private InternalDistributedMember recipient;

    @Override
    public int hashCode() {
      return listenerId;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null) {
        return false;
      }
      if (o instanceof RemoteStatListenerImpl) {
        return listenerId == ((RemoteStatListenerImpl) o).listenerId;
      } else {
        return false;
      }
    }

    public int getListenerId() {
      return listenerId;
    }

    public InternalDistributedMember getRecipient() {
      return recipient;
    }

    static RemoteStatListenerImpl create(int listenerId, InternalDistributedMember recipient,
        long resourceId, String statName, HostStatSampler sampler) {
      RemoteStatListenerImpl result = null;
      Statistics stats = sampler.getStatisticsManager().findStatisticsByUniqueId(resourceId);
      if (stats == null) {
        throw new RuntimeException(
            "Could not find statistics instance with unique id " + resourceId);
      }
      StatisticDescriptorImpl stat = (StatisticDescriptorImpl) stats.nameToDescriptor(statName);
      switch (stat.getTypeCode()) {
        case StatisticDescriptorImpl.LONG:
          result = new LongStatListenerImpl();
          break;
        case StatisticDescriptorImpl.DOUBLE:
          result = new DoubleStatListenerImpl();
          break;
        default:
          throw new RuntimeException(
              String.format("Illegal field type %s for statistic",
                  stats.getType()));
      }
      result.stats = stats;
      result.stat = stat;
      result.listenerId = listenerId;
      result.recipient = recipient;
      return result;
    }

    /**
     * Checks to see if the value of the stat has changed. If it has then it adds that change to the
     * specified message.
     */
    public void checkForChange(StatListenerMessage msg) {
      long currentValue = stats.getRawBits(stat);
      if (oldValueInitialized) {
        if (currentValue == oldValue) {
          return;
        }
      } else {
        oldValueInitialized = true;
      }
      oldValue = currentValue;
      msg.addChange(listenerId, getBitsAsDouble(currentValue));
    }
  }

  protected static class LongStatListenerImpl extends RemoteStatListenerImpl {
    @Override
    protected double getBitsAsDouble(long bits) {
      return bits;
    }
  }

  protected static class DoubleStatListenerImpl extends RemoteStatListenerImpl {
    @Override
    protected double getBitsAsDouble(long bits) {
      return Double.longBitsToDouble(bits);
    }
  }
}
