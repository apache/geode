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

import org.apache.geode.Statistics;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.PureJavaMode;
import org.apache.geode.internal.admin.ListenerIdMap;
import org.apache.geode.internal.admin.remote.StatListenerMessage;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.statistics.platform.OsStatisticsFactory;
import org.apache.geode.internal.statistics.platform.ProcessStats;

/**
 * GemFireStatSampler adds listeners and rolling archives to HostStatSampler.
 * <p>
 * The StatisticsManager is implemented by DistributedSystem.
 *
 */
public class GemFireStatSampler extends HostStatSampler {

  private static final Logger logger = LogService.getLogger();

  private final ListenerIdMap listeners = new ListenerIdMap();

  // TODO: change the listener maps to be copy-on-write

  private final Map<LocalStatListenerImpl, Boolean> localListeners =
      new ConcurrentHashMap<LocalStatListenerImpl, Boolean>();

  private final Map<InternalDistributedMember, List<RemoteStatListenerImpl>> recipientToListeners =
      new HashMap<InternalDistributedMember, List<RemoteStatListenerImpl>>();

  private final InternalDistributedSystem con;

  private int nextListenerId = 1;
  private ProcessStats processStats = null;

  ////////////////////// Constructors //////////////////////

  public GemFireStatSampler(InternalDistributedSystem con) {
    super(con.getCancelCriterion(), new StatSamplerStats(con, con.getId()));
    this.con = con;
  }

  /**
   * Returns the <code>ProcessStats</code> for this Java VM. Note that <code>null</code> will be
   * returned if operating statistics are disabled.
   *
   * @since GemFire 3.5
   */
  public ProcessStats getProcessStats() {
    return this.processStats;
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
      RemoteStatListenerImpl sl =
          RemoteStatListenerImpl.create(result, recipient, resourceId, statName, this);
      listeners.put(result, sl);
      List<RemoteStatListenerImpl> l = recipientToListeners.get(recipient);
      if (l == null) {
        l = new ArrayList<RemoteStatListenerImpl>();
        recipientToListeners.put(recipient, l);
      }
      l.add(sl);
    }
    return result;
  }

  public boolean removeListener(int listenerId) {
    synchronized (listeners) {
      RemoteStatListenerImpl sl = (RemoteStatListenerImpl) listeners.remove(listenerId);
      if (sl != null) {
        List<RemoteStatListenerImpl> l = recipientToListeners.get(sl.getRecipient());
        l.remove(sl);
      }
      return sl != null;
    }
  }

  public void removeListenersByRecipient(InternalDistributedMember recipient) {
    synchronized (listeners) {
      List<RemoteStatListenerImpl> l = recipientToListeners.get(recipient);
      if (l != null && l.size() != 0) {
        for (RemoteStatListenerImpl sl : l) {
          listeners.remove(sl.getListenerId());
        }
        recipientToListeners.remove(recipient);
      }
    }
  }

  public void addLocalStatListener(LocalStatListener l, Statistics stats, String statName) {
    LocalStatListenerImpl sl = null;
    synchronized (LocalStatListenerImpl.class) {
      sl = LocalStatListenerImpl.create(l, stats, statName);
    }
    this.localListeners.put(sl, Boolean.TRUE);
  }

  public boolean removeLocalStatListener(LocalStatListener listener) {
    Iterator<Map.Entry<LocalStatListenerImpl, Boolean>> it =
        this.localListeners.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<LocalStatListenerImpl, Boolean> entry = it.next();
      if (listener.equals(entry.getKey().getListener())) {
        it.remove();
        return true;
      }
    }
    return false;
  }

  public Set<LocalStatListenerImpl> getLocalListeners() {
    return this.localListeners.keySet();
  }

  @Override
  public File getArchiveFileName() {
    return this.con.getConfig().getStatisticArchiveFile();
  }

  @Override
  public long getArchiveFileSizeLimit() {
    if (fileSizeLimitInKB()) {
      // use KB instead of MB to speed up rolling for testing
      return ((long) this.con.getConfig().getArchiveFileSizeLimit()) * (1024);
    } else {
      return ((long) this.con.getConfig().getArchiveFileSizeLimit()) * (1024 * 1024);
    }
  }

  @Override
  public long getArchiveDiskSpaceLimit() {
    if (fileSizeLimitInKB()) {
      // use KB instead of MB to speed up removal for testing
      return ((long) this.con.getConfig().getArchiveDiskSpaceLimit()) * (1024);
    } else {
      return ((long) this.con.getConfig().getArchiveDiskSpaceLimit()) * (1024 * 1024);
    }
  }

  @Override
  protected void checkListeners() {
    checkLocalListeners();
    synchronized (listeners) {
      if (listeners.size() == 0) {
        return;
      }
      long timeStamp = System.currentTimeMillis();
      Iterator<Map.Entry<InternalDistributedMember, List<RemoteStatListenerImpl>>> it1 =
          recipientToListeners.entrySet().iterator();
      while (it1.hasNext()) {
        if (stopRequested())
          return;
        Map.Entry<InternalDistributedMember, List<RemoteStatListenerImpl>> me = it1.next();
        List<RemoteStatListenerImpl> l = me.getValue();
        if (l.size() > 0) {
          InternalDistributedMember recipient = (InternalDistributedMember) me.getKey();
          StatListenerMessage msg = StatListenerMessage.create(timeStamp, l.size());
          msg.setRecipient(recipient);
          for (RemoteStatListenerImpl statListener : l) {
            if (getStatisticsManager().statisticsExists(statListener.getStatId())) {
              statListener.checkForChange(msg);
            } else {
              // its stale; indicate this with a negative listener id
              // fix for bug 29405
              msg.addChange(-statListener.getListenerId(), 0);
            }
          }
          this.con.getDistributionManager().putOutgoing(msg);
        }
      }
    }
  }

  @Override
  protected int getSampleRate() {
    return this.con.getConfig().getStatisticSampleRate();
  }

  @Override
  public boolean isSamplingEnabled() {
    return this.con.getConfig().getStatisticSamplingEnabled();
  }

  @Override
  protected StatisticsManager getStatisticsManager() {
    return this.con;
  }

  @Override
  protected OsStatisticsFactory getOsStatisticsFactory() {
    return this.con;
  }

  @Override
  protected long getSpecialStatsId() {
    long statId = OSProcess.getId();
    if (statId == 0 || statId == -1) {
      statId = getStatisticsManager().getId();
    }
    return statId;
  }

  @Override
  protected void initProcessStats(long id) {
    if (PureJavaMode.osStatsAreAvailable()) {
      if (osStatsDisabled()) {
        logger.info(LogMarker.STATISTICS_MARKER,
            "OS statistic collection disabled by setting the osStatsDisabled system property to true.");
      } else {
        int retVal = HostStatHelper.initOSStats();
        if (retVal != 0) {
          logger.error(LogMarker.STATISTICS_MARKER,
              "OS statistics failed to initialize properly, some stats may be missing. See bugnote #37160.");
        }
        HostStatHelper.newSystem(getOsStatisticsFactory());
        String statName = getStatisticsManager().getName();
        if (statName == null || statName.length() == 0) {
          statName = "javaApp" + getStatisticsManager().getId();
        }
        Statistics stats =
            HostStatHelper.newProcess(getOsStatisticsFactory(), id, statName + "-proc");
        this.processStats = HostStatHelper.newProcessStats(stats);
      }
    }
  }

  @Override
  protected void sampleProcessStats(boolean prepareOnly) {
    if (prepareOnly || osStatsDisabled() || !PureJavaMode.osStatsAreAvailable()) {
      return;
    }
    List<Statistics> l = getStatisticsManager().getStatsList();
    if (l == null) {
      return;
    }
    if (stopRequested())
      return;
    HostStatHelper.readyRefreshOSStats();
    Iterator<Statistics> it = l.iterator();
    while (it.hasNext()) {
      if (stopRequested())
        return;
      StatisticsImpl s = (StatisticsImpl) it.next();
      if (s.usesSystemCalls()) {
        HostStatHelper.refresh((LocalStatisticsImpl) s);
      }
    }
  }

  @Override
  protected void closeProcessStats() {
    if (PureJavaMode.osStatsAreAvailable()) {
      if (!osStatsDisabled()) {
        if (this.processStats != null) {
          this.processStats.close();
        }
        HostStatHelper.closeOSStats();
      }
    }
  }

  private void checkLocalListeners() {
    for (LocalStatListenerImpl st : this.localListeners.keySet()) {
      if (getStatisticsManager().statisticsExists(st.getStatId())) {
        st.checkForChange();
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
    protected boolean oldValueInitialized = false;
    protected long oldValue;

    public long getStatId() {
      if (this.stats.isClosed()) {
        return -1;
      } else {
        return this.stats.getUniqueId();
      }
    }

    protected abstract double getBitsAsDouble(long bits);
  }

  protected abstract static class LocalStatListenerImpl extends StatListenerImpl {
    private LocalStatListener listener;

    public LocalStatListener getListener() {
      return this.listener;
    }

    static LocalStatListenerImpl create(LocalStatListener l, Statistics stats, String statName) {
      LocalStatListenerImpl result = null;
      StatisticDescriptorImpl stat = (StatisticDescriptorImpl) stats.nameToDescriptor(statName);
      switch (stat.getTypeCode()) {
        case StatisticDescriptorImpl.BYTE:
        case StatisticDescriptorImpl.SHORT:
        case StatisticDescriptorImpl.INT:
        case StatisticDescriptorImpl.LONG:
          result = new LocalLongStatListenerImpl();
          break;
        case StatisticDescriptorImpl.FLOAT:
          result = new LocalFloatStatListenerImpl();
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

  protected static class LocalFloatStatListenerImpl extends LocalStatListenerImpl {
    @Override
    protected double getBitsAsDouble(long bits) {
      return Float.intBitsToFloat((int) bits);
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
      return this.listenerId;
    }

    public InternalDistributedMember getRecipient() {
      return this.recipient;
    }

    static RemoteStatListenerImpl create(int listenerId, InternalDistributedMember recipient,
        long resourceId, String statName, HostStatSampler sampler) {
      RemoteStatListenerImpl result = null;
      Statistics stats = sampler.getStatisticsManager().findStatistics(resourceId);
      StatisticDescriptorImpl stat = (StatisticDescriptorImpl) stats.nameToDescriptor(statName);
      switch (stat.getTypeCode()) {
        case StatisticDescriptorImpl.BYTE:
        case StatisticDescriptorImpl.SHORT:
        case StatisticDescriptorImpl.INT:
        case StatisticDescriptorImpl.LONG:
          result = new LongStatListenerImpl();
          break;
        case StatisticDescriptorImpl.FLOAT:
          result = new FloatStatListenerImpl();
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

  protected static class FloatStatListenerImpl extends RemoteStatListenerImpl {
    @Override
    protected double getBitsAsDouble(long bits) {
      return Float.intBitsToFloat((int) bits);
    }
  }

  protected static class DoubleStatListenerImpl extends RemoteStatListenerImpl {
    @Override
    protected double getBitsAsDouble(long bits) {
      return Double.longBitsToDouble(bits);
    }
  }
}
