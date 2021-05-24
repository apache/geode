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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.internal.CopyOnWriteHashSet;

/**
 * Adds coarser-grained monitoring of entire Statistics instances.
 * <p/>
 * The following code is an example of how to find an existing Statistics instance and define a
 * ValueMonitor listener for notifications of any stat sample containing updates to any stats in the
 * instance being monitored:
 *
 * <pre>
 * StatisticsFactory factory = InternalDistributedSystem.getAnyInstance(); Statistics[] statistics =
 * f.findStatisticsByTextId("statSampler"); if (statistics.length == 1) { ValueMonitor monitor = new
 * ValueMonitor().addStatistics(statistics[0]);
 *
 * monitor.addListener(new StatisticsListener() { public void
 * handleNotification(StatisticsNotification notify) { System.out.println("One or more statSampler
 * stats changed at " + notify.getTimeStamp()); for (StatisticId statId : notify) {
 * System.out.println("\t" + statId.getStatisticDescriptor().getName() + " = " +
 * notify.getValue(statId)); } } }; }
 *
 * @since GemFire 7.0
 * @see org.apache.geode.Statistics
 */
public class ValueMonitor extends StatisticsMonitor {

  public enum Type {
    CHANGE, MATCH, DIFFER
  }

  private final CopyOnWriteHashSet<Statistics> statistics = new CopyOnWriteHashSet<>();

  public ValueMonitor() {
    super();
  }

  @Override
  public ValueMonitor addStatistic(final StatisticId statId) {
    super.addStatistic(statId);
    return this;
  }

  @Override
  public ValueMonitor removeStatistic(final StatisticId statId) {
    super.removeStatistic(statId);
    return this;
  }

  public ValueMonitor addStatistics(final Statistics statistics) {
    if (statistics == null) {
      throw new NullPointerException("Statistics is null");
    }
    this.statistics.add(statistics);
    return this;
  }

  public ValueMonitor removeStatistics(final Statistics statistics) {
    if (statistics == null) {
      throw new NullPointerException("Statistics is null");
    }
    this.statistics.remove(statistics);
    return this;
  }

  @Override
  protected void monitor(final long millisTimeStamp,
      final List<ResourceInstance> resourceInstances) {
    super.monitor(millisTimeStamp, resourceInstances);
    monitorStatistics(millisTimeStamp, resourceInstances);
  }

  protected void monitorStatistics(final long millisTimeStamp,
      final List<ResourceInstance> resourceInstances) {
    if (!this.statistics.isEmpty()) {
      Map<StatisticId, Number> stats = new HashMap<>();
      for (ResourceInstance resource : resourceInstances) {
        if (this.statistics.contains(resource.getStatistics())) {
          ResourceType resourceType = resource.getResourceType();
          StatisticDescriptor[] sds = resourceType.getStatisticDescriptors();
          resource.setStatValuesNotified(true);
          int[] updatedStats = resource.getUpdatedStats();
          for (int i = 0; i < updatedStats.length; i++) {
            int idx = updatedStats[i];
            StatisticDescriptorImpl sdi = (StatisticDescriptorImpl) sds[idx];
            SimpleStatisticId statId = new SimpleStatisticId(sdi, resource.getStatistics());
            long rawbits = resource.getLatestStatValues()[idx];
            stats.put(statId, sdi.getNumberForRawBits(rawbits));
          }
        }
      }
      if (!stats.isEmpty()) {
        MapBasedStatisticsNotification notification = new MapBasedStatisticsNotification(
            millisTimeStamp, StatisticsNotification.Type.VALUE_CHANGED, stats);
        notifyListeners(notification);
      }
    }
  }

  @Override
  protected StringBuilder appendToString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("statistics=").append(this.statistics);
    return sb;
  }
}
