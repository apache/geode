/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.  
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.StatisticsTypeFactory;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * An abstract standalone implementation of {@link StatisticsFactory}.
 * It can be used in contexts that do not have the GemFire product
 * or in vm's that do not have a distributed system nor a gemfire connection.
 *
 * @author Darrel Schneider
 * @author Kirk Lund
 * @since 7.0
 */
public abstract class AbstractStatisticsFactory 
    implements StatisticsFactory, StatisticsManager {

  private final long id;
  private final String name;
  private final List<Statistics> statsList;
  private int statsListModCount = 0;
  private long statsListUniqueId = 1;
  private final Object statsListUniqueIdLock;
  private final StatisticsTypeFactory tf;
  private final long startTime;

  public AbstractStatisticsFactory(long id, String name, long startTime) {
    this.id = id;
    this.name = name;
    this.startTime = startTime;
    
    this.statsList = new ArrayList<Statistics>();
    this.statsListUniqueIdLock = new Object();
    this.tf = StatisticsTypeFactoryImpl.singleton();
  }

  public void close() {
  }
  
  @Override
  public final String getName() {
    return this.name;
  }
  
  @Override
  public final long getId() {
    return this.id;
  }
  
  @Override
  public final long getStartTime() {
    return this.startTime;
  }
  
  @Override
  public final int getStatListModCount() {
    return this.statsListModCount;
  }
  
  @Override
  public final List<Statistics> getStatsList() {
    return this.statsList;
  }

  @Override
  public final int getStatisticsCount() {
    int result = 0;
    List<Statistics> statsList = this.statsList;
    if (statsList != null) {
      result = statsList.size();
    }
    return result;
  }
  
  @Override
  public final Statistics findStatistics(long id) {
    List<Statistics> statsList = this.statsList;
    synchronized (statsList) {
      for (Statistics s : statsList) {
        if (s.getUniqueId() == id) {
          return s;
        }
      }
    }
    throw new RuntimeException(LocalizedStrings.PureStatSampler_COULD_NOT_FIND_STATISTICS_INSTANCE.toLocalizedString());
  }
  
  @Override
  public final boolean statisticsExists(long id) {
    List<Statistics> statsList = this.statsList;
    synchronized (statsList) {
      for (Statistics s : statsList) {
        if (s.getUniqueId() == id) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public final Statistics[] getStatistics() {
    List<Statistics> statsList = this.statsList;
    synchronized (statsList) {
      return (Statistics[])statsList.toArray(new Statistics[statsList.size()]);
    }
  }
  
  // StatisticsFactory methods
  
  @Override
  public final Statistics createStatistics(StatisticsType type) {
    return createOsStatistics(type, null, 0, 0);
  }
  
  @Override
  public final Statistics createStatistics(StatisticsType type, String textId) {
    return createOsStatistics(type, textId, 0, 0);
  }
  
  @Override
  public final Statistics createStatistics(StatisticsType type, String textId, long numericId) {
    return createOsStatistics(type, textId, 0, 0);
  }
  
  protected Statistics createOsStatistics(StatisticsType type, String textId, long numericId, int osStatFlags) {
    long myUniqueId;
    synchronized (statsListUniqueIdLock) {
      myUniqueId = statsListUniqueId++; // fix for bug 30597
    }
    Statistics result = new LocalStatisticsImpl(type, textId, numericId, myUniqueId, false, osStatFlags, this);
    synchronized (statsList) {
      statsList.add(result);
      statsListModCount++;
    }
    return result;
  }

  @Override
  public final Statistics[] findStatisticsByType(StatisticsType type) {
    List<Statistics> hits = new ArrayList<Statistics>();
    synchronized (statsList) {
      Iterator<Statistics> it = statsList.iterator();
      while (it.hasNext()) {
        Statistics s = (Statistics)it.next();
        if (type == s.getType()) {
          hits.add(s);
        }
      }
    }
    Statistics[] result = new Statistics[hits.size()];
    return (Statistics[])hits.toArray(result);
  }
  
  @Override
  public final Statistics[] findStatisticsByTextId(String textId) {
    List<Statistics> hits = new ArrayList<Statistics>();
    synchronized (statsList) {
      Iterator<Statistics> it = statsList.iterator();
      while (it.hasNext()) {
        Statistics s = (Statistics)it.next();
        if (s.getTextId().equals(textId)) {
          hits.add(s);
        }
      }
    }
    Statistics[] result = new Statistics[hits.size()];
    return (Statistics[])hits.toArray(result);
  }
  
  @Override
  public final Statistics[] findStatisticsByNumericId(long numericId) {
    List<Statistics> hits = new ArrayList<Statistics>();
    synchronized (statsList) {
      Iterator<Statistics> it = statsList.iterator();
      while (it.hasNext()) {
        Statistics s = (Statistics)it.next();
        if (numericId == s.getNumericId()) {
          hits.add(s);
        }
      }
    }
    Statistics[] result = new Statistics[hits.size()];
    return (Statistics[])hits.toArray(result);
  }
  
  public final Statistics findStatisticsByUniqueId(long uniqueId) {
    synchronized (statsList) {
      Iterator<Statistics> it = statsList.iterator();
      while (it.hasNext()) {
        Statistics s = (Statistics)it.next();
        if (uniqueId == s.getUniqueId()) {
          return s;
        }
      }
    }
    return null;
  }

  /** for internal use only. Its called by {@link LocalStatisticsImpl#close}. */
  @Override
  public final void destroyStatistics(Statistics stats) {
    synchronized (statsList) {
      if (statsList.remove(stats)) {
        statsListModCount++;
      }
    }
  }

  @Override
  public final Statistics createAtomicStatistics(StatisticsType type) {
    return createAtomicStatistics(type, null, 0);
  }
  
  @Override
  public final Statistics createAtomicStatistics(StatisticsType type, String textId) {
    return createAtomicStatistics(type, textId, 0);
  }
  
  @Override
  public Statistics createAtomicStatistics(StatisticsType type, String textId, long numericId) {
    long myUniqueId;
    synchronized (statsListUniqueIdLock) {
      myUniqueId = statsListUniqueId++; // fix for bug 30597
    }
    Statistics result = StatisticsImpl.createAtomicNoOS(type, textId, numericId, myUniqueId, this);
    synchronized (statsList) {
      statsList.add(result);
      statsListModCount++;
    }
    return result;
  }

  // StatisticsTypeFactory methods
  
  /**
   * Creates or finds a StatisticType for the given shared class.
   */
  @Override
  public final StatisticsType createType(String name, String description, StatisticDescriptor[] stats) {
    return tf.createType(name, description, stats);
  }
  
  @Override
  public final StatisticsType findType(String name) {
    return tf.findType(name);
  }
  
  @Override
  public final StatisticsType[] createTypesFromXml(Reader reader)
    throws IOException {
    return tf.createTypesFromXml(reader);
  }

  @Override
  public final StatisticDescriptor createIntCounter(String name, String description, String units) {
    return tf.createIntCounter(name, description, units);
  }
  
  @Override
  public final StatisticDescriptor createLongCounter(String name, String description, String units) {
    return tf.createLongCounter(name, description, units);
  }
  
  @Override
  public final StatisticDescriptor createDoubleCounter(String name, String description, String units) {
    return tf.createDoubleCounter(name, description, units);
  }
  
  @Override
  public final StatisticDescriptor createIntGauge(String name, String description, String units) {
    return tf.createIntGauge(name, description, units);
  }
  
  @Override
  public final StatisticDescriptor createLongGauge(String name, String description, String units) {
    return tf.createLongGauge(name, description, units);
  }
  
  @Override
  public final StatisticDescriptor createDoubleGauge(String name, String description, String units) {
    return tf.createDoubleGauge(name, description, units);
  }
  
  @Override
  public final StatisticDescriptor createIntCounter(String name, String description, String units, boolean largerBetter) {
    return tf.createIntCounter(name, description, units, largerBetter);
  }
  
  @Override
  public final StatisticDescriptor createLongCounter(String name, String description, String units, boolean largerBetter) {
    return tf.createLongCounter(name, description, units, largerBetter);
  }
  
  @Override
  public final StatisticDescriptor createDoubleCounter(String name, String description, String units, boolean largerBetter) {
    return tf.createDoubleCounter(name, description, units, largerBetter);
  }
  
  @Override
  public final StatisticDescriptor createIntGauge(String name, String description, String units, boolean largerBetter) {
    return tf.createIntGauge(name, description, units, largerBetter);
  }
  
  @Override
  public final StatisticDescriptor createLongGauge(String name, String description, String units, boolean largerBetter) {
    return tf.createLongGauge(name, description, units, largerBetter);
  }
  
  @Override
  public final StatisticDescriptor createDoubleGauge(String name, String description, String units, boolean largerBetter) {
    return tf.createDoubleGauge(name, description, units, largerBetter);
  }
}
