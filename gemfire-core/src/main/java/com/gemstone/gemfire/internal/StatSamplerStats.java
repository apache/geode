/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.internal;

import com.gemstone.gemfire.*;

/**
 * Statistics related to the statistic sampler.
 */
public class StatSamplerStats {
  public final static String SAMPLE_COUNT = "sampleCount"; // int
  public final static String SAMPLE_TIME = "sampleTime"; // long
  public final static String DELAY_DURATION = "delayDuration"; // int
  public final static String STAT_RESOURCES = "statResources"; // int
  public final static String JVM_PAUSES = "jvmPauses"; // int
  
  private final static StatisticsType samplerType;
  private final static int sampleCountId;
  private final static int sampleTimeId;
  private final static int delayDurationId;
  private final static int statResourcesId;
  private final static int jvmPausesId;
  static {
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
    samplerType = f.createType("StatSampler",
                               "Stats on the statistic sampler.",
                               new StatisticDescriptor[] {
                                 f.createIntCounter(SAMPLE_COUNT,
                                                    "Total number of samples taken by this sampler.",
                                                    "samples", false),
                                 f.createLongCounter(SAMPLE_TIME,
                                                     "Total amount of time spent taking samples.",
                                                     "milliseconds", false),
                                 f.createIntGauge(DELAY_DURATION,
                                                  "Actual duration of sampling delay taken before taking this sample.",
                                                  "milliseconds", false),
                                 f.createIntGauge(STAT_RESOURCES,
                                                  "Current number of statistic resources being sampled by this sampler.",
                                                  "resources", false),
                                 f.createIntCounter(JVM_PAUSES,
                                                    "Total number of JVM pauses (which may or may not be full GC pauses) detected by this sampler. A JVM pause is defined as a system event which kept the statistics sampler thread from sampling for 3000 or more milliseconds. This threshold can be customized by setting the system property gemfire.statSamplerDelayThreshold (units are milliseconds).",
                                                    "jvmPauses", false),
                               });
    sampleCountId = samplerType.nameToId(SAMPLE_COUNT);
    sampleTimeId = samplerType.nameToId(SAMPLE_TIME);
    delayDurationId = samplerType.nameToId(DELAY_DURATION);
    statResourcesId = samplerType.nameToId(STAT_RESOURCES);
    jvmPausesId = samplerType.nameToId(JVM_PAUSES);
  }

  private final Statistics samplerStats;

  public StatSamplerStats(StatisticsFactory f, long id) {
    this.samplerStats = f.createStatistics(samplerType, "statSampler", id);
  }

  public void tookSample(long nanosSpentWorking, int statResources, long nanosSpentSleeping) {
    this.samplerStats.incInt(sampleCountId, 1);
    this.samplerStats.incLong(sampleTimeId, nanosSpentWorking / 1000000);
    this.samplerStats.setInt(delayDurationId, (int)(nanosSpentSleeping / 1000000));
    this.samplerStats.setInt(statResourcesId, statResources);
  }
  
  public void incJvmPauses() {
    this.samplerStats.incInt(jvmPausesId, 1);
  }
  
  public int getSampleCount() {
    return this.samplerStats.getInt(SAMPLE_COUNT);
  }
  
  public long getSampleTime() {
    return this.samplerStats.getLong(SAMPLE_TIME);
  }
  
  public int getDelayDuration() {
    return this.samplerStats.getInt(DELAY_DURATION);
  }
  
  public int getStatResources() {
    return this.samplerStats.getInt(STAT_RESOURCES);
  }
  
  public int getJvmPauses() {
    return this.samplerStats.getInt(JVM_PAUSES);
  }
  
  public void close() {
    this.samplerStats.close();
  }
  
  public Statistics getStats(){
    return this.samplerStats;
  }
  
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getName());
    sb.append("@").append(System.identityHashCode(this)).append("{");
    sb.append("isClosed=").append(this.samplerStats.isClosed());
    sb.append(", ").append(SAMPLE_COUNT + "=").append(this.samplerStats.getInt(SAMPLE_COUNT));
    sb.append(", ").append(SAMPLE_TIME + "=").append(this.samplerStats.getLong(SAMPLE_TIME));
    sb.append(", ").append(DELAY_DURATION + "=").append(this.samplerStats.getInt(DELAY_DURATION));
    sb.append(", ").append(STAT_RESOURCES + "=").append(this.samplerStats.getInt(STAT_RESOURCES));
    sb.append(", ").append(JVM_PAUSES + "=").append(this.samplerStats.getInt(JVM_PAUSES));
    sb.append("}");
    return sb.toString();
  }
}
