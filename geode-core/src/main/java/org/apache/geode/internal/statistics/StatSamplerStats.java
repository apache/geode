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

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.annotations.Immutable;

/**
 * Statistics related to the statistic sampler.
 */
public class StatSamplerStats {
  public static final String SAMPLE_COUNT = "sampleCount"; // int
  private static final String SAMPLE_TIME = "sampleTime"; // long
  private static final String DELAY_DURATION = "delayDuration"; // int
  private static final String STAT_RESOURCES = "statResources"; // int
  private static final String JVM_PAUSES = "jvmPauses"; // int
  private static final String SAMPLE_CALLBACKS = "sampleCallbacks"; // int
  private static final String SAMPLE_CALLBACK_ERRORS = "sampleCallbackErrors"; // int
  private static final String SAMPLE_CALLBACK_DURATION = "sampleCallbackDuration"; // long

  @Immutable
  private static final StatisticsType samplerType;
  private static final int sampleCountId;
  private static final int sampleTimeId;
  private static final int delayDurationId;
  private static final int statResourcesId;
  private static final int jvmPausesId;
  private static final int sampleCallbacksId;
  private static final int sampleCallbackErrorsId;
  private static final int sampleCallbackDurationId;
  static {
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
    samplerType = f.createType("StatSampler", "Stats on the statistic sampler.",
        new StatisticDescriptor[] {
            f.createLongCounter(SAMPLE_COUNT, "Total number of samples taken by this sampler.",
                "samples", false),
            f.createLongCounter(SAMPLE_TIME, "Total amount of time spent taking samples.",
                "milliseconds", false),
            f.createLongGauge(DELAY_DURATION,
                "Actual duration of sampling delay taken before taking this sample.",
                "milliseconds", false),
            f.createLongGauge(STAT_RESOURCES,
                "Current number of statistic resources being sampled by this sampler.", "resources",
                false),
            f.createLongCounter(JVM_PAUSES,
                "Total number of JVM pauses (which may or may not be full GC pauses) detected by this sampler. A JVM pause is defined as a system event which kept the statistics sampler thread from sampling for 3000 or more milliseconds. This threshold can be customized by setting the system property gemfire.statSamplerDelayThreshold (units are milliseconds).",
                "jvmPauses", false),
            f.createLongGauge(SAMPLE_CALLBACKS,
                "Current number of statistics that are sampled using callbacks.", "resources",
                false),
            f.createLongCounter(SAMPLE_CALLBACK_ERRORS,
                "Total number of exceptions thrown by callbacks when performing sampling", "errors",
                false),
            f.createLongCounter(SAMPLE_CALLBACK_DURATION,
                "Total amount of time invoking sampling callbacks", "milliseconds", false),});
    sampleCountId = samplerType.nameToId(SAMPLE_COUNT);
    sampleTimeId = samplerType.nameToId(SAMPLE_TIME);
    delayDurationId = samplerType.nameToId(DELAY_DURATION);
    statResourcesId = samplerType.nameToId(STAT_RESOURCES);
    jvmPausesId = samplerType.nameToId(JVM_PAUSES);
    sampleCallbacksId = samplerType.nameToId(SAMPLE_CALLBACKS);
    sampleCallbackErrorsId = samplerType.nameToId(SAMPLE_CALLBACK_ERRORS);
    sampleCallbackDurationId = samplerType.nameToId(SAMPLE_CALLBACK_DURATION);
  }

  private final Statistics samplerStats;

  public StatSamplerStats(StatisticsFactory f, long id) {
    this.samplerStats = f.createStatistics(samplerType, "statSampler", id);
  }

  void tookSample(long nanosSpentWorking, int statResources, long nanosSpentSleeping) {
    this.samplerStats.incLong(sampleCountId, (long) 1);
    this.samplerStats.incLong(sampleTimeId, nanosSpentWorking / 1000000);
    this.samplerStats.setLong(delayDurationId, (nanosSpentSleeping / 1000000));
    this.samplerStats.setLong(statResourcesId, (long) statResources);
  }

  void incJvmPauses() {
    this.samplerStats.incLong(jvmPausesId, (long) 1);
  }

  public void incSampleCallbackErrors(int delta) {
    this.samplerStats.incLong(sampleCallbackErrorsId, (long) delta);
  }

  public void setSampleCallbacks(int count) {
    this.samplerStats.setLong(sampleCallbacksId, (long) count);
  }

  public void incSampleCallbackDuration(long delta) {
    this.samplerStats.incLong(sampleCallbackDurationId, delta);
  }

  public int getSampleCount() {
    return (int) this.samplerStats.getLong(SAMPLE_COUNT);
  }

  public long getSampleTime() {
    return this.samplerStats.getLong(SAMPLE_TIME);
  }

  public int getDelayDuration() {
    return (int) this.samplerStats.getLong(DELAY_DURATION);
  }

  public int getStatResources() {
    return (int) this.samplerStats.getLong(STAT_RESOURCES);
  }

  public int getJvmPauses() {
    return (int) this.samplerStats.getLong(JVM_PAUSES);
  }

  public void close() {
    this.samplerStats.close();
  }

  public Statistics getStats() {
    return this.samplerStats;
  }

  @Override
  public String toString() {
    return getClass().getName() + "@" + System.identityHashCode(this) + "{"
        + "isClosed=" + this.samplerStats.isClosed()
        + ", " + SAMPLE_COUNT + "=" + (int) this.samplerStats.getLong(SAMPLE_COUNT)
        + ", " + SAMPLE_TIME + "=" + this.samplerStats.getLong(SAMPLE_TIME)
        + ", " + DELAY_DURATION + "=" + (int) this.samplerStats.getLong(DELAY_DURATION)
        + ", " + STAT_RESOURCES + "=" + (int) this.samplerStats.getLong(STAT_RESOURCES)
        + ", " + JVM_PAUSES + "=" + (int) this.samplerStats.getLong(JVM_PAUSES)
        + "}";
  }
}
