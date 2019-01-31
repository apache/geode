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

/**
 * Statistics related to the statistic sampler.
 */
public class StatSamplerStats {
  static final String SAMPLE_COUNT = "sampleCount";
  private static final String SAMPLE_TIME = "sampleTime";
  private static final String DELAY_DURATION = "delayDuration";
  private static final String STAT_RESOURCES = "statResources";
  private static final String JVM_PAUSES = "jvmPauses";
  private static final String SAMPLE_CALLBACKS = "sampleCallbacks";
  private static final String SAMPLE_CALLBACK_ERRORS = "sampleCallbackErrors";
  private static final String SAMPLE_CALLBACK_DURATION = "sampleCallbackDuration";

  private static final StatisticsType samplerType;
  private static final int sampleCountId;
  private static final int sampleTimeId;
  private static final int delayDurationId;
  private static final int statResourcesId;
  static final int jvmPausesId;
  private static final int sampleCallbacksId;
  private static final int sampleCallbackErrorsId;
  private static final int sampleCallbackDurationId;

  static {
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
    samplerType = f.createType("StatSampler", "Stats on the statistic sampler.",
        new StatisticDescriptor[] {
            f.createIntCounter(SAMPLE_COUNT, "Total number of samples taken by this sampler.",
                "samples", false),
            f.createLongCounter(SAMPLE_TIME, "Total amount of time spent taking samples.",
                "milliseconds", false),
            f.createIntGauge(DELAY_DURATION,
                "Actual duration of sampling delay taken before taking this sample.",
                "milliseconds", false),
            f.createIntGauge(STAT_RESOURCES,
                "Current number of statistic resources being sampled by this sampler.", "resources",
                false),
            f.createLongCounter(JVM_PAUSES,
                "Total number of JVM pauses (which may or may not be full GC pauses) detected by this sampler. A JVM pause is defined as a system event which kept the statistics sampler thread from sampling for 3000 or more milliseconds. This threshold can be customized by setting the system property gemfire.statSamplerDelayThreshold (units are milliseconds).",
                "jvmPauses", false),
            f.createIntGauge(SAMPLE_CALLBACKS,
                "Current number of statistics that are sampled using callbacks.", "resources",
                false),
            f.createIntCounter(SAMPLE_CALLBACK_ERRORS,
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

  StatSamplerStats(StatisticsFactory f, long id) {
    this.samplerStats = f.createStatistics(samplerType, "statSampler", id);
  }

  static StatisticsType getStatisticsType() {
    return samplerType;
  }

  void tookSample(long nanosSpentWorking, int statResources, long nanosSpentSleeping) {
    this.samplerStats.incInt(sampleCountId, 1);
    this.samplerStats.incLong(sampleTimeId, nanosSpentWorking / 1000000);
    this.samplerStats.setInt(delayDurationId, (int) (nanosSpentSleeping / 1000000));
    this.samplerStats.setInt(statResourcesId, statResources);
  }

  void incJvmPauses() {
    this.samplerStats.incLong(jvmPausesId, 1);
  }

  void incSampleCallbackErrors(int delta) {
    this.samplerStats.incInt(sampleCallbackErrorsId, delta);
  }

  void setSampleCallbacks(int count) {
    this.samplerStats.setInt(sampleCallbacksId, count);
  }

  void incSampleCallbackDuration(long delta) {
    this.samplerStats.incLong(sampleCallbackDurationId, delta);
  }

  int getSampleCount() {
    return this.samplerStats.getInt(SAMPLE_COUNT);
  }

  long getJvmPauses() {
    return this.samplerStats.getLong(JVM_PAUSES);
  }

  void close() {
    this.samplerStats.close();
  }

  public Statistics getStats() {
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
