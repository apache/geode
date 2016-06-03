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
  public final static String SAMPLE_CALLBACKS = "sampleCallbacks"; // int
  public final static String SAMPLE_CALLBACK_ERRORS = "sampleCallbackErrors"; // int
  public final static String SAMPLE_CALLBACK_DURATION = "sampleCallbackDuration"; // long

  private final static StatisticsType samplerType;
  private final static int sampleCountId;
  private final static int sampleTimeId;
  private final static int delayDurationId;
  private final static int statResourcesId;
  private final static int jvmPausesId;
  private final static int sampleCallbacksId;
  private final static int sampleCallbackErrorsId;
  private final static int sampleCallbackDurationId;
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
                                 f.createIntGauge(SAMPLE_CALLBACKS,
                                   "Total number of statistics that are sampled using callbacks.",
                                   "resources", false),
                                 f.createIntCounter(SAMPLE_CALLBACK_ERRORS,
                                   "Total number of exceptions thrown by callbacks when performing sampling",
                                   "errors", false),
                                 f.createLongCounter(SAMPLE_CALLBACK_DURATION,
                                   "Total amount of time invoking sampling callbacks",
                                   "milliseconds", false),
                               });
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

  public void tookSample(long nanosSpentWorking, int statResources, long nanosSpentSleeping) {
    this.samplerStats.incInt(sampleCountId, 1);
    this.samplerStats.incLong(sampleTimeId, nanosSpentWorking / 1000000);
    this.samplerStats.setInt(delayDurationId, (int)(nanosSpentSleeping / 1000000));
    this.samplerStats.setInt(statResourcesId, statResources);
  }
  
  public void incJvmPauses() {
    this.samplerStats.incInt(jvmPausesId, 1);
  }

  public void incSampleCallbackErrors(int delta) {
    this.samplerStats.incInt(sampleCallbackErrorsId, delta);
  }

  public void setSampleCallbacks(int count) {
    this.samplerStats.setInt(sampleCallbacksId, count);
  }

  public void incSampleCallbackDuration(long delta) {
    this.samplerStats.incLong(sampleCallbackDurationId, delta);
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
