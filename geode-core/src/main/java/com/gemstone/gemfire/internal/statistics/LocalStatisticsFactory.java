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
package com.gemstone.gemfire.internal.statistics;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;
import com.gemstone.gemfire.internal.statistics.AbstractStatisticsFactory;
import com.gemstone.gemfire.internal.statistics.DummyStatisticsImpl;
import com.gemstone.gemfire.internal.statistics.SimpleStatSampler;
import com.gemstone.gemfire.internal.statistics.StatisticsManager;

/**
 * A standalone implementation of {@link StatisticsFactory}.
 * It can be used in contexts that do not have the GemFire product
 * or in vm's that do not have a distributed system nor a gemfire connection.
 *
 */
public class LocalStatisticsFactory extends AbstractStatisticsFactory
    implements StatisticsFactory, StatisticsManager {

  private static final Logger logger = LogService.getLogger();
  
  public static final String STATS_DISABLE_NAME_PROPERTY = "stats.disable";

  private final SimpleStatSampler sampler;
  private final boolean statsDisabled;

  public LocalStatisticsFactory(CancelCriterion stopper) {
    super(initId(), initName(), initStartTime());
    
    this.statsDisabled = Boolean.getBoolean(STATS_DISABLE_NAME_PROPERTY);
    if (statsDisabled) {
      this.sampler = null;
      logger.info(LogMarker.STATISTICS, LocalizedMessage.create(LocalizedStrings.LocalStatisticsFactory_STATISTIC_COLLECTION_IS_DISABLED_USE_DSTATSDISABLEFALSE_TO_TURN_ON_STATISTICS));
    } else if (stopper != null) {
      this.sampler = new SimpleStatSampler(stopper, this);
      this.sampler.start();
    } else {
      this.sampler = null;
    }
  }
  
  protected static long initId() {
    return Thread.currentThread().hashCode();
  }
  
  protected static String initName() {
    return System.getProperty("stats.name", Thread.currentThread().getName());
  }
  
  protected static long initStartTime() {
    return System.currentTimeMillis();
  }
  
 protected SimpleStatSampler getStatSampler() {
    return this.sampler;
  }

  @Override
  public void close() {
    if (this.sampler != null) {
      this.sampler.stop();
    }
  }
  
  @Override
  protected Statistics createOsStatistics(StatisticsType type, String textId, long numericId, int osStatFlags) {
    if (this.statsDisabled) {
      return new DummyStatisticsImpl(type, textId, numericId);
    }
    return super.createOsStatistics(type, textId, numericId, osStatFlags);
  }

  @Override
  public Statistics createAtomicStatistics(StatisticsType type, String textId, long numericId) {
    if (this.statsDisabled) {
      return new DummyStatisticsImpl(type, textId, numericId);
    }
    return super.createAtomicStatistics(type, textId, numericId);
  }
}
