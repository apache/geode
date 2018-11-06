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

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.internal.statistics.platform.OsStatisticsFactory;

/**
 * @since GemFire 7.0
 */
public class TestStatisticsManager extends AbstractStatisticsFactory
    implements StatisticsManager, OsStatisticsFactory {

  public TestStatisticsManager(final long id, final String name, final long startTime) {
    super(id, name, startTime);
  }

  @Override
  public Statistics createOsStatistics(final StatisticsType type, final String textId,
      final long numericId, final int osStatFlags) {
    return null;
  }
}
