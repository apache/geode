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

import java.util.Collections;
import java.util.List;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.annotations.Immutable;

/**
 * A statistics registry that creates dummy statistics instances, and does not keep a list of the
 * instances it has created.
 */
public class DummyStatisticsRegistry extends StatisticsRegistry {
  @Immutable
  private static final List<Statistics> emptyInstances = Collections.emptyList();

  public DummyStatisticsRegistry(String systemName, long startTime) {
    super(systemName, startTime);
  }

  @Override
  public List<Statistics> getStatsList() {
    return emptyInstances;
  }

  @Override
  public int getStatListModCount() {
    return 0;
  }

  @Override
  public void destroyStatistics(Statistics instance) {
    // noop
  }

  @Override
  protected Statistics newAtomicStatistics(StatisticsType type, long uniqueId, long numericId,
      String textId) {
    return new DummyStatisticsImpl(type, textId, numericId);
  }

  @Override
  protected Statistics newOsStatistics(StatisticsType type, long uniqueId, long numericId,
      String textId, int osStatFlags) {
    return new DummyStatisticsImpl(type, textId, numericId);
  }
}
