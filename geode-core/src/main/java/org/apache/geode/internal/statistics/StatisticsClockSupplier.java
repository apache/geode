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

import org.apache.geode.distributed.internal.DistributionConfig;

@FunctionalInterface
public interface StatisticsClockSupplier {

  /**
   * Please pass the {@code StatisticsClock} through constructors where possible instead of
   * accessing it from this supplier.
   *
   * <p>
   * Various factories may use this supplier to acquire the {@code StatisticsClock} which is
   * created by the Cache as configured by {@link DistributionConfig#getEnableTimeStatistics()}.
   */
  StatisticsClock getStatisticsClock();
}
