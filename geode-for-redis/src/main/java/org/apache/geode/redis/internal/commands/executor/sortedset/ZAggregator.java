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

package org.apache.geode.redis.internal.commands.executor.sortedset;

import java.util.function.BiFunction;

/**
 * Enums representing aggregation functions used in {@link ZUnionStoreExecutor} and
 * {@link ZInterStoreExecutor}.
 */
public enum ZAggregator {

  SUM((score1, score2) -> {
    // in native redis, adding -inf and +inf results in 0. java math returns NaN
    if (score1.isInfinite() && score2.isInfinite()) {
      if (score1 == -score2) {
        return 0D;
      }
    }
    return Double.sum(score1, score2);
  }),
  MIN(Math::min),
  MAX(Math::max);

  private final BiFunction<Double, Double, Double> function;

  ZAggregator(BiFunction<Double, Double, Double> function) {
    this.function = function;
  }

  public BiFunction<Double, Double, Double> getFunction() {
    return function;
  }
}
