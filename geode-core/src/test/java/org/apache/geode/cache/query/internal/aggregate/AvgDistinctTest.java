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
package org.apache.geode.cache.query.internal.aggregate;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

public class AvgDistinctTest extends DistinctAggregatorTest {

  @Before
  public void setUp() {
    distinctAggregator = new AvgDistinct();
  }

  @Test
  public void terminateShouldCorrectlyComputeAverageUponDistinctAccumulatedValues() {
    distinctAggregator.accumulate(5);
    distinctAggregator.accumulate(5);
    distinctAggregator.accumulate(10);
    distinctAggregator.accumulate(10);
    distinctAggregator.accumulate(15);
    distinctAggregator.accumulate(15);

    Object result = distinctAggregator.terminate();
    assertThat(result).isInstanceOf(Number.class);
    assertThat(((Number) result).floatValue()).isEqualTo(10);
  }
}
