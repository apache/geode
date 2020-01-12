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

import org.apache.geode.cache.query.QueryService;

public class AvgBucketNodeTest {
  private AvgBucketNode avgBucketNode;

  @Before
  public void setUp() {
    avgBucketNode = new AvgBucketNode();
  }

  @Test
  public void accumulateShouldIgnoreNull() {
    avgBucketNode.accumulate(null);

    assertThat(avgBucketNode.getCount()).isEqualTo(0);
    assertThat(avgBucketNode.getResult()).isEqualTo(0);
  }

  @Test
  public void accumulateShouldIgnoreUndefined() {
    avgBucketNode.accumulate(QueryService.UNDEFINED);

    assertThat(avgBucketNode.getCount()).isEqualTo(0);
    assertThat(avgBucketNode.getResult()).isEqualTo(0);
  }

  @Test
  public void accumulateShouldIncreaseAccumulatedCount() {
    avgBucketNode.accumulate(1);
    avgBucketNode.accumulate(25.78f);

    assertThat(avgBucketNode.getCount()).isEqualTo(2);
    assertThat(avgBucketNode.getResult()).isEqualTo(26.78f);
  }

  @Test
  public void terminateShouldCorrectlyComputeAverageUponAccumulatedValues() {
    avgBucketNode.accumulate(1);
    avgBucketNode.accumulate(2);
    avgBucketNode.accumulate(3);
    avgBucketNode.accumulate(4);
    avgBucketNode.accumulate(5);
    avgBucketNode.accumulate(6);
    avgBucketNode.accumulate(7);
    avgBucketNode.accumulate(null);
    avgBucketNode.accumulate(QueryService.UNDEFINED);

    Object result = avgBucketNode.terminate();
    assertThat(result).isInstanceOf(Object[].class);
    assertThat(((Long) ((Object[]) result)[0]).intValue()).isEqualTo(7);
    assertThat(((Number) ((Object[]) result)[1]).intValue()).isEqualTo(28);
  }
}
