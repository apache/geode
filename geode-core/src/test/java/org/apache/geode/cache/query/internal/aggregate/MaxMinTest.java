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

import java.util.Arrays;
import java.util.List;

import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.query.QueryService;

public class MaxMinTest {
  private MaxMin min;
  private MaxMin max;

  @Before
  public void setUp() {
    max = new MaxMin(true);
    min = new MaxMin(false);
  }

  @Test
  public void accumulateShouldIgnoreNull() {
    max.accumulate(null);
    min.accumulate(null);

    AssertionsForClassTypes.assertThat(max.getCurrentOptima()).isNull();
    AssertionsForClassTypes.assertThat(min.getCurrentOptima()).isNull();
  }

  @Test
  public void accumulateShouldIgnoreUndefined() {
    max.accumulate(QueryService.UNDEFINED);
    min.accumulate(QueryService.UNDEFINED);

    AssertionsForClassTypes.assertThat(max.getCurrentOptima()).isNull();
    AssertionsForClassTypes.assertThat(min.getCurrentOptima()).isNull();
  }

  @Test
  public void accumulateShouldUpdateOptimaOnIntermediateAdditions() {
    AssertionsForClassTypes.assertThat(max.getCurrentOptima()).isNull();
    max.accumulate(10);
    AssertionsForClassTypes.assertThat(max.getCurrentOptima()).isEqualTo(10);
    max.accumulate(5);
    AssertionsForClassTypes.assertThat(max.getCurrentOptima()).isEqualTo(10);
    max.accumulate(15);
    AssertionsForClassTypes.assertThat(max.getCurrentOptima()).isEqualTo(15);

    AssertionsForClassTypes.assertThat(min.getCurrentOptima()).isNull();
    min.accumulate(10);
    AssertionsForClassTypes.assertThat(min.getCurrentOptima()).isEqualTo(10);
    min.accumulate(5);
    AssertionsForClassTypes.assertThat(min.getCurrentOptima()).isEqualTo(5);
    min.accumulate(6);
    AssertionsForClassTypes.assertThat(min.getCurrentOptima()).isEqualTo(5);
  }

  @Test
  public void terminateShouldReturnOptimaFound() {
    List<String> comparableStrings = Arrays.asList("B", "D", "Z", "E", "A");
    comparableStrings.forEach(string -> {
      max.accumulate(string);
      min.accumulate(string);
    });

    Object maxResult = max.terminate();
    assertThat(maxResult).isInstanceOf(String.class);
    assertThat(maxResult).isEqualTo("Z");

    Object minResult = min.terminate();
    assertThat(minResult).isInstanceOf(String.class);
    assertThat(minResult).isEqualTo("A");
  }
}
