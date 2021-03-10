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
package org.apache.geode.cache.query.internal.types;

import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Comparator;

import org.junit.Test;

import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.internal.index.IndexManager;

public class ExtendedNumericComparatorTest {
  Comparator<Object> comparator = uncheckedCast(TypeUtils.getExtendedNumericComparator());

  @Test
  public void nullIsGreaterThanUndefined() {
    assertThat(comparator.compare(IndexManager.NULL, QueryService.UNDEFINED)).isEqualTo(1);
  }

  @Test
  public void undefinedIsSmallerThanNull() {
    assertThat(comparator.compare(QueryService.UNDEFINED, IndexManager.NULL)).isEqualTo(-1);
  }

  @Test
  public void undefinedIsEqualToUndefined() {
    assertThat(comparator.compare(QueryService.UNDEFINED, QueryService.UNDEFINED)).isEqualTo(0);
  }

  @Test
  public void nullIsEqualToNull() {
    assertThat(comparator.compare(IndexManager.NULL, IndexManager.NULL)).isEqualTo(0);
  }

  @Test
  public void undefinedIsSmallerThanNumber() {
    assertThat(comparator.compare(QueryService.UNDEFINED, 3)).isEqualTo(-1);
  }

  @Test
  public void numberIsGreaterThanUndefined() {
    assertThat(comparator.compare(3, QueryService.UNDEFINED)).isEqualTo(1);
  }

  @Test
  public void nullIsSmallerThanNumber() {
    assertThat(comparator.compare(IndexManager.NULL, 3)).isEqualTo(-1);
  }

  @Test
  public void numberIsGreaterThanNull() {
    assertThat(comparator.compare(3, IndexManager.NULL)).isEqualTo(1);
  }

  @Test
  public void numberXisEqualToNumberX() {
    assertThat(comparator.compare(6, 6)).isEqualTo(0);
  }

  @Test
  public void numberXPlusOneIsGreaterThanNumberX() {
    assertThat(comparator.compare(4, 3)).isEqualTo(1);
  }

  @Test
  public void numberXIsSmallerThanNumberXPlusOne() {
    assertThat(comparator.compare(2, 3)).isEqualTo(-1);
  }
}
