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

package org.apache.geode.test.junit.assertions;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

public class TabularResultModelColumnAssert<T> extends TabularResultModelSliceAssert<T> {
  TabularResultModelColumnAssert(TabularResultModelAssert tabularResultModelAssert,
      List<T> valuesInColumn) {
    super(tabularResultModelAssert, valuesInColumn);
  }

  /**
   * Verifies that the selected column contains the given values only once.
   */
  @SafeVarargs
  public final TabularResultModelAssert containsOnlyOnce(T... values) {
    assertThat(this.values).containsOnlyOnce(values);
    return parent;
  }

  /**
   * Verifies that the selected column contains exactly the given values and nothing else, <b>in any
   * order</b>.
   */
  @SafeVarargs
  public final TabularResultModelAssert containsExactlyInAnyOrder(T... values) {
    assertThat(this.values).containsExactlyInAnyOrder(values);
    return parent;
  }

  /**
   * verifies the expected number of values in the column
   */
  public final TabularResultModelColumnAssert<T> hasSize(int expected) {
    assertThat(values).hasSize(expected);
    return this;
  }
}
