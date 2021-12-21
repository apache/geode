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

import org.assertj.core.api.ListAssert;

class TabularResultModelSliceAssert<T> {
  protected TabularResultModelAssert parent;
  protected List<T> values;

  TabularResultModelSliceAssert(TabularResultModelAssert tabularResultModelAssert,
      List<T> valuesInSlice) {
    parent = tabularResultModelAssert;
    values = valuesInSlice;
  }

  /**
   * Verifies that the selected row or column contains the given values, in any order.
   */
  @SafeVarargs
  public final TabularResultModelAssert contains(T... values) {
    assertThat(this.values).contains(values);
    return parent;
  }

  /**
   * Verifies that the selected row or column contains only the given values and nothing else, in
   * any order and ignoring duplicates (i.e. once a value is found, its duplicates are also
   * considered found).
   */
  @SafeVarargs
  public final TabularResultModelAssert containsOnly(T... values) {
    assertThat(this.values).containsOnly(values);
    return parent;
  }

  /**
   * Verifies that the selected row or column contains exactly the given values and nothing else,
   * <b>in order</b>.
   */
  @SafeVarargs
  public final TabularResultModelAssert containsExactly(T... values) {
    assertThat(this.values).containsExactly(values);
    return parent;
  }

  /**
   * Verifies that the selected row or column contains at least one of the given values.
   */
  @SafeVarargs
  public final TabularResultModelAssert containsAnyOf(T... values) {
    assertThat(this.values).containsAnyOf(values);
    return parent;
  }

  /**
   * Verifies that the selected row or column does not contain the given values.
   */
  @SafeVarargs
  public final TabularResultModelAssert doesNotContain(T... values) {
    assertThat(this.values).doesNotContain(values);
    return parent;
  }

  /**
   * Verifies that all values in the selected row or column are present in the given values.
   */
  @SafeVarargs
  public final TabularResultModelAssert isSubsetOf(T... values) {
    assertThat(this.values).isSubsetOf(values);
    return parent;
  }

  /**
   * Provides the flexibility to verify the selected row or column using the full power of
   * ListAssert (with the tradeoff that you will not be able to chain verifications for other rows
   * or columns in the table after this).
   */
  public final ListAssert<T> asList() {
    return new ListAssert<>(values);
  }
}
