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

import static org.assertj.core.api.Assertions.fail;

import org.apache.commons.lang3.StringUtils;

public class TabularResultModelAnyRowAssert<T extends String> {
  protected TabularResultModelAssert parent;

  TabularResultModelAnyRowAssert(TabularResultModelAssert tabularResultModelAssert) {
    parent = tabularResultModelAssert;
  }

  /**
   * Verifies that any row contains the given values, in any order.
   */
  @SafeVarargs
  public final TabularResultModelAssert contains(T... rowValues) {
    for (int i = 0; i < parent.getActual().getRowSize(); i++) {
      try {
        parent.hasRow(i).contains(rowValues);
        return parent;
      } catch (AssertionError ignore) {
      }
    }
    fail("Did not find " + StringUtils.join(rowValues, " and ") + " in any rows");
    return parent;
  }

  /**
   * Verifies that any row contains only the given values and nothing else, in any order and
   * ignoring duplicates (i.e. once a value is found, its duplicates are also considered found).
   */
  @SafeVarargs
  public final TabularResultModelAssert containsOnly(T... rowValues) {
    for (int i = 0; i < parent.getActual().getRowSize(); i++) {
      try {
        parent.hasRow(i).containsOnly(rowValues);
        return parent;
      } catch (AssertionError ignore) {
      }
    }
    fail("Did not find only " + StringUtils.join(rowValues, " and ") + " in any rows");
    return parent;
  }

  /**
   * Verifies that any row contains exactly the given values and nothing else, <b>in order</b>.
   */
  @SafeVarargs
  public final TabularResultModelAssert containsExactly(T... rowValues) {
    for (int i = 0; i < parent.getActual().getRowSize(); i++) {
      try {
        parent.hasRow(i).containsExactly(rowValues);
        return parent;
      } catch (AssertionError ignore) {
      }
    }
    fail("Did not find exactly [" + StringUtils.join(rowValues, ", ") + "] in any rows");
    return parent;
  }

  /**
   * Verifies that any row contains at least one of the given values.
   */
  @SafeVarargs
  public final TabularResultModelAssert containsAnyOf(T... rowValues) {
    for (int i = 0; i < parent.getActual().getRowSize(); i++) {
      try {
        parent.hasRow(i).containsAnyOf(rowValues);
        return parent;
      } catch (AssertionError ignore) {
      }
    }
    fail("Did not find " + StringUtils.join(rowValues, " or ") + " in any rows");
    return parent;
  }
}
