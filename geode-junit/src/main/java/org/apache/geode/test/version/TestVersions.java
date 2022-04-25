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
 *
 */

package org.apache.geode.test.version;

import java.util.function.Predicate;

/**
 * Factory methods for predicates to evaluate {@link TestVersion}s.
 */
public class TestVersions {
  /**
   * Returns a predicate that tests if its argument is less than {@code bound}.
   *
   * @param bound the upper bound
   * @return a predicate that tests if its argument is less than @{code bound}
   */
  public static Predicate<TestVersion> lessThan(TestVersion bound) {
    return v -> v.lessThan(bound);
  }

  /**
   * Returns a predicate that tests if its argument is at most {@code bound}.
   *
   * @param bound the upper bound
   * @return a predicate that tests if its argument is at most @{code bound}
   */
  public static Predicate<TestVersion> atMost(TestVersion bound) {
    return v -> v.lessThanOrEqualTo(bound);
  }

  /**
   * Returns a predicate that tests if its argument at least {@code bound}.
   *
   * @param bound the lower bound
   * @return a predicate that tests if its argument is at least @{code bound}
   */
  public static Predicate<TestVersion> atLeast(TestVersion bound) {
    return v -> v.greaterThanOrEqualTo(bound);
  }

  /**
   * Returns a predicate that tests if its argument is greater than {@code bound}.
   *
   * @param bound the lower bound
   * @return a predicate that tests if its argument is greater than @{code bound}
   */
  public static Predicate<TestVersion> greaterThan(TestVersion bound) {
    return v -> v.greaterThanOrEqualTo(bound);
  }
}
