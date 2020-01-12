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
package org.apache.geode.test.assertj.internal;

import static org.apache.geode.test.assertj.internal.ShouldBeLessSpecificThan.shouldBeLessSpecificThan;

import org.apache.logging.log4j.Level;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.AssertionInfo;
import org.assertj.core.internal.Failures;

public abstract class AbstractLogLevelAssert<SELF extends AbstractLogLevelAssert<SELF>>
    extends AbstractAssert<SELF, Level> {

  private final Failures failures = Failures.instance();

  public AbstractLogLevelAssert(Level actual, Class<?> selfType) {
    super(actual, selfType);
  }

  public SELF isLessSpecificThan(final Level level) {
    assertLessSpecificThan(info, actual, level);
    return myself;
  }

  private void assertLessSpecificThan(final AssertionInfo info, final Level actual,
      final Level level) {
    if (actual.isLessSpecificThan(level)) {
      return;
    }
    throw failures.failure(info, shouldBeLessSpecificThan(actual, level));
  }

}
