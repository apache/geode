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
package org.apache.geode.test.dunit;

import static org.junit.Assert.fail;

import org.awaitility.core.ThrowingRunnable;

import org.apache.geode.test.awaitility.GeodeAwaitility;

/**
 * Defines an asynchronous criterion to wait for by invoking a method in {@link Wait}.
 *
 * <p>
 * Extracted from DistributedTestCase.
 *
 * <p>
 * See javadocs on {@link Wait} for examples and guidelines for converting to Awaitility.
 *
 * @deprecated Use {@link GeodeAwaitility} instead.
 *
 * @see Wait
 * @see GeodeAwaitility
 * @see org.awaitility.Duration
 * @see org.awaitility.core.ConditionFactory
 */
public interface WaitCriterion extends ThrowingRunnable {

  public boolean done();

  public String description();

  default void run() {
    if (!done()) {
      fail(description());
    }

  }

}
