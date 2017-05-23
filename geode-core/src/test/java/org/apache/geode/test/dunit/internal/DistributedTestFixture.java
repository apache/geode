/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.geode.test.dunit.internal;

import java.io.Serializable;
import java.util.Properties;

/**
 * Defines the {@code DistributedTestCase} methods that can be overridden by its subclasses.
 */
public interface DistributedTestFixture extends Serializable {

  /**
   * {@code preSetUp()} is invoked before {@code DistributedTestCase#setUp()}.
   *
   * <p>
   * Override this as needed. Default implementation is empty.
   */
  void preSetUp() throws Exception;

  /**
   * {@code postSetUp()} is invoked after {@code DistributedTestCase#setUp()}.
   *
   * <p>
   * Override this as needed. Default implementation is empty.
   */
  void postSetUp() throws Exception;

  /**
   * {@code preTearDown()} is invoked before {@code DistributedTestCase#tearDown()}.
   *
   * <p>
   * Override this as needed. Default implementation is empty.
   */
  void preTearDown() throws Exception;

  /**
   * {@code postTearDown()} is invoked after {@code DistributedTestCase#tearDown()}.
   *
   * <p>
   * Override this as needed. Default implementation is empty.
   */
  void postTearDown() throws Exception;

  /**
   * {@code preTearDownAssertions()} is invoked before any tear down methods have been invoked. If
   * this method throws anything, tear down methods will still be invoked.
   *
   * <p>
   * Override this as needed. Default implementation is empty.
   */
  void preTearDownAssertions() throws Exception;

  /**
   * {@code postTearDownAssertions()} is invoked after all tear down methods have completed. This
   * method will not be invoked if {@code preTearDownAssertions()} throws.
   *
   * <p>
   * Override this as needed. Default implementation is empty.
   */
  void postTearDownAssertions() throws Exception;

  /**
   * Returns the {@code Properties} used to define the {@code DistributedSystem}.
   *
   * <p>
   * Override this as needed. This method is called by various {@code getSystem} methods in
   * {@code DistributedTestCase}.
   */
  Properties getDistributedSystemProperties();

  /**
   * Returns the {@code name} of the test method being executed.
   */
  String getName();

}
