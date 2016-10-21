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
package org.apache.geode.test.dunit.tests;

import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.dunit.Assert.*;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;

/**
 * Verifies that overriding {@code getDistributedSystemProperties} results in
 * {@code disconnectAllFromDS} during tear down.
 */
@Category(DistributedTest.class)
public class JUnit4OverridingGetPropertiesDisconnectsAllDUnitTest
    extends JUnit4DistributedTestCase {

  @Override
  public final void preTearDownAssertions() throws Exception {
    invokeInEveryVM(() -> assertNotNull(basicGetSystem()));
  }

  @Override
  public final void postTearDownAssertions() throws Exception {
    invokeInEveryVM(() -> assertNull(basicGetSystem()));
  }

  @Override
  public final Properties getDistributedSystemProperties() {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    return props;
  }

  @Test
  public void testDisconnects() throws Exception {
    invokeInEveryVM(() -> assertFalse(getDistributedSystemProperties().isEmpty()));
    invokeInEveryVM(() -> assertNotNull(getSystem()));
  }
}
