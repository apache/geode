/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.test.dunit.tests;

import com.gemstone.gemfire.test.dunit.DistributedTestCase;

import java.util.Properties;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.MCAST_PORT;
import static com.gemstone.gemfire.test.dunit.Invoke.invokeInEveryVM;

/**
 * Verifies that overriding {@code getDistributedSystemProperties} results
 * in {@code disconnectAllFromDS} during tear down.
 */
public class OverridingGetPropertiesDisconnectsAllDUnitTest extends DistributedTestCase {

  public OverridingGetPropertiesDisconnectsAllDUnitTest(final String name) {
    super(name);
  }

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

  public void testDisconnects() throws Exception {
    invokeInEveryVM(() -> assertFalse(getDistributedSystemProperties().isEmpty()));
    invokeInEveryVM(() -> assertNotNull(getSystem()));
  }
}
