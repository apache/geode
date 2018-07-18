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

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.Test;

import org.apache.geode.test.dunit.DistributedTestCase;

/**
 * Verifies that overriding {@code getDistributedSystemProperties} results in
 * {@code disconnectAllFromDS} during tear down.
 */

@SuppressWarnings("serial")
public class OverridingGetPropertiesDisconnectsAllDistributedTest extends DistributedTestCase {

  @Override
  public final void preTearDownAssertions() throws Exception {
    invokeInEveryVM(() -> {
      assertThat(basicGetSystem().isConnected()).isTrue();
    });
  }

  @Override
  public final void postTearDownAssertions() throws Exception {
    invokeInEveryVM(() -> {
      assertThat(basicGetSystem().isConnected()).isFalse();
    });
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    return config;
  }

  @Test
  public void testDisconnects() throws Exception {
    invokeInEveryVM(() -> {
      assertThat(getDistributedSystemProperties()).isNotEmpty();
    });
    invokeInEveryVM(() -> {
      assertThat(getSystem().isConnected()).isTrue();
    });
  }
}
