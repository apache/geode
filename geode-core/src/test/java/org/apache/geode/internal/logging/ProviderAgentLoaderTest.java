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
package org.apache.geode.internal.logging;

import static org.apache.geode.internal.logging.ProviderAgentLoader.PROVIDER_AGENT_NAME_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.logging.ProviderAgentLoader.AvailabilityChecker;
import org.apache.geode.internal.logging.log4j.Log4jAgent;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Unit tests for {@link ProviderAgentLoader}.
 */
@Category(LoggingTest.class)
public class ProviderAgentLoaderTest {

  private ProviderAgentLoader providerAgentLoader;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() {
    providerAgentLoader = new ProviderAgentLoader();
  }

  @Test
  public void createProviderAgent_defaultsTo_Log4jAgent() {
    assertThat(providerAgentLoader.createProviderAgent()).isInstanceOf(Log4jAgent.class);
  }

  @Test
  public void createProviderAgent_usesSystemPropertySetTo_Log4jAgent() {
    System.setProperty(PROVIDER_AGENT_NAME_PROPERTY, Log4jAgent.class.getName());

    assertThat(providerAgentLoader.createProviderAgent()).isInstanceOf(Log4jAgent.class);
  }

  @Test
  public void createProviderAgent_usesSystemPropertySetTo_NullProviderAgent() {
    System.setProperty(PROVIDER_AGENT_NAME_PROPERTY, NullProviderAgent.class.getName());

    assertThat(providerAgentLoader.createProviderAgent()).isInstanceOf(NullProviderAgent.class);
  }

  @Test
  public void createProviderAgent_usesSystemPropertySetTo_SimpleProviderAgent() {
    System.setProperty(PROVIDER_AGENT_NAME_PROPERTY, SimpleProviderAgent.class.getName());

    assertThat(providerAgentLoader.createProviderAgent()).isInstanceOf(
        SimpleProviderAgent.class);
  }

  @Test
  public void createProviderAgent_usesNullProviderAgent_whenClassNotFoundException() {
    System.setProperty(PROVIDER_AGENT_NAME_PROPERTY, SimpleProviderAgent.class.getSimpleName());

    assertThat(providerAgentLoader.createProviderAgent()).isInstanceOf(NullProviderAgent.class);
  }

  @Test
  public void createProviderAgent_usesNullProviderAgent_whenClassCastException() {
    System.setProperty(PROVIDER_AGENT_NAME_PROPERTY, NotProviderAgent.class.getName());

    assertThat(providerAgentLoader.createProviderAgent()).isInstanceOf(NullProviderAgent.class);
  }

  @Test
  public void findProviderAgent_defaultsTo_createProviderAgent() {
    assertThat(providerAgentLoader.findProviderAgent()).isInstanceOf(Log4jAgent.class);
  }

  @Test
  public void isLog4jCoreAvailable_isTrue() {
    assertThat(providerAgentLoader.isDefaultAvailable()).isTrue();
  }

  @Test
  public void isLog4jCoreAvailable_usesProvidedAvailabilityChecker() {
    providerAgentLoader = new ProviderAgentLoader(mock(AvailabilityChecker.class));

    assertThat(providerAgentLoader.isDefaultAvailable()).isFalse();
  }

  @Test
  public void createProviderAgent_usesNullProviderAgent_whenIsDefaultAvailableIsFalse() {
    providerAgentLoader = new ProviderAgentLoader(mock(AvailabilityChecker.class));

    assertThat(providerAgentLoader.createProviderAgent()).isInstanceOf(NullProviderAgent.class);
  }

  static class SimpleProviderAgent implements ProviderAgent {

    @Override
    public void configure(LogConfig logConfig,
        LogLevelUpdateOccurs logLevelUpdateOccurs,
        LogLevelUpdateScope logLevelUpdateScope) {
      // nothing
    }

    @Override
    public void cleanup() {
      // nothing
    }
  }

  @SuppressWarnings("all")
  static class NotProviderAgent {

  }
}
