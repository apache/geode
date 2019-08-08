/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.alerting.internal;

import static org.apache.geode.alerting.internal.AlertingProviderLoader.ALERTING_PROVIDER_NAME_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.alerting.spi.AlertingProvider;
import org.apache.geode.test.junit.categories.AlertingTest;

/**
 * Unit tests for {@link AlertingProviderLoader}.
 */
@Category({AlertingTest.class, AlertingTest.class})
public class AlertingProviderLoaderTest {

  private AlertingProviderLoader alertingProviderLoader;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() {
    alertingProviderLoader = new AlertingProviderLoader();
  }

  @Test
  public void createProviderAgent_usesSystemPropertySetTo_NullProviderAgent() {
    System.setProperty(ALERTING_PROVIDER_NAME_PROPERTY, SimpleAlertingProvider.class.getName());

    AlertingProvider value = alertingProviderLoader.load();

    assertThat(value).isInstanceOf(SimpleAlertingProvider.class);
  }

  @Test
  public void createProviderAgent_usesSystemPropertySetTo_SimpleProviderAgent() {
    System.setProperty(ALERTING_PROVIDER_NAME_PROPERTY, TestAlertingProvider.class.getName());

    AlertingProvider value = alertingProviderLoader.load();

    assertThat(value).isInstanceOf(TestAlertingProvider.class);
  }

  @Test
  public void createProviderAgent_usesNullProviderAgent_whenClassNotFoundException() {
    System.setProperty(ALERTING_PROVIDER_NAME_PROPERTY, TestAlertingProvider.class.getSimpleName());

    AlertingProvider value = alertingProviderLoader.load();

    assertThat(value).isInstanceOf(SimpleAlertingProvider.class);
  }

  @Test
  public void createProviderAgent_usesNullProviderAgent_whenClassCastException() {
    System.setProperty(ALERTING_PROVIDER_NAME_PROPERTY, NotAlertingProvider.class.getName());

    AlertingProvider value = alertingProviderLoader.load();

    assertThat(value).isInstanceOf(SimpleAlertingProvider.class);
  }

  static class TestAlertingProvider implements AlertingProvider {

    @Override
    public int getPriority() {
      return Integer.MIN_VALUE;
    }
  }

  @SuppressWarnings("all")
  static class NotAlertingProvider {

  }
}
