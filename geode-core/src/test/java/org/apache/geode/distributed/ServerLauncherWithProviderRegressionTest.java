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
package org.apache.geode.distributed;

import static org.apache.geode.internal.process.ProcessType.PROPERTY_TEST_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Extracted from {@link ServerLauncherLocalIntegrationTest}. This tests the same mechanism used by
 * Spring Data GemFire/Geode. This test confirms the fix for TRAC #51201 (see below).
 *
 * <p>
 * TRAC #51201: ServerLauncher.start fails to configure server with Spring
 */
@Category(IntegrationTest.class)
public class ServerLauncherWithProviderRegressionTest extends ServerLauncherIntegrationTestCase {

  private Cache providerCache;

  @Before
  public void setUp() throws Exception {
    disconnectFromDS();
    System.setProperty(PROPERTY_TEST_PREFIX, getUniqueName() + "-");

    providerCache = mock(Cache.class);
    TestServerLauncherCacheProvider.setCache(providerCache);
  }

  @After
  public void tearDown() throws Exception {
    TestServerLauncherCacheProvider.setCache(null);
    disconnectFromDS();
  }

  @Test
  public void startGetsCacheFromServerLauncherCacheProvider() throws Exception {
    startServer(newBuilder().setDisableDefaultServer(true).setSpringXmlLocation(springXml()));

    Cache cache = launcher.getCache();

    assertThat(cache).isEqualTo(providerCache);
  }

  private String springXml() {
    return "spring/spring-gemfire-context.xml";
  }
}
