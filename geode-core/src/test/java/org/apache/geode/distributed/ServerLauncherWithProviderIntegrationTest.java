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
package org.apache.geode.distributed;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.AbstractLauncher.Status;
import org.apache.geode.distributed.ServerLauncher.Builder;
import org.apache.geode.internal.process.ProcessType;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.*;

/**
 * Extracted from ServerLauncherLocalIntegrationTest.
 */
@Category(IntegrationTest.class)
public class ServerLauncherWithProviderIntegrationTest extends AbstractServerLauncherIntegrationTestCase {

  @Before
  public final void setUpServerLauncherWithSpringTest() throws Exception {
    disconnectFromDS();
    System.setProperty(ProcessType.TEST_PREFIX_PROPERTY, getUniqueName()+"-");
  }

  @After
  public final void tearDownServerLauncherWithSpringTest() throws Exception {
    MockServerLauncherCacheProvider.setCache(null);
    disconnectFromDS();
    
  }

  // NOTE make sure bugs like Trac #51201 never happen again!!!
  @Test
  public void testBootstrapGemFireServerWithProvider() throws Throwable {
    Cache mockCache = Mockito.mock(Cache.class);
    MockServerLauncherCacheProvider.setCache(mockCache);
    this.launcher = new Builder()
      .setDisableDefaultServer(true)
      .setForce(true)
      .setMemberName(getUniqueName())
      .setSpringXmlLocation("spring/spring-gemfire-context.xml")
        .set(MCAST_PORT, "0")
      .build();

    assertNotNull(this.launcher);

    try {
      assertEquals(Status.ONLINE, this.launcher.start().getStatus());

      waitForServerToStart(this.launcher);

      Cache cache = this.launcher.getCache();

      assertEquals(mockCache, cache);
    }
    catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      assertNull(this.launcher.getCache());
    }
    catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }
}
