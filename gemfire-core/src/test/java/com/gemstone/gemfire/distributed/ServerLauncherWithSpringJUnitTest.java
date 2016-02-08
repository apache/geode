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
package com.gemstone.gemfire.distributed;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.data.gemfire.support.SpringContextBootstrappingInitializer;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.distributed.AbstractLauncher.Status;
import com.gemstone.gemfire.distributed.ServerLauncher.Builder;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.process.ProcessType;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Extracted from ServerLauncherLocalJUnitTest.
 * 
 * @author John Blum
 * @author Kirk Lund
 */
@Category(IntegrationTest.class)
public class ServerLauncherWithSpringJUnitTest extends AbstractServerLauncherJUnitTestCase {

  @Before
  public final void setUpServerLauncherWithSpringTest() throws Exception {
    disconnectFromDS();
    System.setProperty(ProcessType.TEST_PREFIX_PROPERTY, getUniqueName()+"-");
  }

  @After
  public final void tearDownServerLauncherWithSpringTest() throws Exception {    
    disconnectFromDS();
    SpringContextBootstrappingInitializer.getApplicationContext().close();
  }

  // NOTE make sure bugs like Trac #51201 never happen again!!!
  @Test
  public void testBootstrapGemFireServerWithSpring() throws Throwable {
    this.launcher = new Builder()
      .setDisableDefaultServer(true)
      .setForce(true)
      .setMemberName(getUniqueName())
      .setSpringXmlLocation("spring/spring-gemfire-context.xml")
      .set(DistributionConfig.MCAST_PORT_NAME, "0")
      .build();

    assertNotNull(this.launcher);

    try {
      assertEquals(Status.ONLINE, this.launcher.start().getStatus());

      waitForServerToStart(this.launcher);

      Cache cache = this.launcher.getCache();

      assertNotNull(cache);
      assertTrue(cache.getCopyOnRead());
      assertEquals(0.95f, cache.getResourceManager().getCriticalHeapPercentage(), 0);
      assertEquals(0.85f, cache.getResourceManager().getEvictionHeapPercentage(), 0);
      assertFalse(cache.getPdxIgnoreUnreadFields());
      assertTrue(cache.getPdxPersistent());
      assertTrue(cache.getPdxReadSerialized());
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
