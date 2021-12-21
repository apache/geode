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
package org.apache.geode.internal.cache.wan;

import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.mockito.Mockito.spy;

import org.junit.After;
import org.junit.Before;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.RegionQueue;

public abstract class WaitUntilGatewaySenderFlushedCoordinatorJUnitTest {

  protected GemFireCacheImpl cache;

  protected AbstractGatewaySender sender;

  @Before
  public void setUp() {
    createCache();
    createGatewaySender();
  }

  @After
  public void tearDown() {
    if (cache != null) {
      cache.close();
    }
  }

  private void createCache() {
    cache = (GemFireCacheImpl) new CacheFactory().set(MCAST_PORT, "0")
        .set(LOG_LEVEL, "warning").create();
  }

  protected void createGatewaySender() {
    sender = spy(AbstractGatewaySender.class);
    sender.cache = cache;
    sender.eventProcessor = getEventProcessor();
  }

  protected RegionQueue getQueue() {
    return sender.eventProcessor.getQueue();
  }

  protected abstract AbstractGatewaySenderEventProcessor getEventProcessor();
}
