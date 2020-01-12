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

package org.apache.geode.cache30;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Properties;

import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.internal.cache.xmlcache.AsyncEventQueueCreation;

public class CacheXml70DUnitTestHelper {

  public static class MyAsyncEventListener implements AsyncEventListener, Declarable {

    @Override
    public boolean processEvents(List<AsyncEvent> events) {
      return true;
    }

    @Override
    public void close() {}

    @Override
    public void init(Properties properties) {}
  }

  public static void validateAsyncEventQueue(AsyncEventQueue eventChannelFromXml,
      AsyncEventQueue channel) {
    assertEquals("AsyncEventQueue id doesn't match", eventChannelFromXml.getId(), channel.getId());
    assertEquals("AsyncEventQueue batchSize doesn't match", eventChannelFromXml.getBatchSize(),
        channel.getBatchSize());
    assertEquals("AsyncEventQueue batchTimeInterval doesn't match",
        eventChannelFromXml.getBatchTimeInterval(), channel.getBatchTimeInterval());
    assertEquals("AsyncEventQueue batchConflationEnabled doesn't match",
        eventChannelFromXml.isBatchConflationEnabled(), channel.isBatchConflationEnabled());
    assertEquals("AsyncEventQueue persistent doesn't match", eventChannelFromXml.isPersistent(),
        channel.isPersistent());
    assertEquals("AsyncEventQueue diskStoreName doesn't match",
        eventChannelFromXml.getDiskStoreName(), channel.getDiskStoreName());
    assertEquals("AsyncEventQueue isDiskSynchronous doesn't match",
        eventChannelFromXml.isDiskSynchronous(), channel.isDiskSynchronous());
    assertEquals("AsyncEventQueue maximumQueueMemory doesn't match",
        eventChannelFromXml.getMaximumQueueMemory(), channel.getMaximumQueueMemory());
    assertEquals("AsyncEventQueue Parallel doesn't match", eventChannelFromXml.isParallel(),
        channel.isParallel());
    assertEquals("AsyncEventQueue GatewayEventFilters doesn't match",
        eventChannelFromXml.getGatewayEventFilters().size(),
        channel.getGatewayEventFilters().size());
    assertTrue("AsyncEventQueue should be instanceof Creation",
        eventChannelFromXml instanceof AsyncEventQueueCreation);
    assertTrue("AsyncEventQueue should be instanceof Impl", channel instanceof AsyncEventQueueImpl);
  }

}
