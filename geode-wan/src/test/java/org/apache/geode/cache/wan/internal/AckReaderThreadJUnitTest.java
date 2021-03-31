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
package org.apache.geode.cache.wan.internal;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.BatchException70;
import org.apache.geode.test.fake.Fakes;

public class AckReaderThreadJUnitTest {

  private GemFireCacheImpl cache;
  private AbstractGatewaySender sender;
  private GatewaySenderEventRemoteDispatcher dispatcher;

  @Before
  public void setUpGemFire() {
    createCache();
    createSender();
    createDispatcher();
  }

  private void createCache() {
    // Mock cache
    this.cache = Fakes.cache();
  }

  private void createSender() {
    // Mock gateway sender
    this.sender = mock(AbstractGatewaySender.class);
    when(this.sender.getCache()).thenReturn(this.cache);
  }

  private void createDispatcher() {
    this.dispatcher = mock(GatewaySenderEventRemoteDispatcher.class);
  }

  @Test
  public void testLogBatchExceptions() {

    // Create AckReaderThread
    GatewaySenderEventRemoteDispatcher.AckReaderThread thread =
        this.dispatcher.new AckReaderThread(this.sender, "AckReaderThread");

    // Create parent BatchException containing a NullPointerException with no index
    List<BatchException70> batchExceptions = new ArrayList<>();
    batchExceptions
        .add(new BatchException70("null pointer exception", new NullPointerException(), -1, 0));
    BatchException70 batchException = new BatchException70(batchExceptions);

    // Attempt to handle the parent BatchException. If this method fails, an Exception will be
    // thrown, and this test will fail. If it succeeds, there won't be an exception, and the test
    // will fall through.
    thread.logBatchExceptions(batchException);
  }
}
