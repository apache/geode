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

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewayQueueEvent;

// Note: This class is used by AsyncEventQueueValidationsJUnitTest
// testAsyncEventQueueConfiguredFromXmlUsesFilter
public class MyGatewayEventFilter implements GatewayEventFilter, Declarable {

  private AtomicInteger beforeEnqueueInvocations = new AtomicInteger();

  private AtomicInteger beforeTransmitInvocations = new AtomicInteger();

  private AtomicInteger afterAcknowledgementInvocations = new AtomicInteger();

  @Override
  public boolean beforeEnqueue(GatewayQueueEvent event) {
    beforeEnqueueInvocations.incrementAndGet();
    return true;
  }

  @Override
  public boolean beforeTransmit(GatewayQueueEvent event) {
    beforeTransmitInvocations.incrementAndGet();
    return true;
  }

  @Override
  public void afterAcknowledgement(GatewayQueueEvent event) {
    afterAcknowledgementInvocations.incrementAndGet();
  }

  public int getBeforeEnqueueInvocations() {
    return this.beforeEnqueueInvocations.get();
  }

  public int getBeforeTransmitInvocations() {
    return this.beforeTransmitInvocations.get();
  }

  public int getAfterAcknowledgementInvocations() {
    return this.afterAcknowledgementInvocations.get();
  }

  @Override
  public void init(Properties props) {}

  @Override
  public void close() {}
}
