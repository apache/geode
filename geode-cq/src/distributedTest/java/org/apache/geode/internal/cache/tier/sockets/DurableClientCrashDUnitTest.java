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
package org.apache.geode.internal.cache.tier.sockets;

import static org.junit.Assert.assertNotNull;

import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * Class <code>DurableClientCrashDUnitTest</code> tests durable client functionality when clients
 * crash.
 *
 * @since GemFire 5.2
 */
@Category({ClientSubscriptionTest.class})
public class DurableClientCrashDUnitTest extends DurableClientTestCase {

  @Override
  protected final void postSetUpDurableClientTestCase() {
    configureClientStop1();
  }

  public void configureClientStop1() {
    this.durableClientVM.invoke(() -> CacheServerTestUtil.setClientCrash(Boolean.TRUE));
  }

  @Override
  protected void preTearDownDurableClientTestCase() throws Exception {
    configureClientStop2();
  }

  public void configureClientStop2() {
    this.durableClientVM.invoke(() -> CacheServerTestUtil.setClientCrash(Boolean.FALSE));
  }

  @Override
  public void verifySimpleDurableClient() {
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
      }
    });
  }

  @Override
  public void verifySimpleDurableClientMultipleServers() {
    // Verify the durable client is no longer on server1
    this.server1VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
      }
    });

    // Verify the durable client is no longer on server2
    this.server2VM.invoke(new CacheSerializableRunnable("Verify durable client") {
      public void run2() throws CacheException {
        // Find the proxy
        checkNumberOfClientProxies(1);
        CacheClientProxy proxy = getClientProxy();
        assertNotNull(proxy);
      }
    });
  }
}
