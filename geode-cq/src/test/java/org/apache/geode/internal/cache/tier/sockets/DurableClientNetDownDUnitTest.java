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
package com.gemstone.gemfire.internal.cache.tier.sockets;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.DistributedTest;

/**
 * Class <code>DurableClientCrashDUnitTest</code> tests durable client
 * functionality when clients are disconnected from servers.
 * 
 * @since GemFire 5.2
 */
@Category(DistributedTest.class)
public class DurableClientNetDownDUnitTest extends DurableClientCrashDUnitTest {

  @Override
  protected final void preTearDownDurableClientTestCase() throws Exception {
    //ensure that the test flag is no longer set in this vm
    this.durableClientVM.invoke(() -> CacheServerTestUtil.reconnectClient());
  }

  @Override
  public void setPrimaryRecoveryCheck() {
  }

  @Override
  public void checkPrimaryRecovery() {
  }

  @Override
  public void configureClientStop1() {
  }

  @Override
  public void configureClientStop2() {
  }

  @Override
  public void closeDurableClient() {
    this.durableClientVM.invoke(() -> CacheServerTestUtil.reconnectClient());
    this.durableClientVM.invoke(() -> CacheServerTestUtil.closeCache());
  }

  @Override
  public void disconnectDurableClient() {
    this.durableClientVM.invoke(() -> CacheServerTestUtil.disconnectClient());
  }

  @Override
  public void disconnectDurableClient(boolean keepAlive) {
    this.disconnectDurableClient();
  }

  @Override
  public void restartDurableClient(Object[] args) {
    this.durableClientVM.invoke(() -> CacheServerTestUtil.reconnectClient());  
  }

  @Override
  public void verifyListenerUpdatesDisconnected(int numberOfEntries) {
    this.verifyListenerUpdates(numberOfEntries);
  }

  @Override
  public void verifyListenerUpdates(int numEntries, int numEntriesBeforeDisconnect) {
    this.verifyListenerUpdatesEntries(numEntries, numEntriesBeforeDisconnect);
  }
  
}
