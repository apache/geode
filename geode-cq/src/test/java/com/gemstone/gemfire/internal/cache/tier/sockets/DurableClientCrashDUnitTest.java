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

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;

/**
 * Class <code>DurableClientCrashDUnitTest</code> tests durable client
 * functionality when clients crash.
 * 
 * @author Abhijit Bhaware
 * 
 * @since 5.2
 */
public class DurableClientCrashDUnitTest extends DurableClientTestCase {

  public DurableClientCrashDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    configureClientStop1();
  }
  
  public void configureClientStop1()
  {
    this.durableClientVM.invoke(CacheServerTestUtil.class, "setClientCrash", new Object[] {new Boolean(true)});    
  }
  
  @Override
  protected void preTearDownDurableClientTestCase() throws Exception {
    configureClientStop2();
  }
  
  public void configureClientStop2()
  {
    this.durableClientVM.invoke(CacheServerTestUtil.class, "setClientCrash", new Object[] {new Boolean(false)});    
  }
  
  public void verifySimpleDurableClient() {
    this.server1VM
        .invoke(new CacheSerializableRunnable("Verify durable client") {
          public void run2() throws CacheException {
            // Find the proxy
            checkNumberOfClientProxies(1);
            CacheClientProxy proxy = getClientProxy();
            assertNotNull(proxy);
          }
        });
  }
  
  public void verifySimpleDurableClientMultipleServers() 
  {
    // Verify the durable client is no longer on server1
    this.server1VM
        .invoke(new CacheSerializableRunnable("Verify durable client") {
          public void run2() throws CacheException {
            // Find the proxy
            checkNumberOfClientProxies(1);
            CacheClientProxy proxy = getClientProxy();
            assertNotNull(proxy);
          }
        });

    // Verify the durable client is no longer on server2
    this.server2VM
        .invoke(new CacheSerializableRunnable("Verify durable client") {
          public void run2() throws CacheException {
            // Find the proxy
            checkNumberOfClientProxies(1);
            CacheClientProxy proxy = getClientProxy();
            assertNotNull(proxy);
          }
        });
  }
  
  // AB: Following tests are not relevant for client crash case.
  
  public void testCqCloseExceptionDueToActiveConnection() throws Exception {}

  public void testCloseCacheProxy() throws Exception {}

}
