/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
  
  public void tearDown2() throws Exception {
    configureClientStop2();
    super.tearDown2();
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
