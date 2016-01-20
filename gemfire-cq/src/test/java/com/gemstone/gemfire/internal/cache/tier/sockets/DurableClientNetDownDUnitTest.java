/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;


/**
 * Class <code>DurableClientCrashDUnitTest</code> tests durable client
 * functionality when clients are disconnected from servers.
 * 
 * @author Abhijit Bhaware
 * 
 * @since 5.2
 */
public class DurableClientNetDownDUnitTest extends DurableClientCrashDUnitTest {

  public DurableClientNetDownDUnitTest(String name) {
    super(name);
  }
  
  @Override
  public void tearDown2() throws Exception {
    //ensure that the test flag is no longer set in this vm
    this.durableClientVM.invoke(CacheServerTestUtil.class, "reconnectClient");
    super.tearDown2();
  }

  public void setPrimaryRecoveryCheck() {}
  
  public void checkPrimaryRecovery() {}
  
  public void configureClientStop1() {}
  
  public void configureClientStop2() {}
  
  public void closeDurableClient()
  {
    this.durableClientVM.invoke(CacheServerTestUtil.class, "reconnectClient");
    this.durableClientVM.invoke(CacheServerTestUtil.class, "closeCache");
  }
  
  public void disconnectDurableClient()
  {
    this.durableClientVM.invoke(CacheServerTestUtil.class, "disconnectClient");
  }

  public void disconnectDurableClient(boolean keepAlive)
  {
    this.disconnectDurableClient();
  }
  
  public void restartDurableClient(Object[] args)
  {
    this.durableClientVM.invoke(CacheServerTestUtil.class, "reconnectClient");  
  }
  
  public void verifyListenerUpdatesDisconnected(int numberOfEntries)
  {
    this.verifyListenerUpdates(numberOfEntries);
  }

  public void verifyListenerUpdates(int numEntries, int numEntriesBeforeDisconnect)
  {
    this.verifyListenerUpdatesEntries(numEntries, numEntriesBeforeDisconnect);
  }
  
}
