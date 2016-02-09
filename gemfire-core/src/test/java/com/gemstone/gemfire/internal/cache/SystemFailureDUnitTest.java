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
package com.gemstone.gemfire.internal.cache;

import java.util.concurrent.atomic.AtomicInteger;
import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.util.*;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.RMIException;
import com.gemstone.gemfire.test.dunit.VM;

import java.io.Serializable;
import java.util.ArrayList;

import util.TestException;

import junit.framework.Assert;

/**
 * This class tests the response of GemFire to various
 * occurrences of {@link VirtualMachineError}
 * 
 * @author jpenney
 * @since 5.1
 */
public class SystemFailureDUnitTest extends DistributedCacheTestCase {
  
  static final String REGION_NAME = "SystemFailureDUnitTest";
  static final Scope SCOPE = Scope.DISTRIBUTED_ACK;
  
  volatile static Object newValue, oldValue;
  
  public SystemFailureDUnitTest(String name) {
    super(name);
  }

  /////////  Public test methods

  public void testNullFailure() {
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("TODO: this test needs to use VM#bounce.");
    try {
      SystemFailure.initiateFailure(null);
      fail("Null failure set allowed");
    }
    catch (IllegalArgumentException e) {
      // pass
    }
  }
  
  /**
   * @see StackOverflowError
   */
  public void _testStackOverflow()
    throws CacheException, InterruptedException {
    String exceptions =
      StackOverflowError.class.getName() 
      + "||"
//      + OutOfMemoryError.class.getName()
//      + "||"
      + AssertionError.class.getName();
    
    try {
      String name = "testStackOverflow";
      
      doMessage(
          "<ExpectedException action=add>"
          + exceptions
          + "</ExpectedException>");
      doCreateEntry(name);
      doVerifyDisconnected();
    }
    finally {
      doMessage(
        "<ExpectedException action=remove>"
        + exceptions
        + "</ExpectedException>");
      resetVM();
    }
  }

  /**
   * @see OutOfMemoryError
   * @throws CacheException
   * @throws InterruptedException
   */
  public void _testOutOfMemory()
    throws CacheException, InterruptedException {
  
    String exceptions =
      OutOfMemoryError.class.getName()
      + "||"
      + AssertionError.class.getName();
    try {
      String name = "testOutOfMemory";
      doMessage(
          "<ExpectedException action=add>"
          + exceptions
          + "</ExpectedException>");
      doCreateEntry(name);
      doVerifyDisconnected();
    }
    finally {
      doMessage(
        "<ExpectedException action=remove>"
        + exceptions
        + "</ExpectedException>");
      resetVM();
    }
  
  }

  /**
   * @see OutOfMemoryError
   * @throws CacheException
   * @throws InterruptedException
   */
  public void _testPersistentOutOfMemory()
    throws CacheException, InterruptedException {
  
    String exceptions =
      OutOfMemoryError.class.getName()
      + "||"
      + AssertionError.class.getName();
    try {
      String name = "testPersistentOutOfMemory";
      doExec("setListener2");
      doMessage(
          "<ExpectedException action=add>"
          + exceptions
          + "</ExpectedException>");
      doCreateEntry(name);
      doVerifyDisconnected();
    }
    finally {
      doMessage(
        "<ExpectedException action=remove>"
        + exceptions
        + "</ExpectedException>");
      resetVM();
    }
  
  }

  /**
   * @see OutOfMemoryError
   * @throws CacheException
   * @throws InterruptedException
   */
  public void _testMemoryMonitor()
    throws CacheException, InterruptedException {
  
    String exceptions =
      OutOfMemoryError.class.getName()
      + "||"
      + AssertionError.class.getName();
    try {
      String name = "testMemoryMonitor";
      doExec("setListener2");
      doMessage(
          "<ExpectedException action=add>"
          + exceptions
          + "</ExpectedException>");
      doCreateEntry(name);
      doVerifyDisconnected();
    }
    finally {
      doMessage(
        "<ExpectedException action=remove>"
        + exceptions
        + "</ExpectedException>");
      resetVM();
    }

  }

    /**
     * @see InternalError
     * @throws CacheException
     * @throws InterruptedException
     */
    public void _testInternalError()
    throws CacheException, InterruptedException {
      String exceptions =
        InternalError.class.getName()
        + "||"
        + AssertionError.class.getName();
      try {
        String name = "testInternalError";
  
        doMessage(
            "<ExpectedException action=add>"
            + exceptions
            + "</ExpectedException>");
        doCreateEntry(name);
        doVerifyDisconnected();
      }
      finally {
        doMessage(
          "<ExpectedException action=remove>"
          + exceptions
          + "</ExpectedException>");
        resetVM();
      }
  }

  /**
   * @see UnknownError
   * @throws CacheException
   * @throws InterruptedException
   */
  public void _testUnknownError()
      throws CacheException, InterruptedException {
    String exceptions =
      UnknownError.class.getName()
      + "||"
      + AssertionError.class.getName();
      try {
        String name = "testUnknownError";
        doMessage(
            "<ExpectedException action=add>"
            + exceptions
            + "</ExpectedException>");
        doCreateEntry(name);
        doVerifyDisconnected();
      }
      finally {
        doMessage(
          "<ExpectedException action=remove>"
          + exceptions
          + "</ExpectedException>");
        resetVM();
      }
   }

  /**
   * This class can never be successfully loaded.
   * 
   * @author jpenney
   */
  static class SickoClass {
    static private boolean threeCardMonte() {
      return true;
    }
    
    static {
      // Javac isn't keen about a static initializer
      // that has a throw in it, so this test is
      // to obfuscate my obviously bogus code...
      if (System.currentTimeMillis() != 0 || threeCardMonte()) {
        throw new Error("annoying, aren't I?");
      }
    }
  }
  
  /**
     * Create some sort of horrible failure that is <em>not</em>
     * a VirtualMachineError.
     */
    public void _testError()
    throws CacheException, InterruptedException {
    // In this case we do NOT expect a failure
    String exceptions =
      Error.class.getName();

    try {
      String name = "testError";
      doCreateEntry(name);
      Assert.assertTrue(doVerifyConnected());
      
       doMessage(
          "<ExpectedException action=add>"
          + exceptions
          + "</ExpectedException>");
    }
    finally {
      doMessage(
          "<ExpectedException action=remove>"
          + exceptions
          + "</ExpectedException>");
      // just in case
      resetVM();
    }
  }

  static protected final AtomicInteger listenerCount = new AtomicInteger(0);
  static protected Integer getListenerCount() {
    return new Integer(listenerCount.get());
  }
  
  static private final Runnable listener1 = new Runnable() {
    public void run() {
      com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("Inside of preListener1");
      listenerCount.addAndGet(1);
    }
  };
  
  protected static void setListener1() {
    listenerCount.set(0);
    SystemFailure.setFailureAction(listener1);
  }
  
  protected static void setListener2() {
    peskyMemory = null;
    synchronized (SystemFailureDUnitTest.class) {
      SystemFailureDUnitTest.class.notify();
    }
  }
  
  /**
   * Verify that listener gets called, and exactly once.
   */
  public void _testListener()
      throws CacheException, InterruptedException {

  String exceptions =
      Error.class.getName();
  try {
    String name = "testListener";
    doExec("setListener1");
    
    doMessage(
        "<ExpectedException action=add>"
        + exceptions
        + "</ExpectedException>");
    doCreateEntry(name);

    Integer count = (Integer)doExec("getListenerCount");
    Assert.assertEquals(1, count.intValue());
    doVerifyDisconnected();
  }
  finally {
    doMessage(
      "<ExpectedException action=remove>"
      + exceptions
      + "</ExpectedException>");
    resetVM();
  }

}

//  protected static void doReset() {
//    // TODO instead of trying to patch up this VM, Lise should create
//    // me a brand spanking new one
//    throw new TestException("Sorry, ask Lise to fix this");
//    
//    // You'll have to un-comment some methods in order to make
//    // the following hack work ONCE on a VM...
////    try {
////      Thread.sleep(5000);
////    }
////    catch (InterruptedException e) {
////      fail("interrupted");
////    }
////    
////    SystemFailure.reset(); <--- here
////    DistributedCacheTestCase.cache = null;
////
////    // Discard the existing cache instance
////    GemFireCache.clearInstance(); <--- here
////    
////    // This is just to stop the stat sampler thread
////    HostStatSampler.emergencyStop(); <--- here
//  }
  
  private void resetVM() {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    vm.bounce();
  }
  
  private static final long MAX_WAIT = 60 * 1000;
  
  private boolean doVerifyConnected() {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    Object o = vm.invoke(this.getClass(), "verifyConnected");
    return ((Boolean)o).booleanValue();
  }
  
  protected static Boolean verifyConnected() {
    if (SystemFailure.getFailure() != null) {
      com.gemstone.gemfire.test.dunit.Assert.fail("System failure present!", SystemFailure.getFailure());
      return Boolean.FALSE;
    }
    GemFireCacheImpl gfc = (GemFireCacheImpl)cache;
    if (gfc.isClosed()) {
      fail("Cache is closing/closed!");
      return Boolean.FALSE;
    }
    
    // Let's inspect the distributed system.  It should also
    // be connected.
    if (system.getCancelCriterion().cancelInProgress() != null) {
      fail("distributed system cancel in progress");
      return Boolean.FALSE;
    }
    if (!system.isConnected()) {
      fail("distributed system not connected");
      return Boolean.FALSE;
    }
    
    return Boolean.TRUE;
  }

  private boolean doVerifyDisconnected() {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    return ((Boolean)vm.invoke(this.getClass(), "verifyDisconnected"))
        .booleanValue();
  }
  
  protected static Boolean verifyDisconnected() {
    if (SystemFailure.getFailure() == null) {
      fail("No system failure present!");
      return Boolean.FALSE;
    }
    GemFireCacheImpl gfc = (GemFireCacheImpl)cache;

    // Allow cache time to finish disconnecting
    long done = System.currentTimeMillis() + MAX_WAIT;
    for (;;) {
       long now = System.currentTimeMillis();
       if (now >= done) {
         fail("Time out waiting for cache to close: " + cache.toString());
         return Boolean.FALSE;
       }
       if (gfc.isClosed()) {
         break;
       }
       try {
         Thread.sleep(2000);
       }
       catch (InterruptedException e) {
         fail("interrupted");
         return Boolean.FALSE;
       }
    }
    
    // At this point, the cache we peeked earlier should be unavailable
    Assert.assertTrue(GemFireCacheImpl.getInstance() == null);
    
    // Ditto for the distributed system
    InternalDistributedSystem ids = (InternalDistributedSystem)
        gfc.getDistributedSystem();
    if (ids == null) {
      return Boolean.TRUE; // uhhh, pretty dead!
    }
    try {
      DistributionManager dm = (DistributionManager)ids.getDistributionManager();
      if (dm == null) {
        return Boolean.TRUE;
      }
      return new Boolean(dm.getCancelCriterion().cancelInProgress() != null);
    }
    catch (CancelException e) {
      // TODO -- it would be nice to avoid the checkConnected() call above
      return Boolean.TRUE;
    }
  }
  
  static protected volatile ArrayList peskyMemory;
  
  private Object doExec(String method) {
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    return vm.invoke(this.getClass(), method);
  }
  
  private void doMessage(String text) {
    Object args[] = new Object[] {text};
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    vm.invoke(this.getClass(), "message", args);
  }
  
  protected static void message(String s) {
    System.out.println(s);
    System.err.println(s);
    com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info(s);
    cache.getLogger().info(s);
  }
  
  /**
   * Create a region with one entry in this test's region with the
   * given name and attributes.
   */
  private static void createEntry(String name, int ttl, 
                                  ExpirationAction action,
                                  GenericListener l)
    throws CacheException {

    Region region = getRegion();
    AttributesFactory factory =
      new AttributesFactory(region.getAttributes());
    factory.setStatisticsEnabled(true);
    factory.setEntryTimeToLive(new ExpirationAttributes(ttl, action));
    factory.setScope(SCOPE);
    factory.setCacheListener(l);

    Region sub =
      region.createSubregion(name, factory.create());
    sub.create(name, new Integer(0), sub.getCache().getDistributedSystem().getDistributedMember());
  }
  
  static private final GenericListener listener_stackOverflow = new GenericListener() {
    /**
     * gratuitous and stupid recursion
     */
    private void forceOverflow() {
      forceOverflow();
    }

    public void afterCreate(EntryEvent event) {
      forceOverflow();
    }
  };
  
  private static final GenericListener listener_outOfMemory = new GenericListener() {
    /**
     * Allocate objects until death
     */
    private void forceOutOfMemory() {
      ArrayList junk = new ArrayList();
      for (;;) {
        junk.add(new long[100000]);
      }
    }
    public void afterCreate(EntryEvent event) {
      com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("Invoking afterCreate on listener; name=" +
                          event.getKey());
      forceOutOfMemory();
    }
  };
  
  static private final GenericListener listener_persistentOutOfMemory = new GenericListener() {
      /**
       * Allocate objects until death
       */
      private void forceOutOfMemory() {
        peskyMemory = new ArrayList();
        // Allocate this _before_ exhausting memory :-)
        final TestException whoops = new TestException("Timeout!");
        try {
          for (;;) {
            peskyMemory.add(new long[100000]);
          }
        }
        catch (OutOfMemoryError e) {
          // Now then...while we're out of memory...
          // ...signal failure
          SystemFailure.setFailure(e);
          
          // Next, wait for the listener to finish running
          long fin = System.currentTimeMillis() + 60 * 1000;
          for (;;) {
            if (peskyMemory == null) {
              break;
            }
            if (System.currentTimeMillis() > fin) {
              throw whoops;
            }
            synchronized (SystemFailureDUnitTest.class) {
              try {
                SystemFailureDUnitTest.class.wait(2000);
              }
              catch (InterruptedException e2) {
                fail("interrupted");
              }
            }
          }
        }
      }
      public void afterCreate(EntryEvent event) {
        com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("Invoking afterCreate on listener; name=" +
                            event.getKey());
        forceOutOfMemory();
      }
  };
  
  static private final GenericListener listener_memoryMonitor = new GenericListener() {
    
    /**
     * Allocate objects until we are chronically low, but don't generate
     * OutOfMemoryError
     */
    private void forceLowMemory() {
      long maxMem = Runtime.getRuntime().maxMemory();
      long avail = Runtime.getRuntime().freeMemory();
      long thresh = (long)(avail * 0.40);
      long ferSure = (long)(avail * 0.30);
      SystemFailure.setFailureMemoryThreshold(thresh);
      SystemFailure.setFailureAction(new Runnable() { 
        public void run() {
          peskyMemory = null;
          System.gc();
          synchronized (SystemFailure.class) {
            SystemFailure.class.notify();
          }
        }
      });
      
      peskyMemory = new ArrayList();
      // Allocate this _before_ exhausting memory :-)
      final TestException whoops = new TestException("Timeout!");
      
      // Fill up a lot of memory
      for (;;) {
        peskyMemory.add(new long[100000]);
        if (Runtime.getRuntime().totalMemory() < maxMem) {
          continue; // haven't finished allocating max allowed
        }
        if (Runtime.getRuntime().freeMemory() < ferSure) {
          break;
        }
      } // for
      
      
      // Wait for the failure monitor to kick in
      long fin = System.currentTimeMillis() +
          (long)(SystemFailure.MEMORY_MAX_WAIT * 1.5 * 1000);
      for (;;) {
        long now = System.currentTimeMillis();
        if (now > fin) {
          throw whoops;
        }
        synchronized (SystemFailure.class) {
          try {
            if (peskyMemory == null) {
              break;
            }
            SystemFailure.class.wait(fin - now);
          }
          catch (InterruptedException e) {
            fail("interrupted");
          }
        } // synchronized
        if (peskyMemory == null) {
          break;
        }
      } // for
    }

    public void afterCreate(EntryEvent event) {
      com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("Invoking afterCreate on listener; name=" +
                          event.getKey());
      forceLowMemory();
    }
  };

  static private final GenericListener listener_internalError = new GenericListener() {
    /**
     * not really any good way to convince Java to do
     * this, so I'm just gonna throw it directly.
     */
    private void forceInternalError() {
      throw new InternalError("gotcha");
    }
    
    public void afterCreate(EntryEvent event) {
      com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("Invoking afterCreate on listener; name=" +
                          event.getKey());
      forceInternalError();
    }
  };
  
  static private final GenericListener listener_unknownError = new GenericListener() {
    
    /**
     * Not actually used in current JRE?
     */
    private void forceInternalError() {
      throw new UnknownError("gotcha");
    }
    
    public void afterCreate(EntryEvent event) {
      com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("Invoking afterCreate on listener; name=" +
                          event.getKey());
      forceInternalError();
    }
  };
  
  static private final GenericListener listener_error = new GenericListener() {
    private void forceError() {
      new SickoClass();
    }
    
    public void afterCreate(EntryEvent event) {
      com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter().info("Invoking afterCreate on listener; name=" +
                          event.getKey());
      forceError();
    }
  };
  
  /**
   * Set a listener that generates some sort of error
   * 
   * @param which makes it test dependent
   * @return the listener
   */
  static private GenericListener getListener(String which) {
    GenericListener listener;
    if (which.equals("testStackOverflow")) {
      listener = listener_stackOverflow;
    }
    else
    if (which.equals("testOutOfMemory")) {
      listener = listener_outOfMemory;
    }
    else
    if (which.equals("testPersistentOutOfMemory")) {
      listener = listener_persistentOutOfMemory;
    }
    else
    if (which.equals("testMemoryMonitor")) {
      listener = listener_memoryMonitor;
    }
    else
    if (which.equals("testListener")) {
      listener = listener_internalError;
    }
    else
    if (which.equals("testInternalError")) {
      listener = listener_internalError;
    }
    else
    if (which.equals("testUnknownError")) {
      listener = listener_unknownError;
    }
    else
    if (which.equals("testError")) {
      listener = listener_error;
    }
    else {
      throw new TestException("don't know which listener: " + which);
    }
    return listener;
  }
  
  protected void doCreateEntry(String name) {
    LogWriter log = com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter();
    log.info(
        "<ExpectedException action=add>" +
        "dunit.RMIException"
        + "</ExpectedException>");

    Object[] args = new Object[] { name, };
    Host host = Host.getHost(0);
    VM vm = host.getVM(0);
    try {
      vm.invoke(this.getClass(), "createEntry", args);
    }
    catch (RMIException e) {
      // expected
    }

    log.info(
        "<ExpectedException action=add>" +
        "dunit.RMIException"
        + "</ExpectedException>");
  }
  
  /**
   * Sets a listener based on the test, and then (attempts to) create 
   * an entry in this test's region with the given name
   * 
   * @param name the test we are running
   */
  protected static void createEntry(String name) 
  throws CacheException {
    GenericListener l = getListener(name);
    createEntry(name, 0, ExpirationAction.INVALIDATE, l);
  }
  
  /**
   * Gets or creates a region used in this test
   */
  private static Region getRegion() 
    throws CacheException {

    Region root = getRootRegion();
    Region region = root.getSubregion(REGION_NAME);;
    if (region == null) {
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(SCOPE);
      region = root.createSubregion(REGION_NAME,
                                    factory.create());
    }
    
    return region;
  }
  
  /**
   * A class that provides default implementations for the methods of
   * several listener types.
   */
  public static class GenericListener
    extends CacheListenerAdapter implements Serializable {

    ////////  CacheListener  ///////

    public void close() { }
    
    /**
     * is called when an object is newly loaded into cache.
     * @param oevt the ObjectEvent object representing the source object
     * of the event.
     */
    public void afterCreate(EntryEvent oevt) {
      fail("Unexpected listener callback: afterCreate");
    }
  
    /**
     * is called when an object is invalidated.
     * @param oevt the ObjectEvent object representing the source object
     * of the event.
     */
    public void afterInvalidate(EntryEvent oevt) {
      fail("Unexpected listener callback: afterInvalidated");
    }
  
    /**
     * is called when an object is destroyed.
     * @param oevt the ObjectEvent object representing the source object
     * of the event.
     */
    public void afterDestroy(EntryEvent oevt) {
      fail("Unexpected listener callback: afterDestroy");
    }
  
    /**
     * is called when an object is replaced.
     * @param oevt the ObjectEvent object representing the source object
     * of the event.
     */ 
    public void afterUpdate(EntryEvent oevt) {
      fail("Unexpected listener callback: afterUpdate");
    }
    
    /**
     * is called when a region is invalidated.
     * @param revt a RegionEvent to represent the source region.
     * @throws CacheException if any error occurs.
     */
    public void afterRegionInvalidate(RegionEvent revt) {
      fail("Unexpected listener callback: afterRegionInvalidate");
    }
  
    /**
     * is called when a region is destroyed.
     * @param revt a RegionEvent to represent the source region.
     * @throws CacheException if any error occurs.
     */
    public void afterRegionDestroy(RegionEvent revt) {
//       fail("Unexpected listener callback: afterRegionDestroy");
    }    
  }
  
}
