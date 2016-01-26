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

import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionManagerDUnitTest;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * This is the abstract superclass of tests that validate the
 * functionality of GemFire's distributed caches.  It provides a
 * number of convenient helper classes.
 */
public abstract class DistributedCacheTestCase
  extends DistributedTestCase {

  /** The current cache in this VM */
  protected static Cache cache = null;

  ///////////////////////  Constructors  ///////////////////////

  public DistributedCacheTestCase(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    setUp(true);
  }
  /**
   * Creates the {@link Cache} and root region in each remote VM
   * and, if createLocalCache, in this VM.
   */
  protected void setUp(boolean createLocalCache) throws Exception {
    super.setUp();
    if (createLocalCache) {
      try {
        remoteCreateCache();
        assertTrue(cache != null);

      } catch (Exception ex) {
        String s = "While creating cache in this VM";
        throw new InternalGemFireException(s, ex);
      }
    } else {
      this.getSystem(); // make sure we have a connected DistributedSystem
    }

    for (int h = 0; h < Host.getHostCount(); h++) {
      Host host = Host.getHost(h);

      for (int v = 0; v < host.getVMCount(); v++) {
        VM vm = host.getVM(v);
        vm.invoke(this.getClass(), "remoteCreateCache",
                  new Object[] {});
      }
    }
  }

  /** 
   * Creates the root region in a remote VM
   */
  private static void remoteCreateCache() 
    throws Exception {

    Assert.assertTrue(cache == null, "cache should be null");

    DistributedCacheTestCase x = new DistributedCacheTestCase("Lame") { };
    cache = CacheFactory.create(x.getSystem());
    
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_NO_ACK);
    cache.createRegion("root", factory.create());
  }

  /**
   * Closes the cache in this VM and each remote VM
   */
  public void tearDown2() throws Exception {
    StringBuffer problems = new StringBuffer();

    if (cache != null) {
      try {
        if (remoteCloseCache()) {
          problems.append("An exception occurred trying to close the cache.");
        }

        assertTrue(cache == null);

      } catch (Exception ex) {
        String s = "While closing the cache in this VM";
        throw new InternalGemFireException(s, ex);
      }
    }

    for (int h = 0; h < Host.getHostCount(); h++) {
      Host host = Host.getHost(h);

      for (int v = 0; v < host.getVMCount(); v++) {
        VM vm = host.getVM(v);
        boolean exceptionInThreads = 
          vm.invokeBoolean(this.getClass(), "remoteCloseCache");
        if (exceptionInThreads) {
          String s = "An exception occurred in GemFire system";
          problems.append(s);
        }
      }
    }

    assertEquals("Problems while tearing down", 
                 "", problems.toString().trim());

    super.tearDown2();
  }

  /**
   * Closes the Cache for the current VM.  Returns whether or not an
   * exception occurred in the distribution manager to which this VM
   * is attached.  Note that the exception flag is cleared by this
   * method.
   *
   * @see DistributionManager#exceptionInThreads()
   */
  private static boolean remoteCloseCache() throws CacheException {
    Assert.assertTrue(cache != null, "No cache on this VM?");
    Assert.assertTrue(!cache.isClosed(), "Who closed my cache?");

    InternalDistributedSystem system = (InternalDistributedSystem)
      ((GemFireCacheImpl) cache).getDistributedSystem();
    DistributionManager dm = (DistributionManager)system.getDistributionManager();
    boolean exceptionInThreads = dm.exceptionInThreads();
    DistributionManagerDUnitTest.clearExceptionInThreads(dm);

    cache.close();
    cache = null;

    return exceptionInThreads;
  }

  //////////////////////  Helper Methods  //////////////////////

  /**
   * Returns the root region of the cache.  We assume that the {@link
   * Cache} and the root region have already been created.
   */
  protected static Region getRootRegion() throws CacheException {
    if (cache == null) {
      String s = "Cache not created yet!";
      throw new IllegalStateException(s);
    }

    return cache.getRegion("root");
  }

  /**
   * Return the distribution manager associate with the cache
   *
   * @since 2.1
   */
  protected static DistributionManager getDistributionManager() {
    if (cache == null) {
      String s = "Cache not created yet!";
      throw new IllegalStateException(s);
    }

    InternalDistributedSystem system = (InternalDistributedSystem)
      ((GemFireCacheImpl) cache).getDistributedSystem();
    return (DistributionManager)system.getDistributionManager();
  }

  /**
   * Creates a new sub-Region of the root Region in a remote VM with
   * default scope, SCOPE_LOCAL.
   *
   * @param name
   *        The name of the newly-created sub-Region.  It is
   *        recommended that the name of the Region be the {@link
   *        #getUniqueName()} of the test.
   */
  protected static void remoteCreateRegion(String name)
    throws CacheException {
    
    remoteCreateRegion(name, Scope.LOCAL);
  }

  /**
   * Creates a new sub-Region of the root Region in a remote VM.
   *
   * @param name
   *        The name of the newly-created sub-Region.  It is
   *        recommended that the name of the Region be the {@link
   *        #getUniqueName()} of the test.
   * @param scope create the region attributes with this scope
   */
  protected static void remoteCreateRegion(String name, Scope scope)
    throws CacheException {

    Region root = getRootRegion();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(scope);

    Region newRegion =
      root.createSubregion(name, factory.create());
    getLogWriter().info(
      "Created Region '" + newRegion.getFullPath() + "'");
  }

  /**
   * Defines an entry in the Region with the given
   * name and scope. 
   *
   * @param regionName
   *        The name of a region that is a sub-region of the root
   *        region, or a global name 
   * @param entryName
   *        Must be {@link java.io.Serializable}
   */
  protected static void remoteDefineEntry(String regionName,
                                          String entryName,
                                          Scope scope)
    throws CacheException {

    remoteDefineEntry(regionName, entryName, scope, true);
  }


  
  
  /**
   * Defines an entry in the Region with the given name and scope.  In
   * 3.0 this method create a subregion named <code>entryName</code>
   * (with the appropriate attributes) that contains an entry named
   * <code>entryName</code>.
   *
   * @param regionName
   *        The name of a region that is a sub-region of the root
   *        region, or a global name 
   * @param entryName
   *        Must be {@link java.io.Serializable}
   * @param doNetSearch
   *        Will the distributed region perform a netSearch when
   *        looking for objects?  If <code>false</code> a {@link
   *        CacheException} will be thrown if an entry in the region
   *        is asked for and it not there.
   */
  protected static void remoteDefineEntry(String regionName,
                                          String entryName,
                                          Scope scope,
                                          boolean doNetSearch)
    throws CacheException {

    Region root = getRootRegion();
    Region region = root.getSubregion(regionName);

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(scope);

    if (!doNetSearch) {
      factory.setCacheLoader(new CacheLoader() {
          public Object load(LoaderHelper helper)
            throws CacheLoaderException {
            String s = "Should not be loading \"" + helper.getKey() +
              "\" in \"" + helper.getRegion().getFullPath() + "\"";
            throw new CacheLoaderException(s);
          }

          public void close() { }
        });
    }

    Region sub =
      region.createSubregion(entryName,
                             factory.create());
    sub.create(entryName, null);

    getLogWriter().info(
      "Defined Entry named '" + entryName + "' in region '" +
      sub.getFullPath() +"'");
  }
  

  /**
   * Puts (or creates) a value in a subregion of <code>region</code>
   * named <code>entryName</code>.
   */
  protected static void remotePut(String regionName,
                                  String entryName,
                                  Object value, Scope scope)
   throws CacheException {

    Region root = getRootRegion();
    Region region = root.getSubregion(regionName);
    Region sub = region.getSubregion(entryName);
    if (sub == null) {
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(scope);
      sub = region.createSubregion(entryName,
                                   factory.create());
    }

    sub.put(entryName, value);

    getLogWriter().info(
      "Put value " + value + " in entry " + entryName + " in region '" +
      region.getFullPath() +"'");
  }

  /**
   * Does a put with the given value, defining a DISTRIBUTED_NO_ACK entry
   * in the Region with the given name.
   *
   * @param regionName
   *        The name of a region that is a sub-region of the root
   *        region, or a global name 
   * @param entryName
   *        Must be {@link java.io.Serializable}
   */
   protected static void remotePutDistributed(String regionName,
                                              String entryName,
                                              Object value)
   throws CacheException {
     remotePut(regionName, entryName, value, Scope.DISTRIBUTED_NO_ACK);
   }
                                  
  
  /**
   * Replaces the value of an entry in a region in a remote VM
   *
   * @param regionName
   *        The name of a region that is a sub-region of the root
   *        region 
   * @param entryName
   *        Must be {@link java.io.Serializable}
   * @param value
   *        The value used to replace
   *
   * @see Region#put()
   */
  protected static void remoteReplace(String regionName,
                                      String entryName,
                                      Object value)
    throws CacheException {

    Region root = getRootRegion();
    Region region = root.getSubregion(regionName);
    Region sub = region.getSubregion(entryName);
    if (sub == null) {
      String s = "Entry \"" + entryName + "\" does not exist";
      throw new EntryNotFoundException(s);
    }

    sub.put(entryName, value);

    getLogWriter().info(
      "Replaced value " + value + "in entry " + entryName + " in region '" +
      region.getFullPath() +"'");
  }

  /**
   * Invalidates the value of an entry in a region in a remote VM
   *
   * @param regionName
   *        The name of a region that is a sub-region of the root
   *        region 
   * @param entryName
   *        Must be {@link java.io.Serializable}
   *
   * @see Region#replace()
   */
  protected static void remoteInvalidate(String regionName,
                                         String entryName)
    throws CacheException {

    Region root = getRootRegion();
    Region region = root.getSubregion(regionName);
    Region sub = region.getSubregion(entryName);
    if (sub == null) {
      String s = "Entry \"" + entryName + "\" does not exist";
      throw new EntryNotFoundException(s);
    }

    sub.invalidate(entryName);
  }

  /**
   * Destroys the value of an entry in a region in a remote VM
   *
   * @param regionName
   *        The name of a region that is a sub-region of the root
   *        region 
   * @param entryName
   *        Must be {@link java.io.Serializable}
   *
   * @see Region#replace()
   */
  protected static void remoteDestroy(String regionName,
                                      String entryName)
    throws CacheException {

    Region root = getRootRegion();
    Region region = root.getSubregion(regionName);
    Region sub = region.getSubregion(entryName);
    if (sub == null) {
      String s = "Entry \"" + entryName + "\" does not exist";
      throw new EntryNotFoundException(s);
    }

    assertNotNull(sub.getEntry(entryName));
    sub.destroy(entryName);
    assertNull(sub.getEntry(entryName));
  }

  /**
   * Asserts that the value of an entry in a region is what we expect
   * it to be.
   *
   * @param regionName
   *        The name of a region that is a sub-region of the root
   *        region 
   * @param entryName
   *        Must be {@link java.io.Serializable}
   */
  protected static void remoteAssertEntryValue(String regionName,
                                               String entryName, 
                                               Object expected) 
    throws CacheException {
    
    Region root = getRootRegion();
    Region region = root.getSubregion(regionName);
    Region sub = region.getSubregion(entryName);
    if (sub == null) {
      String s = "Entry \"" + entryName + "\" does not exist";
      throw new EntryNotFoundException(s);
    }

    assertEquals(expected, sub.get(entryName));
  }

  /**
   * Assumes there is only one host, and invokes the given method in
   * every VM that host knows about.
   */
  public void forEachVMInvoke(String methodName, Object[] args) {
    Host host = Host.getHost(0);
    int vmCount = host.getVMCount();
    for (int i=0; i<vmCount; i++) {
      getLogWriter().info("Invoking " + methodName + "on VM#" + i);
      host.getVM(i).invoke(this.getClass(), methodName, args);
    }
  }
}
