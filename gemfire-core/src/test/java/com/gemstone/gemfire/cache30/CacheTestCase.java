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
package com.gemstone.gemfire.cache30;

import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheExistsException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessageObserver;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.FileUtil;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.HARegion;
import com.gemstone.gemfire.internal.cache.InternalRegionArguments;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

/**
 * The abstract superclass of tests that require the creation of a
 * {@link Cache}.
 *
 * @author David Whitlock
 * @since 3.0
 */
public abstract class CacheTestCase extends DistributedTestCase {
  private static final Logger logger = LogService.getLogger();

  /** The Cache from which regions are obtained 
   * 
   * All references synchronized via <code>CacheTestCase.class</code>
   * */
  // static so it doesn't get serialized with SerializableRunnable inner classes
  protected static Cache cache;
  
  ////////  Constructors

  public CacheTestCase(String name) {
    super(name);
  }

  ////////  Helper methods
  /**
   * Creates the <code>Cache</code> for this test
   */
  private void createCache() {
    createCache(false);
  }
  
  private void createCache(boolean client) {
    createCache(client, null);
  }
  
  private void createCache(boolean client, CacheFactory cf) {
    synchronized(CacheTestCase.class) {
      try {
        System.setProperty("gemfire.DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE", "true");
        Cache c;
        if (client) {
          c = (Cache)new ClientCacheFactory(getSystem().getProperties()).create();
        } else {
          if(cf == null) {
            c = CacheFactory.create(getSystem());
          } else {
            Properties props = getSystem().getProperties();
            for(Map.Entry entry : props.entrySet()) {
              cf.set((String) entry.getKey(), (String)entry.getValue());
            }
            c = cf.create();
          }
        }
        cache = c;
      } catch (CacheExistsException e) {
        Assert.fail("the cache already exists", e);

      } catch (RuntimeException ex) {
        throw ex;

      } catch (Exception ex) {
        Assert.fail("Checked exception while initializing cache??", ex);
      } finally {
        System.clearProperty("gemfire.DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE");
      }
    }
  }

  /**
   * Creates the <code>Cache</code> for this test that is not connected
   * to other members
   */
  public Cache createLonerCache() {
    synchronized(CacheTestCase.class) {
      try {
        System.setProperty("gemfire.DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE", "true");
        Cache c = CacheFactory.create(getLonerSystem()); 
        cache = c;
      } catch (CacheExistsException e) {
        Assert.fail("the cache already exists", e);

      } catch (RuntimeException ex) {
        throw ex;

      } catch (Exception ex) {
        Assert.fail("Checked exception while initializing cache??", ex);
      } finally {
        System.clearProperty("gemfire.DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE");
      }
      return cache;
    }
  }
  
  /**
   * Creates the <code>Cache</code> for this test that is not connected
   * to other members.
   * Added specifically to test scenario of defect #47181.
   */
  public Cache createLonerCacheWithEnforceUniqueHost() {
    synchronized(CacheTestCase.class) {
      try {
        System.setProperty("gemfire.DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE", "true");
        Cache c = CacheFactory.create(getLonerSystemWithEnforceUniqueHost()); 
        cache = c;
      } catch (CacheExistsException e) {
        Assert.fail("the cache already exists", e);

      } catch (RuntimeException ex) {
        throw ex;

      } catch (Exception ex) {
        Assert.fail("Checked exception while initializing cache??", ex);
      } finally {
        System.clearProperty("gemfire.DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE");
      }
      return cache;
    }
  }

  /**
   * Sets this test up with a CacheCreation as its cache.
   * Any existing cache is closed. Whoever calls this must also call finishCacheXml
   */
  public static synchronized void beginCacheXml() {
//    getLogWriter().info("before closeCache");
    closeCache();
//    getLogWriter().info("before TestCacheCreation");
    cache = new TestCacheCreation();
//    getLogWriter().info("after TestCacheCreation");
  }
  /**
   * Finish what beginCacheXml started. It does this be generating a cache.xml
   * file and then creating a real cache using that cache.xml.
   */
  public void finishCacheXml(String name) {
    synchronized(CacheTestCase.class) {
      File file = new File(name + "-cache.xml");
      try {
        PrintWriter pw = new PrintWriter(new FileWriter(file), true);
        CacheXmlGenerator.generate(cache, pw);
        pw.close();
      } catch (IOException ex) {
        Assert.fail("IOException during cache.xml generation to " + file, ex);
      }
      cache = null;
      GemFireCacheImpl.testCacheXml = file;
      try {
        createCache();
      } finally {
        GemFireCacheImpl.testCacheXml = null;
      }
    }
  }
  
  /**
   * Finish what beginCacheXml started. It does this be generating a cache.xml
   * file and then creating a real cache using that cache.xml.
   */
  public void finishCacheXml(String name, boolean useSchema, String xmlVersion) {
    synchronized(CacheTestCase.class) {
      File dir = new File("XML_" + xmlVersion);
      dir.mkdirs();
      File file = new File(dir, name + ".xml");
      try {
        PrintWriter pw = new PrintWriter(new FileWriter(file), true);
        CacheXmlGenerator.generate(cache, pw, useSchema, xmlVersion);
        pw.close();
      } catch (IOException ex) {
        Assert.fail("IOException during cache.xml generation to " + file, ex);
      }
      cache = null;
      GemFireCacheImpl.testCacheXml = file;
      try {
        createCache();
      } finally {
        GemFireCacheImpl.testCacheXml = null;
      }
    }
  }

  /**
   * Return a cache for obtaining regions, created lazily.
   */
  public final Cache getCache() {
    return getCache(false);
  }
  
  public final Cache getCache(CacheFactory cf) {
    return getCache(false, cf);
  }
  
  public final Cache getCache(boolean client) {
    return getCache(client, null);
  }

  public final Cache getCache(boolean client, CacheFactory cf) {
    synchronized (CacheTestCase.class) {
      final GemFireCacheImpl gfCache = GemFireCacheImpl.getInstance();
      if (gfCache != null && !gfCache.isClosed()
          && gfCache.getCancelCriterion().cancelInProgress() != null) {
        Wait.waitForCriterion(new WaitCriterion() {

          public boolean done() {
            return gfCache.isClosed();
          }

          public String description() {
            return "waiting for cache to close";
          }
        }, 30 * 1000, 300, true);
      }
      if (cache == null || cache.isClosed()) {
        cache = null;
        createCache(client, cf);
      }
      if (client && cache != null) {
        IgnoredException.addIgnoredException("java.net.ConnectException");
      }
      return cache;
    }
  }

  /**
   * creates a client cache from the factory if one does not already exist
   * @since 6.5
   * @param factory
   * @return the client cache
   */
  public final ClientCache getClientCache(ClientCacheFactory factory) {
    synchronized (CacheTestCase.class) {
      final GemFireCacheImpl gfCache = GemFireCacheImpl.getInstance();
      if (gfCache != null && !gfCache.isClosed()
          && gfCache.getCancelCriterion().cancelInProgress() != null) {
        Wait.waitForCriterion(new WaitCriterion() {

          public boolean done() {
            return gfCache.isClosed();
          }

          public String description() {
            return "waiting for cache to close";
          }
        }, 30 * 1000, 300, true);
      }
      if (cache == null || cache.isClosed()) {
        cache = null;
        disconnectFromDS();
        cache = (Cache)factory.create();
      }
      if (cache != null) {
        IgnoredException.addIgnoredException("java.net.ConnectException");
      }
      return (ClientCache)cache;
    }
  }

  /**
   * same as {@link #getCache()} but with casting
   */
  public final GemFireCacheImpl getGemfireCache() {
    return (GemFireCacheImpl)getCache();
  }
  public static synchronized final boolean hasCache() {
      return cache != null;
  }

  /**
   * Return current cache without creating one.
   */
  public static synchronized final Cache basicGetCache() {
      return cache;
  }

  public static synchronized void disconnectFromDS() {
    closeCache();
    DistributedTestCase.disconnectFromDS();
  }
  
  /** Close the cache */
  public static synchronized void closeCache() {
    //Workaround for that fact that some classes are now extending
    //CacheTestCase but not using it properly.
    if(cache == null) {
      cache = GemFireCacheImpl.getInstance();
    }
    try {
    if (cache != null) {
      try {
        if (!cache.isClosed()) {
          if (cache instanceof GemFireCacheImpl) {
            CacheTransactionManager txMgr = ((GemFireCacheImpl)cache).getTxManager();
            if (txMgr != null) {
              if (txMgr.exists()) {
                try {
                  // make sure we cleanup this threads txid stored in a thread local
                  txMgr.rollback();
                }catch(Exception ignore) {
                  
                }
              }
            }
          }
          cache.close();
        }
      }
      finally {
        cache = null;
      }
    } // cache != null
    } finally {
      //Make sure all pools are closed, even if we never
      //created a cache
      PoolManager.close(false);
    }
  }

  /** Closed the cache in all VMs. */
  protected void closeAllCache() {
    closeCache();
    Invoke.invokeInEveryVM(CacheTestCase.class, "closeCache");
  }

  @Override
  protected final void preTearDown() throws Exception {
    preTearDownCacheTestCase();
    
    // locally destroy all root regions and close the cache
    remoteTearDown();
    // Now invoke it in every VM
    for (int h = 0; h < Host.getHostCount(); h++) {
      Host host = Host.getHost(h);
      for (int v = 0; v < host.getVMCount(); v++) {
        VM vm = host.getVM(v);
        vm.invoke(()->remoteTearDown());
      }
    }
    
    postTearDownCacheTestCase();
  }
  
  protected void preTearDownCacheTestCase() throws Exception {
  }

  protected void postTearDownCacheTestCase() throws Exception {
  }

  /**
   * Local destroy all root regions and close the cache.  
   */
  protected synchronized static void remoteTearDown() {
    try {
      DistributionMessageObserver.setInstance(null);
      destroyRegions(cache);
    }
    finally {
      try {
        closeCache();
      }
      finally {
        try {
          cleanDiskDirs();
        } catch(Exception e) {
          LogWriterUtils.getLogWriter().error("Error cleaning disk dirs", e);
        }
      }
    }
  }

  /**
   * Returns a region with the given name and attributes
   */
  public final Region createRegion(String name,
                                      RegionAttributes attrs)
    throws CacheException {
    return createRegion(name, "root", attrs);
  }
  
  /**
   * Provide any internal region arguments, typically required when 
   * internal use (aka meta-data) regions are needed.
   * @return internal arguements, which may be null.  If null, then default 
   * InternalRegionArguments are used to construct the Region
   */
  public InternalRegionArguments getInternalRegionArguments()
  {
    return null;
  }

  final public Region createRegion(String name, String rootName,
                                      RegionAttributes attrs)
    throws CacheException {
    Region root = getRootRegion(rootName);
    if (root == null) {
      // don't put listeners on root region
      RegionAttributes rootAttrs = attrs;
      AttributesFactory fac = new AttributesFactory(attrs);
      ExpirationAttributes expiration = ExpirationAttributes.DEFAULT;

      // fac.setCacheListener(null);
      fac.setCacheLoader(null);
      fac.setCacheWriter(null);
      fac.setPoolName(null);
      fac.setPartitionAttributes(null);
      fac.setRegionTimeToLive(expiration);
      fac.setEntryTimeToLive(expiration);
      fac.setRegionIdleTimeout(expiration);
      fac.setEntryIdleTimeout(expiration);
      rootAttrs = fac.create();
      root = createRootRegion(rootName, rootAttrs);
    }

    InternalRegionArguments internalArgs = getInternalRegionArguments();
    if (internalArgs == null) {
      return root.createSubregion(name, attrs);
    } else {
      try {
        LocalRegion lr = (LocalRegion) root;
        return lr.createSubregion(name, attrs, internalArgs);
      } catch (IOException ioe) {
        AssertionError assErr = new AssertionError("unexpected exception");
        assErr.initCause(ioe);
        throw assErr;
      } catch (ClassNotFoundException cnfe) {
        AssertionError assErr = new AssertionError("unexpected exception");
        assErr.initCause(cnfe);
        throw assErr;
      } 
    }
  }
  
  public final Region getRootRegion() {
    return getRootRegion("root");
  }
  
  public final Region getRootRegion(String rootName) {
    return getCache().getRegion(rootName);
  }

  protected final Region createRootRegion(RegionAttributes attrs)
  throws RegionExistsException, TimeoutException {
    return createRootRegion("root", attrs);
  }

  public final Region createRootRegion(String rootName, RegionAttributes attrs)
  throws RegionExistsException, TimeoutException {
    return getCache().createRegion(rootName, attrs);
  }
  public final Region createExpiryRootRegion(String rootName, RegionAttributes attrs)
  throws RegionExistsException, TimeoutException {
    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      return createRootRegion(rootName, attrs);
    } finally {
      System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
    }
  }


  /**
   * send an unordered message requiring an ack to all connected members 
   * in order to flush the unordered communication channel
   */
  public void sendUnorderedMessageToAll() {
    //if (getCache() instanceof distcache.gemfire.GemFireCacheImpl) {
      try {
        com.gemstone.gemfire.distributed.internal.HighPriorityAckedMessage msg = new com.gemstone.gemfire.distributed.internal.HighPriorityAckedMessage();
        msg.send(InternalDistributedSystem.getConnectedInstance().getDM().getNormalDistributionManagerIds(), false);
      }
      catch (Exception e) {
        throw new RuntimeException("Unable to send unordered message due to exception", e);
      }
    //}
  }

  /**
   * send an unordered message requiring an ack to all connected admin members 
   * in order to flush the unordered communication channel
   */
//  public void sendUnorderedMessageToAdminMembers() {
//    //if (getCache() instanceof distcache.gemfire.GemFireCacheImpl) {
//      try {
//        com.gemstone.gemfire.distributed.internal.HighPriorityAckedMessage msg = new com.gemstone.gemfire.distributed.internal.HighPriorityAckedMessage();
//        msg.send(DistributedSystemHelper.getAdminMembers(), false);
//      }
//      catch (Exception e) {
//        throw new RuntimeException("Unable to send unordered message due to exception", e);
//      }
//    //}
//  }

  /**
   * send an ordered message requiring an ack to all connected members 
   * in order to flush the ordered communication channel
   */
  public void sendSerialMessageToAll() {
    if (getCache() instanceof GemFireCacheImpl) {
      try {
        com.gemstone.gemfire.distributed.internal.SerialAckedMessage msg = new com.gemstone.gemfire.distributed.internal.SerialAckedMessage();
        msg.send(InternalDistributedSystem.getConnectedInstance().getDM().getNormalDistributionManagerIds(), false);
      }
      catch (Exception e) {
        throw new RuntimeException("Unable to send serial message due to exception", e);
      }
    }
  }

  /**
   * @deprecated Use DistributedTestCase.addExpectedException
   */
  @Deprecated
  protected CacheSerializableRunnable addExceptionTag1(final String expectedException) {
    CacheSerializableRunnable addExceptionTag = new CacheSerializableRunnable(
    "addExceptionTag") {
      public void run2()
      {
        getCache().getLogger().info(
            "<ExpectedException action=add>" + expectedException
            + "</ExpectedException>");
      }
    };
    
    return addExceptionTag;
  }

  /**
   * @deprecated Use DistributedTestCase.addExpectedException
   */
  @Deprecated
  protected CacheSerializableRunnable removeExceptionTag1(final String expectedException) {
    CacheSerializableRunnable removeExceptionTag = new CacheSerializableRunnable(
    "removeExceptionTag") {
      public void run2() throws CacheException {
        getCache().getLogger().info(
            "<ExpectedException action=remove>" + expectedException
            + "</ExpectedException>");
      }
    };
    return removeExceptionTag;
  }

  /**
   * Used to generate a cache.xml. Basically just a CacheCreation
   * with a few more methods implemented.
   */
  static class TestCacheCreation extends CacheCreation {
    private boolean closed = false;
    @Override
    public void close() {
      this.closed = true;
    }
    @Override
    public boolean isClosed() {
      return this.closed;
    }
  }
  
  public static File getDiskDir() {
    int vmNum = VM.getCurrentVMNum();
    File dir = new File("diskDir", "disk" + String.valueOf(vmNum)).getAbsoluteFile();
    dir.mkdirs();
    return dir;
  }
  
  /**
   * Return a set of disk directories
   * for persistence tests. These directories
   * will be automatically cleaned up 
   * on test case closure.
   */
  public static File[] getDiskDirs() {
    return new File[] {getDiskDir()};
  }
  
  public static void cleanDiskDirs() throws IOException {
    FileUtil.delete(getDiskDir());
    File[] defaultStoreFiles = new File(".").listFiles(new FilenameFilter() {
      
      public boolean accept(File dir, String name) {
        return name.startsWith("BACKUPDiskStore-" + System.getProperty("vmid"));
      }
    });
    
    for(File file: defaultStoreFiles) {
      FileUtil.delete(file);
    }
  }
}
