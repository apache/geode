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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskAccessException;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.persistence.UninterruptibleFileChannel;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Tests that if a node doing GII experiences DiskAccessException, it should
 * also not try to recover from the disk
 * @author Asif
 *
 */
public class Bug39079DUnitTest extends CacheTestCase {

  protected static String regionName = "IGNORE_EXCEPTION_Bug39079";

  static Properties props = new Properties();
  

  private static VM vm0 = null;

  private static VM vm1 = null;

  private static  String REGION_NAME = "IGNORE_EXCEPTION_testBridgeServerStoppingInSynchPersistOnlyForIOExceptionCase";  
  
  private static Cache gemfirecache = null;

  private static Region region;
  
  protected static File[] dirs = null;

  private static final int maxEntries = 10000;

  /**
   * Constructor
   * 
   * @param name
   */
  public Bug39079DUnitTest(String name) {
    super(name);
    File file1 = new File(name + "1");
    file1.mkdir();
    file1.deleteOnExit();
    File file2 = new File(name + "2");
    file2.mkdir();
    file2.deleteOnExit();
    dirs = new File[2];
    dirs[0] = file1;
    dirs[1] = file2;
  }

  public void setUp() throws Exception {
    super.setUp();
    final Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);

    vm0.invoke(Bug39079DUnitTest.class, "ignorePreAllocate", new Object[] { Boolean.TRUE });
    vm1.invoke(Bug39079DUnitTest.class, "ignorePreAllocate", new Object[] { Boolean.TRUE });
  }
 

  /**
   * This method is used to create Cache in VM0
   * 
   * @return CacheSerializableRunnable
   */

  private CacheSerializableRunnable createCacheForVM0() {
    SerializableRunnable createCache = new CacheSerializableRunnable(
        "createCache") {
      public void run2() {
        try {

          (new Bug39079DUnitTest("vm0_diskReg"))
              .getSystem();         
          
          assertTrue(getCache() != null);
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);         
          factory.setDiskSynchronous(false);
          factory.setDiskStoreName(getCache().createDiskStoreFactory()
                                   .setDiskDirs(dirs)
                                   .create("Bug39079DUnitTest")
                                   .getName());
          RegionAttributes attr = factory.create();
          getCache().createRegion(regionName, attr);
        }
        catch (Exception ex) {
          ex.printStackTrace();
          fail("Error Creating cache / region ");
        }
      }
    };
    return (CacheSerializableRunnable)createCache;
  }

  /**
   * This method is used to create Cache in VM1
   * 
   * @return CacheSerializableRunnable
   */
  private CacheSerializableRunnable createCacheForVM1() {
    SerializableRunnable createCache = new CacheSerializableRunnable(
        "createCache") {
      public void run2() {
        try {
          (new Bug39079DUnitTest("vm1_diskReg"))
              .getSystem();
          
          
          assertTrue("cache found null", getCache() != null);

          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);          
          factory.setDiskSynchronous(false);
          factory.setDiskStoreName(getCache().createDiskStoreFactory()
                                   .setDiskDirs(dirs)
                                   .create("Bug39079DUnitTest")
                                   .getName());
          RegionAttributes attr = factory.create();
          getCache().createRegion(regionName, attr);

        }
        catch (Exception ex) {
          ex.printStackTrace();
          fail("Error Creating cache / region " + ex);
        }
      }
    };
    return (CacheSerializableRunnable)createCache;
  }

  @Override
  protected final void postTearDownCacheTestCase() throws Exception {
    disconnectAllFromDS();

    vm0.invoke(Bug39079DUnitTest.class, "ignorePreAllocate", new Object[] { Boolean.FALSE });
    vm1.invoke(Bug39079DUnitTest.class, "ignorePreAllocate", new Object[] { Boolean.FALSE });
  }
  
  static void ignorePreAllocate(boolean flag) throws Exception {
    DiskStoreImpl.SET_IGNORE_PREALLOCATE = flag;
  }

  
  /**
   * If the node expreriences disk access exception during GII, it should
   * get destroyed & not attempt to recover from the disk
   * 
   */

  public void testGIIDiskAccessException() {

    vm0.invoke(createCacheForVM0());
    vm1.invoke(createCacheForVM1());
    //Create DiskRegion locally in controller VM also
    this.getSystem();
    
    
    assertTrue(getCache() != null);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);    
    factory.setDiskSynchronous(false);
    factory.setDiskStoreName(getCache().createDiskStoreFactory()
                             .setDiskDirs(dirs)
                             .create("Bug39079DUnitTest")
                             .getName());
    RegionAttributes attr = factory.create();
    Region rgn = getCache().createRegion(regionName, attr);
    //Now put entries in the disk region
    for (int i = 0; i < 100; ++i) {
      rgn.put(new Integer(i), new Integer(i));
    }
    //Now close the  region in the controller VM
    rgn.close();

    //Now recreate the region but set the factory such that disk region entry object
    //used is customized by us to throw exception while writing to disk

    DistributedRegion distRegion = new DistributedRegion(regionName, attr,
        null, (GemFireCacheImpl)getCache(), new InternalRegionArguments()
            .setDestroyLockFlag(true).setRecreateFlag(false)
            .setSnapshotInputStream(null).setImageTarget(null));
//    assertTrue("Distributed Region is null", distRegion != null); (cannot be null)

    ((AbstractRegionMap)distRegion.entries)
        .setEntryFactory(Bug39079DUnitTest.TestAbstractDiskRegionEntry.getEntryFactory());
    rgn = null;
    try {
      rgn = ((GemFireCacheImpl)getCache()).createVMRegion(regionName, attr,
          new InternalRegionArguments().setInternalMetaRegion(distRegion)
              .setDestroyLockFlag(true).setSnapshotInputStream(null)
              .setImageTarget(null));
    }
    catch (DiskAccessException dae) {
      //Ok
    }
    catch (Exception e) {
      fail(" test failed because of exception =" + e.toString());
    }

    assertTrue(rgn == null || rgn.isDestroyed());

  }

  static class TestAbstractDiskRegionEntry extends VMThinDiskRegionEntryHeapObjectKey {
    protected TestAbstractDiskRegionEntry(RegionEntryContext r, Object key,
        Object value) {
      super(r, key, value);
    }

    private static RegionEntryFactory factory = new RegionEntryFactory() {
      public final RegionEntry createEntry(RegionEntryContext r, Object key,
          Object value) {

        throw new DiskAccessException(new IOException("Test Exception"));
        //return new Bug39079DUnitTest.TestAbstractDiskRegionEntry(r, key, value);
      }

      public final Class getEntryClass() {

        return Bug39079DUnitTest.TestAbstractDiskRegionEntry.class;
      }

      @Override
      public RegionEntryFactory makeVersioned() {
        return this;
      }
      
      public RegionEntryFactory makeOnHeap() {
        return this;
      }
    };

    /**
     * Overridden setValue method to throw exception
     */
    @Override
    protected void setValueField(Object v) {
      throw new DiskAccessException(new IOException("Test Exception"));
    }

    public static RegionEntryFactory getEntryFactory() {
      return factory;
    }
  }
  
  /**
   * If IOException occurs while updating an entry in an already initialized
   * DiskRegion ,then the bridge servers should be stopped , if any running 
   * 
   * @throws Exception
   */
  public void testBridgeServerStoppingInSynchPersistOnlyForIOExceptionCase()
      throws Exception {    
   // create server cache 
   Integer port = (Integer)vm0.invoke(Bug39079DUnitTest.class, "createServerCache");
   //create cache client
   vm1.invoke(Bug39079DUnitTest.class, "createClientCache",
       new Object[] { NetworkUtils.getServerHostName(vm0.getHost()), port});
   
   // validate 
   vm0.invoke(Bug39079DUnitTest.class, "validateRuningBridgeServerList");
   
   // close server cache
   vm0.invoke(Bug39079DUnitTest.class, "closeCache");
   // close client cache
   vm1.invoke(Bug39079DUnitTest.class, "closeCache");
  }
  
  public static Integer createServerCache() throws Exception
  {
    new Bug39079DUnitTest("temp").createCache(new Properties());
    DiskRegionProperties props = new DiskRegionProperties();
    props.setRegionName(REGION_NAME);
    props.setOverflow(true);
    props.setRolling(true);
    props.setDiskDirs(dirs);
    props.setPersistBackup(true);
    
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(gemfirecache, props, Scope.DISTRIBUTED_ACK);
    assertNotNull(region);
    CacheServer bs1 = gemfirecache.addCacheServer();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    bs1.setPort(port);
    bs1.start();
    return new Integer(bs1.getPort());
  }

  public static void closeCache()
  {
    if (gemfirecache != null && !gemfirecache.isClosed()) {
      gemfirecache.close();
      gemfirecache.getDistributedSystem().disconnect();
    }
  }
  
  private void createCache(Properties props) throws Exception
  {
    DistributedSystem ds = getSystem(props);
    assertNotNull(ds);
    ds.disconnect();
    ds = getSystem(props);
    gemfirecache = CacheFactory.create(ds);
    assertNotNull(gemfirecache);
  }
  
  private static void validateRuningBridgeServerList(){
    /*Region region = gemfirecache.getRegion(Region.SEPARATOR + REGION_NAME);
    assertNotNull(region);*/
    try {        
      region.create("key1", new byte[16]);
      region.create("key2", new byte[16]);
//    Get the oplog handle & hence the underlying file & close it
      UninterruptibleFileChannel oplogFileChannel = ((LocalRegion)region).getDiskRegion()
          .testHook_getChild().getFileChannel();
      try {
        oplogFileChannel.close();
        region.put("key2", new byte[16]);
      }catch(DiskAccessException dae) {
        //OK expected
      }catch (IOException e) {
        Assert.fail("test failed due to ", e);
      }
      
      ((LocalRegion) region).getDiskStore().waitForClose();
      assertTrue(region.getRegionService().isClosed());
      
      region = null;
      List bsRunning = gemfirecache.getCacheServers();
      assertTrue(bsRunning.isEmpty());
    }
    finally {
      if (region != null) {
        region.destroyRegion();
      }
    }
  }
  
  public static void createClientCache(String host, Integer port1)
      throws Exception {
    new Bug39079DUnitTest("temp");
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    new Bug39079DUnitTest("temp").createCache(props);
    PoolImpl p = (PoolImpl)PoolManager.createFactory().addServer(host,
        port1.intValue())
        .setSubscriptionEnabled(true).setSubscriptionRedundancy(0)
        .setThreadLocalConnections(true).setMinConnections(0).setReadTimeout(
            20000).setRetryAttempts(1).create("Bug39079DUnitTest");

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setPoolName(p.getName());

    RegionAttributes attrs = factory.create();
    Region r = gemfirecache.createRegion(REGION_NAME, attrs);
    //getRegion(Region.SEPARATOR + REGION_NAME);
    r.registerInterest("ALL_KEYS");
  }
}
