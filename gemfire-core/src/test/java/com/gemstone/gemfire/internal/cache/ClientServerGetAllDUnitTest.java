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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestUtils;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Class <code>ClientServerGetAllDUnitTest</code> test client/server getAll.
 *
 * @author Barry Oglesby
 * @since 5.7
 */
 public class ClientServerGetAllDUnitTest extends ClientServerTestCase {

  public ClientServerGetAllDUnitTest(String name) {
    super(name);
  }

  @Override
  protected final void postTearDownCacheTestCase() throws Exception {
    disconnectAllFromDS();
  }

  public void testGetAllFromServer() throws Exception {
    final Host host = Host.getHost(0);
    final VM server = host.getVM(0);
    final VM client = host.getVM(1);
    final String regionName = getUniqueName();
    final int mcastPort = 0; /* loner is ok for this test*/ //AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    final int serverPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String serverHost = NetworkUtils.getServerHostName(server.getHost());

    createBridgeServer(server, regionName, serverPort, false, false);

    createBridgeClient(client, regionName, serverHost, new int[] {serverPort});

    // Run getAll
    client.invoke(new CacheSerializableRunnable("Get all entries from server") {
      @Override
      public void run2() throws CacheException {
        // Build collection of keys
        Collection keys = new ArrayList();
        for (int i=0; i<5; i++) {
          keys.add("key-"+i);
        }
        
        keys.add(ClientServerTestCase.NON_EXISTENT_KEY); // this will not be load CacheLoader
        
        // Invoke getAll
        Region region = getRootRegion(regionName);
        Map result = region.getAll(keys);

        // Verify result size is correct
        assertEquals(6, result.size());

        // Verify the result contains each key,
        // and the value for each key is correct
        // (the server has a loader that returns the key as the value)
        for (Iterator i = keys.iterator(); i.hasNext();) {
          String key = (String) i.next();
          assertTrue(result.containsKey(key));
          Object value = result.get(key);
          if(!key.equals(ClientServerTestCase.NON_EXISTENT_KEY))
            assertEquals(key, value);
          else
            assertEquals(null, value);
        }
        
        assertEquals(null, region.get(ClientServerTestCase.NON_EXISTENT_KEY));
      }
    });

    stopBridgeServer(server);
  }

  public void testOffHeapGetAllFromServer() throws Exception {
    final Host host = Host.getHost(0);
    final VM server = host.getVM(0);
    final VM client = host.getVM(1);
    final String regionName = getUniqueName();
    final int serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final String serverHost = NetworkUtils.getServerHostName(server.getHost());

    createBridgeServer(server, regionName, serverPort, false, false, true/*offheap*/);

    createBridgeClient(client, regionName, serverHost, new int[] {serverPort});

    // Run getAll
    client.invoke(new CacheSerializableRunnable("Get all entries from server") {
      @Override
      public void run2() throws CacheException {
        // Build collection of keys
        Collection keys = new ArrayList();
        for (int i=0; i<5; i++) {
          keys.add("key-"+i);
        }
        
        keys.add(ClientServerTestCase.NON_EXISTENT_KEY); // this will not be load CacheLoader
        
        // Invoke getAll
        Region region = getRootRegion(regionName);
        Map result = region.getAll(keys);

        // Verify result size is correct
        assertEquals(6, result.size());

        // Verify the result contains each key,
        // and the value for each key is correct
        // (the server has a loader that returns the key as the value)
        for (Iterator i = keys.iterator(); i.hasNext();) {
          String key = (String) i.next();
          assertTrue(result.containsKey(key));
          Object value = result.get(key);
          if(!key.equals(ClientServerTestCase.NON_EXISTENT_KEY))
            assertEquals(key, value);
          else
            assertEquals(null, value);
        }
        
        assertEquals(null, region.get(ClientServerTestCase.NON_EXISTENT_KEY));
      }
    });
    checkServerForOrphans(server, regionName);

    stopBridgeServer(server);
  }

  public void testLargeOffHeapGetAllFromServer() throws Throwable {
    final Host host = Host.getHost(0);
    final VM server = host.getVM(0);
    final VM client = host.getVM(1);
    final String regionName = getUniqueName();
    final int serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final String serverHost = NetworkUtils.getServerHostName(server.getHost());

    createBridgeServer(server, regionName, serverPort, false, false, true/*offheap*/);

    createBridgeClient(client, regionName, serverHost, new int[] {serverPort}, true);
    
    final int VALUE_SIZE = 1024 * 2/* *1024*/;

    final int VALUE_COUNT = 100;
    
    client.invoke(new CacheSerializableRunnable("put entries on server") {
      @Override
      public void run2() throws CacheException {
        final byte[] VALUE = new byte[VALUE_SIZE];
        for (int i=0; i < VALUE_SIZE; i++) {
          VALUE[i] = (byte)i;
        }
        Region region = getRootRegion(regionName);
        for (int i=0; i < VALUE_COUNT; i++) {
          region.put("k"+i, new UnitTestValueHolder(VALUE));
        }
      }
    });

    CacheSerializableRunnable clientGetAll = new CacheSerializableRunnable("Get all entries from server") {
      @Override
      public void run2() throws CacheException {
        // Build collection of keys
        Collection keys = new ArrayList();
        for (int i=0; i<VALUE_COUNT; i++) {
          keys.add("k"+i);
        }
        
        // Invoke getAll
        Region region = getRootRegion(regionName);
        final int GET_COUNT = 10/*0*/;
        long start = System.currentTimeMillis();
        Map result = null;
        for (int i=0; i < GET_COUNT; i++) {
          result = null; // allow gc to get rid of previous map before deserializing the next one
          result = region.getAll(keys);
        }
        long end = System.currentTimeMillis();
        long totalBytesRead = ((long)GET_COUNT * VALUE_COUNT * VALUE_SIZE);
        long elapsedMillis = (end-start);
        System.out.println("PERF: read " + totalBytesRead + " bytes in " + elapsedMillis + " millis. bps=" + (((double)totalBytesRead / elapsedMillis) * 1000));

        // Verify result size is correct
        assertEquals(VALUE_COUNT, result.size());

        final byte[] EXPECTED = new byte[VALUE_SIZE];
        for (int i=0; i < VALUE_SIZE; i++) {
          EXPECTED[i] = (byte)i;
        }
        // Verify the result contains each key,
        // and the value for each key is correct
        // (the server has a loader that returns the key as the value)
        for (Iterator i = keys.iterator(); i.hasNext();) {
          String key = (String) i.next();
          assertTrue(result.containsKey(key));
          Object value = result.get(key);
          if (value instanceof UnitTestValueHolder) {
            Object v = ((UnitTestValueHolder) value).getValue();
            if (v instanceof byte[]) {
              byte[] bytes = (byte[]) v;
              if (bytes.length != VALUE_SIZE) {
                fail("expected value for key " + key + " to be an array of size " + (VALUE_SIZE) + " but it was: " + bytes.length);
              }
              if (!Arrays.equals(EXPECTED, bytes)) {
                fail("expected bytes=" + Arrays.toString(bytes) + " to be expected=" + Arrays.toString(EXPECTED));
              }
            } else {
              fail("expected v for key " + key + " to be a byte array but it was: " + v);
            }
          } else {
            fail("expected value for key " + key + " to be a UnitTestValueHolder but it was: " + value);
          }
        }
      }
    };
    // Run getAll
    {
      final int THREAD_COUNT = 4;
      AsyncInvocation[] ais = new AsyncInvocation[THREAD_COUNT];
      for (int i=0; i < THREAD_COUNT; i++) {
        ais[i] = client.invokeAsync(clientGetAll);
      }
      for (int i=0; i < THREAD_COUNT; i++) {
        ais[i].getResult();
      }
    }

    server.invoke(new CacheSerializableRunnable("Dump OffHeap Stats") {
      @Override
      public void run2() throws CacheException {
        SimpleMemoryAllocatorImpl ma = SimpleMemoryAllocatorImpl.getAllocator();
        System.out.println("STATS: objects=" + ma.getStats().getObjects() + " usedMemory=" + ma.getStats().getUsedMemory() + " reads=" + ma.getStats().getReads());
      }
    });

    checkServerForOrphans(server, regionName);

    stopBridgeServer(server);
  }
  
  public void testLargeGetAllFromServer() throws Throwable {
    final Host host = Host.getHost(0);
    final VM server = host.getVM(0);
    final VM client = host.getVM(1);
    final String regionName = getUniqueName();
    final int mcastPort = 0; /* loner is ok for this test*/ //AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    final int serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final String serverHost = NetworkUtils.getServerHostName(server.getHost());

    createBridgeServer(server, regionName, serverPort, false, false);

    createBridgeClient(client, regionName, serverHost, new int[] {serverPort}, true);
    
    final int VALUE_SIZE = 1024 * 2/* *1024*/;

    final int VALUE_COUNT = 100;
    
    client.invoke(new CacheSerializableRunnable("put entries on server") {
      @Override
      public void run2() throws CacheException {
        final byte[] VALUE = new byte[VALUE_SIZE];
        for (int i=0; i < VALUE_SIZE; i++) {
          VALUE[i] = (byte)i;
        }
        Region region = getRootRegion(regionName);
        for (int i=0; i < VALUE_COUNT; i++) {
          region.put("k"+i, new UnitTestValueHolder(VALUE));
        }
      }
    });

    // force them to be deserialized on server
    server.invoke(new CacheSerializableRunnable("deserialize entries on server") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion(regionName);
        for (int i=0; i < VALUE_COUNT; i++) {
          region.get("k"+i);
        }
      }
    });

    CacheSerializableRunnable clientGetAll = new CacheSerializableRunnable("Get all entries from server") {
      @Override
      public void run2() throws CacheException {
        // Build collection of keys
        Collection keys = new ArrayList();
        for (int i=0; i<VALUE_COUNT; i++) {
          keys.add("k"+i);
        }
        
        // Invoke getAll
        Region region = getRootRegion(regionName);
        final int GET_COUNT = 10/*0*/;
        long start = System.currentTimeMillis();
        Map result = null;
        for (int i=0; i < GET_COUNT; i++) {
          result = null; // allow gc to get rid of previous map before deserializing the next one
          result = region.getAll(keys);
        }
        long end = System.currentTimeMillis();
        long totalBytesRead = ((long)GET_COUNT * VALUE_COUNT * VALUE_SIZE);
        long elapsedMillis = (end-start);
        System.out.println("PERF: read " + totalBytesRead + " bytes in " + elapsedMillis + " millis. bps=" + (((double)totalBytesRead / elapsedMillis) * 1000));

        // Verify result size is correct
        assertEquals(VALUE_COUNT, result.size());

        final byte[] EXPECTED = new byte[VALUE_SIZE];
        for (int i=0; i < VALUE_SIZE; i++) {
          EXPECTED[i] = (byte)i;
        }
        // Verify the result contains each key,
        // and the value for each key is correct
        // (the server has a loader that returns the key as the value)
        for (Iterator i = keys.iterator(); i.hasNext();) {
          String key = (String) i.next();
          assertTrue(result.containsKey(key));
          Object value = result.get(key);
          if (value instanceof UnitTestValueHolder) {
            Object v = ((UnitTestValueHolder) value).getValue();
            if (v instanceof byte[]) {
              byte[] bytes = (byte[]) v;
              if (bytes.length != VALUE_SIZE) {
                fail("expected value for key " + key + " to be an array of size " + (VALUE_SIZE) + " but it was: " + bytes.length);
              }
              if (!Arrays.equals(EXPECTED, bytes)) {
                fail("expected bytes=" + Arrays.toString(bytes) + " to be expected=" + Arrays.toString(EXPECTED));
              }
            } else {
              fail("expected v for key " + key + " to be a byte array but it was: " + v);
            }
          } else {
            fail("expected value for key " + key + " to be a UnitTestValueHolder but it was: " + value);
          }
        }
      }
    };
    // Run getAll
    {
      final int THREAD_COUNT = 4;
      AsyncInvocation[] ais = new AsyncInvocation[THREAD_COUNT];
      for (int i=0; i < THREAD_COUNT; i++) {
        ais[i] = client.invokeAsync(clientGetAll);
      }
      for (int i=0; i < THREAD_COUNT; i++) {
        ais[i].getResult();
      }
    }
    stopBridgeServer(server);
  }

  public void testGetAllWithCallbackFromServer() throws Exception {
    final Host host = Host.getHost(0);
    final VM server = host.getVM(0);
    final VM client = host.getVM(1);
    final String regionName = getUniqueName();
    final int serverPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String serverHost = NetworkUtils.getServerHostName(server.getHost());

    createBridgeServer(server, regionName, serverPort, false, true);

    createBridgeClient(client, regionName, serverHost, new int[] {serverPort});

    // Run getAll
    client.invoke(new CacheSerializableRunnable("Get all entries from server") {
      @Override
      public void run2() throws CacheException {
        // Build collection of keys
        Collection keys = new ArrayList();
        for (int i=0; i<5; i++) {
          keys.add("key-"+i);
        }
        
        keys.add(ClientServerTestCase.NON_EXISTENT_KEY); // this will not be load CacheLoader
        
        // Invoke getAll
        Region region = getRootRegion(regionName);
        Map result = region.getAll(keys, CALLBACK_ARG);

        // Verify result size is correct
        assertEquals(6, result.size());

        // Verify the result contains each key,
        // and the value for each key is correct
        // (the server has a loader that returns the key as the value)
        for (Iterator i = keys.iterator(); i.hasNext();) {
          String key = (String) i.next();
          assertTrue(result.containsKey(key));
          Object value = result.get(key);
          if(!key.equals(ClientServerTestCase.NON_EXISTENT_KEY))
            assertEquals(key, value);
          else
            assertEquals(null, value);
        }
        
        assertEquals(null, region.get(ClientServerTestCase.NON_EXISTENT_KEY));
      }
    });

    stopBridgeServer(server);
  }

  public void testGetSomeFromServer() throws Exception {
    testGetFromServer(2);
  }

  public void testGetAllFromClient() throws Exception {
    testGetFromServer(5);
  }

  public void testGetAllFromServerWithPR() throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM server2 = host.getVM(1);
    final VM client = host.getVM(2);
    final String regionName = getUniqueName();
    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final int server1Port = ports[0];
    final int server2Port = ports[1];
    final String serverHost = NetworkUtils.getServerHostName(server1.getHost());

    createBridgeServer(server1, regionName, server1Port, true, false);

    createBridgeServer(server2, regionName, server2Port, true, false);

    createBridgeClient(client, regionName, serverHost, new int[] {server1Port, server2Port});

    // Run getAll
    client.invoke(new CacheSerializableRunnable("Get all entries from server") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion(regionName);
        for (int i=0; i<200; i++) {
          region.put(i, i);
        }
        
        try {
          Thread.currentThread().sleep(1000);
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();          
        }
        // Build collection of keys
        Collection keys = new ArrayList();
        for (int i=0; i<5; i++) {
          keys.add("key-"+i);
        }
        keys.add(ClientServerTestCase.NON_EXISTENT_KEY); // this will not be load CacheLoader
        
        // Invoke getAll
        
        Map result = region.getAll(keys);
        

        // Verify result size is correct
        assertEquals(6, result.size());

        // Verify the result contains each key,
        // and the value for each key is correct
        // (the server has a loader that returns the key as the value)
        for (Iterator i = keys.iterator(); i.hasNext();) {
          String key = (String) i.next();
          assertTrue(result.containsKey(key));
          Object value = result.get(key);
          if(!key.equals(ClientServerTestCase.NON_EXISTENT_KEY))
            assertEquals(key, value);
          else
            assertEquals(null, value);
        }
        assertEquals(null, region.get(ClientServerTestCase.NON_EXISTENT_KEY));
      }
    });

    stopBridgeServer(server1);

    stopBridgeServer(server2);
  }

  private void testGetFromServer(final int numLocalValues) {
    final Host host = Host.getHost(0);
    final VM server = host.getVM(0);
    final VM client = host.getVM(1);
    final String regionName = getUniqueName();
    final int serverPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String serverHost = NetworkUtils.getServerHostName(server.getHost());

    createBridgeServer(server, regionName, serverPort, false, false);

    createBridgeClient(client, regionName, serverHost, new int[] {serverPort});

    // Put some entries from the client
    client.invoke(new CacheSerializableRunnable("Put entries from client") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion(regionName);
        for (int i=0; i<numLocalValues; i++) {
          region.put("key-"+i, "value-from-client-"+i);
        }
      }
    });

    // Run getAll
    client.invoke(new CacheSerializableRunnable("Get all entries from server") {
      @Override
      public void run2() throws CacheException {
        // Build collection of keys
        Collection keys = new ArrayList();
        for (int i=0; i<5; i++) {
          keys.add("key-"+i);
        }

        // Invoke getAll
        Region region = getRootRegion(regionName);
        Map result = region.getAll(keys);

        // Verify result size is correct
        assertEquals(5, result.size());

        // Verify the result contains each key,
        // and the value for each key is correct
        // (the server has a loader that returns the key as the value)
        // (the local value contains the phrase 'from client')
        int i = 0;
        for (Iterator it = keys.iterator(); it.hasNext(); i++) {
          String key = (String) it.next();
          assertTrue(result.containsKey(key));
          Object value = result.get(key);
          if (i < numLocalValues) {
            assertEquals("value-from-client-"+i, value);
          } else {
            assertEquals(key, value);
          }
        }
      }
    });

    // client may see "server unreachable" exceptions after this
    IgnoredException.addIgnoredException("Server unreachable", client);
    stopBridgeServer(server);
  }
  
  public void testGetAllWithExtraKeyFromServer() throws Exception {
    final Host host = Host.getHost(0);
    final VM server = host.getVM(0);
    final VM client = host.getVM(1);
    final String regionName = getUniqueName();
    final int serverPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String serverHost = NetworkUtils.getServerHostName(server.getHost());
    final int numLocalValues = 101;
    
    createBridgeServerWithoutLoader(server, regionName, serverPort, false);

    createBridgeClient(client, regionName, serverHost, new int[] {serverPort});

    // Put some entries from the client
    client.invoke(new CacheSerializableRunnable("Put entries from client") {
      public void run2() throws CacheException {
        Region region = getRootRegion(regionName);
        for (int i = 0; i < numLocalValues; i++) {
          region.put("key-" + i, "value-from-client-" + i);
        }
      }
    });
    
    server.invoke(new CacheSerializableRunnable("Put entries from server") {
      public void run2() throws CacheException {
        Region region = getRootRegion(regionName);
        for (int i = numLocalValues; i < numLocalValues*2; i++) {
          region.put("key-" + i, "value-from-server-" + i);
        }
        region.getCache().getLogger().fine("The region entries in server " + region.entrySet());
      }
    });
    
    
    // Run getAll
    client.invoke(new CacheSerializableRunnable("Get all entries from server") {
      public void run2() throws CacheException {
        // Build collection of keys
        Collection keys = new ArrayList();
        for (int i=0; i<numLocalValues*3; i++) {
          keys.add("key-"+i);
        }

        // Invoke getAll
        Region region = getRootRegion(regionName);
        region.getCache().getLogger().fine("The region entries in client before getAll " + region.entrySet());
        assertEquals(region.entrySet().size(),numLocalValues);
        Map result = region.getAll(keys);
        assertEquals(region.entrySet().size(),2*numLocalValues);
        region.getCache().getLogger().fine("The region entries in client after getAll " + region.entrySet());
        
        // Verify result size is correct
        assertEquals(3*numLocalValues, result.size());

        // Verify the result contains each key,
        // and the value for each key is correct
        int i = 0;
        for (Iterator it = keys.iterator(); it.hasNext(); i++) {
          String key = (String) it.next();
          assertTrue(result.containsKey(key));
          Object value = result.get(key);
          if (i < numLocalValues) {
            assertEquals("value-from-client-"+i, value);
          }else if(i < 2*numLocalValues) {
            assertEquals("value-from-server-"+i, value);
          }
          else {
            assertEquals(null, value);
          }
        }
      }
    });

    stopBridgeServer(server);
  }

  private void createBridgeServer(VM server, final String regionName, final int serverPort, final boolean createPR, final boolean expectCallback) {
    createBridgeServer(server, regionName, serverPort, createPR, expectCallback, false);
  }
  

  private void createBridgeServer(VM server, final String regionName, final int serverPort, final boolean createPR, final boolean expectCallback, final boolean offheap) {
    server.invoke(new CacheSerializableRunnable("Create server") {
      @Override
      public void run2() throws CacheException {
        // Create DS
        Properties config = new Properties();
        config.setProperty("locators", "localhost["+DistributedTestUtils.getDUnitLocatorPort()+"]");
        if (offheap) {
          config.setProperty(DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME, "350m");
        }
        getSystem(config);

        // Create Region
        AttributesFactory factory = new AttributesFactory();
        if (offheap) {
          factory.setOffHeap(true);
        }
        if (expectCallback) {
          factory.setCacheLoader(new CallbackCacheServerCacheLoader());
        } else {
          factory.setCacheLoader(new CacheServerCacheLoader());
        }
        if (createPR) {
          factory.setDataPolicy(DataPolicy.PARTITION);
          factory.setPartitionAttributes((new PartitionAttributesFactory()).create());
        } else {
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.REPLICATE);
        }
        Region region = createRootRegion(regionName, factory.create());
        if (createPR) {
          assertTrue(region instanceof PartitionedRegion);
        }
        try {
          Cache cache = getCache();
          CacheServer bridge = cache.addCacheServer();
          bridge.setPort(serverPort);
          // for off-heap I want the server to use a selector
          bridge.setMaxThreads(offheap ? 16 : getMaxThreads());
          bridge.start();
        } catch (Exception e) {
          Assert.fail("While starting CacheServer", e);
        }
      }
    });
  }
  
  private static final String CALLBACK_ARG = "ClientServerGetAllDUnitTestCB";

  private static class CallbackCacheServerCacheLoader extends CacheServerCacheLoader {
    @Override
    public Object load2(LoaderHelper helper) {
      if (helper.getArgument() instanceof String) {
        if (!CALLBACK_ARG.equals(helper.getArgument())) {
          fail("Expected " + helper.getArgument() + " to be " + CALLBACK_ARG);
        }
      } else {
        if (!helper.getKey().equals(ClientServerTestCase.NON_EXISTENT_KEY)) {
          fail("Expected callback arg to be " + CALLBACK_ARG + " but it was null");
        }
      }
      return super.load2(helper);
    }
  }

  private void createBridgeServerWithoutLoader(VM server, final String regionName, final int serverPort, final boolean createPR) {
    server.invoke(new CacheSerializableRunnable("Create server") {
      public void run2() throws CacheException {
        // Create DS
        Properties config = new Properties();
        config.setProperty("locators", "localhost["+DistributedTestUtils.getDUnitLocatorPort()+"]");
        getSystem(config);

        // Create Region
        AttributesFactory factory = new AttributesFactory();
        if (createPR) {
          factory.setDataPolicy(DataPolicy.PARTITION);
          factory.setPartitionAttributes((new PartitionAttributesFactory()).create());
        } else {
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.REPLICATE);
        }
        Region region = createRootRegion(regionName, factory.create());
        if (createPR) {
          assertTrue(region instanceof PartitionedRegion);
        }
        try {
          startBridgeServer(serverPort);
          System.out.println("Started bridger server ");
        } catch (Exception e) {
          Assert.fail("While starting CacheServer", e);
        }
      }
    });
  }
  private void createBridgeClient(VM client, final String regionName, final String serverHost, final int[] serverPorts) {
    createBridgeClient(client, regionName, serverHost, serverPorts, false);
  }
  
  private void createBridgeClient(VM client, final String regionName, final String serverHost, final int[] serverPorts, final boolean proxy) {
    client.invoke(new CacheSerializableRunnable("Create client") {
      @Override
      public void run2() throws CacheException {
        // Create DS
        Properties config = new Properties();
        config.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
        config.setProperty(DistributionConfig.LOCATORS_NAME, "");
        getSystem(config);

        // Create Region
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        if (proxy) {
          factory.setDataPolicy(DataPolicy.EMPTY);
        }
        {
          PoolFactory pf = PoolManager.createFactory();
          for (int i=0; i < serverPorts.length; i++) {
            pf.addServer(serverHost, serverPorts[i]);
          }
          if (proxy) {
            pf.setReadTimeout(30000);
          }
          pf.create("myPool");
        }
        factory.setPoolName("myPool");
        createRootRegion(regionName, factory.create());
      }
    });
  }

  private void stopBridgeServer(VM server) {
    server.invoke(new CacheSerializableRunnable("Stop Server") {
      @Override
      public void run2() throws CacheException {
        stopBridgeServers(getCache());
      }
    });
  }
  private void checkServerForOrphans(VM server, final String regionName) {
    server.invoke(new CacheSerializableRunnable("Stop Server") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion(regionName);
        region.close();
        OffHeapTestUtil.checkOrphans();
      }
    });

  }
}

