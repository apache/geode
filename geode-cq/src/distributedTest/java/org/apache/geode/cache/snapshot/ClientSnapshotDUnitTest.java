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
package org.apache.geode.cache.snapshot;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import com.examples.snapshot.MyObject;
import com.examples.snapshot.MyPdxSerializer;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.snapshot.SnapshotOptions.SnapshotFormat;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.cache.util.CqListenerAdapter;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

@Category({ClientSubscriptionTest.class})
public class ClientSnapshotDUnitTest extends JUnit4CacheTestCase {

  private transient Region<Integer, MyObject> region;

  @Override
  public final void postSetUp() throws Exception {
    loadCache();
  }

  @Test
  public void testExport() throws Exception {
    int count = 10000;
    for (int i = 0; i < count; i++) {
      region.put(i, new MyObject(i, "clienttest " + i));
    }

    SerializableCallable export = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        File f = new File(getDiskDirs()[0], "client-export.snapshot.gfd");
        Region<Integer, MyObject> r = getCache().getRegion("clienttest");

        r.getSnapshotService().save(f, SnapshotFormat.GEODE);

        return f;
      }
    };

    File snapshot = (File) Host.getHost(0).getVM(3).invoke(export);

    SnapshotIterator<Integer, MyObject> iter = SnapshotReader.read(snapshot);
    try {
      while (iter.hasNext()) {
        iter.next();
        count--;
      }
      assertEquals(0, count);
    } finally {
      iter.close();
    }
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties result = super.getDistributedSystemProperties();
    result.put(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        "com.examples.snapshot.MyObject");
    return result;
  }

  @Test
  public void testImport() throws Exception {
    int count = 1000;
    for (int i = 0; i < count; i++) {
      region.put(i, new MyObject(i, "clienttest " + i));
    }

    SerializableCallable export = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        File f = new File(getDiskDirs()[0], "client-import.snapshot.gfd");
        Region<Integer, MyObject> r = getCache().getRegion("clienttest");

        r.getSnapshotService().save(f, SnapshotFormat.GEODE);

        return f;
      }
    };

    Host.getHost(0).getVM(3).invoke(export);
    for (int i = 0; i < count; i++) {
      region.put(i, new MyObject(i, "XXX"));
    }

    SerializableCallable imp = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        final AtomicBoolean cqtest = new AtomicBoolean(false);
        CqAttributesFactory af = new CqAttributesFactory();
        af.addCqListener(new CqListenerAdapter() {
          @Override
          public void onEvent(CqEvent aCqEvent) {
            cqtest.set(true);
          }
        });

        Region<Integer, MyObject> r = getCache().getRegion("clienttest");
        CqQuery cq =
            r.getRegionService().getQueryService()
                .newCq("SELECT * FROM " + SEPARATOR + "clienttest", af.create());
        cq.execute();

        File f = new File(getDiskDirs()[0], "client-import.snapshot.gfd");
        r.getSnapshotService().load(f, SnapshotFormat.GEODE);

        return cqtest.get();
      }
    };

    // add callbacks
    region.getAttributesMutator().setCacheWriter(new CacheWriterAdapter<Integer, MyObject>() {
      @Override
      public void beforeUpdate(EntryEvent<Integer, MyObject> event) {
        fail("CacheWriter invoked during import");
      }
    });

    final AtomicBoolean cltest = new AtomicBoolean(false);
    region.getAttributesMutator().addCacheListener(new CacheListenerAdapter<Integer, MyObject>() {
      @Override
      public void afterUpdate(EntryEvent<Integer, MyObject> event) {
        cltest.set(true);
      }
    });

    boolean cqtest = (Boolean) Host.getHost(0).getVM(3).invoke(imp);
    assertEquals("CacheListener invoked during import", false, cltest.get());
    assertEquals("CqListener invoked during import", false, cqtest);

    for (MyObject obj : region.values()) {
      assertTrue(obj.getF2().startsWith("clienttest"));
    }
  }

  @Test
  public void testClientCallbacks() throws Exception {
    int count = 1000;
    for (int i = 0; i < count; i++) {
      region.put(i, new MyObject(i, "clienttest " + i));
    }

    File f = new File(getDiskDirs()[0], "client-callback.snapshot.gfd");
    region.getSnapshotService().save(f, SnapshotFormat.GEODE);

    for (int i = 0; i < count; i++) {
      region.put(i, new MyObject(i, "XXX"));
    }

    SerializableCallable callbacks = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region<Integer, MyObject> r = getCache().getRegion("clienttest");
        r.registerInterestRegex(".*");

        r.getAttributesMutator().setCacheWriter(new CacheWriterAdapter<Integer, MyObject>() {
          @Override
          public void beforeUpdate(EntryEvent<Integer, MyObject> event) {
            fail("CacheWriter invoked during import");
          }
        });

        r.getAttributesMutator().addCacheListener(new CacheListenerAdapter<Integer, MyObject>() {
          @Override
          public void afterUpdate(EntryEvent<Integer, MyObject> event) {
            fail("CacheListener was invoked during import");
          }
        });

        final AtomicBoolean cqtest = new AtomicBoolean(false);
        CqAttributesFactory af = new CqAttributesFactory();
        af.addCqListener(new CqListenerAdapter() {
          @Override
          public void onEvent(CqEvent aCqEvent) {
            fail("Cq was invoked during import");
          }
        });

        CqQuery cq =
            r.getRegionService().getQueryService()
                .newCq("SELECT * FROM " + SEPARATOR + "clienttest", af.create());
        cq.execute();

        return null;
      }
    };

    Host.getHost(0).getVM(3).invoke(callbacks);
    region.getSnapshotService().load(f, SnapshotFormat.GEODE);
  }

  @Test
  public void testInvalidate() throws Exception {
    SerializableCallable invalid = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region<Integer, MyObject> r = getCache().getRegion("clienttest");

        r.put(1, new MyObject(1, "invalidate"));
        r.invalidate(1);

        File f = new File(getDiskDirs()[0], "client-invalidate.snapshot.gfd");
        r.getSnapshotService().save(f, SnapshotFormat.GEODE);
        r.getSnapshotService().load(f, SnapshotFormat.GEODE);

        return null;
      }
    };

    Host.getHost(0).getVM(3).invoke(invalid);

    assertTrue(region.containsKey(1));
    assertFalse(region.containsValueForKey(1));
    assertNull(region.get(1));
  }

  @SuppressWarnings("serial")
  public void loadCache() throws Exception {
    CacheFactory cf = new CacheFactory().setPdxSerializer(new MyPdxSerializer());
    cf.set(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER, "com.examples.snapshot.MyObject");
    Cache cache = getCache(cf);

    CacheServer server = cache.addCacheServer();
    final int port = AvailablePortHelper.getRandomAvailableTCPPort();
    server.setPort(port);
    server.start();

    region =
        cache.<Integer, MyObject>createRegionFactory(RegionShortcut.REPLICATE).create("clienttest");

    final Host host = Host.getHost(0);
    SerializableCallable client = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf =
            new ClientCacheFactory().set(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel())
                .setPdxSerializer(new MyPdxSerializer())
                .addPoolServer(NetworkUtils.getServerHostName(host), port)
                .setPoolSubscriptionEnabled(true).setPoolPRSingleHopEnabled(false);
        cf.set(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
            "com.examples.snapshot.MyObject");

        ClientCache cache = getClientCache(cf);
        Region r = cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY_HEAP_LRU)
            .setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(5))
            .create("clienttest");
        return null;
      }
    };

    SerializableCallable remote = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        CacheFactory cf = new CacheFactory().setPdxSerializer(new MyPdxSerializer());
        cf.set(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
            "com.examples.snapshot.MyObject");
        Cache cache = getCache(cf);

        cache.<Integer, MyObject>createRegionFactory(RegionShortcut.REPLICATE).create("clienttest");
        return null;
      }
    };

    host.getVM(3).invoke(client);
    host.getVM(2).invoke(remote);
  }
}
