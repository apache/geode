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
package org.apache.geode.memcached;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;

import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.memcached.KeyWrapper;
import org.apache.geode.memcached.GemFireMemcachedServer.Protocol;

/**
 * Test for binary protocol
 */
public class GemcachedBinaryClientJUnitTest extends GemcachedDevelopmentJUnitTest {

  @Override
  protected Protocol getProtocol() {
    return Protocol.BINARY;
  }

  @Override
  protected MemcachedClient createMemcachedClient() throws IOException {
    List<InetSocketAddress> addrs = new ArrayList<>();
    addrs.add(new InetSocketAddress(InetAddress.getLocalHost(), PORT));
    MemcachedClient client = new MemcachedClient(new BinaryConnectionFactory(), addrs);
    return client;
  }

  @SuppressWarnings("unchecked")
  public void testCacheWriterException() throws Exception {
    MemcachedClient client = createMemcachedClient();
    assertTrue(client.set("key", 0, "value".getBytes()).get());
    client.set("exceptionkey", 0, "exceptionvalue").get();

    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    Region region = cache.getRegion(GemFireMemcachedServer.REGION_NAME);
    region.getAttributesMutator().setCacheWriter(new CacheWriterAdapter() {
      @Override
      public void beforeCreate(EntryEvent event) throws CacheWriterException {
        if (event.getKey().equals(KeyWrapper.getWrappedKey("exceptionkey".getBytes()))) {
          throw new RuntimeException("ExpectedStrings: Cache writer exception");
        }
      }

      @Override
      public void beforeUpdate(EntryEvent event) throws CacheWriterException {
        if (event.getKey().equals(KeyWrapper.getWrappedKey("exceptionkey".getBytes()))) {
          throw new RuntimeException("ExpectedStrings: Cache writer exception");
        }
      }
    });
    long start = System.nanoTime();
    try {
      client.set("exceptionkey", 0, "exceptionvalue").get();
      throw new RuntimeException("expected exception not thrown");
    } catch (ExecutionException e) {
      // expected
    }
    assertTrue(client.set("key2", 0, "value2".getBytes()).get());
  }

  @SuppressWarnings("unchecked")
  public void testCacheLoaderException() throws Exception {
    MemcachedClient client = createMemcachedClient();
    assertTrue(client.set("key", 0, "value").get());

    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    Region region = cache.getRegion(GemFireMemcachedServer.REGION_NAME);
    region.getAttributesMutator().setCacheLoader(new CacheLoader() {
      @Override
      public void close() {}

      @Override
      public Object load(LoaderHelper helper) throws CacheLoaderException {
        if (helper.getKey().equals(KeyWrapper.getWrappedKey("exceptionkey".getBytes()))) {
          throw new RuntimeException("ExpectedStrings: Cache loader exception");
        }
        return null;
      }
    });
    long start = System.nanoTime();
    try {
      client.get("exceptionkey");
      throw new RuntimeException("expected exception not thrown");
    } catch (Exception e) {
      // expected
    }
    assertEquals("value", client.get("key"));
  }

  @Override
  public void testDecr() throws Exception {
    super.testDecr();
    MemcachedClient client = createMemcachedClient();
    assertEquals(0, client.decr("decrkey", 999));
  }

  @Override
  public void testFlushDelay() throws Exception {
    // for some reason the server never gets expiration bits from the
    // client, so disabling for now
  }
}
