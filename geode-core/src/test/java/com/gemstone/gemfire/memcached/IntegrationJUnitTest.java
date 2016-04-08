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
package com.gemstone.gemfire.memcached;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.Future;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import net.spy.memcached.MemcachedClient;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

import junit.framework.TestCase;

/**
 * 
 */
@Category(IntegrationTest.class)
public class IntegrationJUnitTest {

  @Test
  public void testGemFireProperty() throws Exception {
    Properties props = new Properties();
    final int port = AvailablePortHelper.getRandomAvailableTCPPort();
    props.setProperty("memcached-port", port+"");
    props.setProperty("mcast-port", "0");
    CacheFactory cf = new CacheFactory(props);
    Cache cache = cf.create();
    
    MemcachedClient client = new MemcachedClient(
        new InetSocketAddress(InetAddress.getLocalHost(), port));
    Future<Boolean> f = client.add("key", 10, "myStringValue");
    assertTrue(f.get());
    Future<Boolean> f1 = client.add("key1", 10, "myStringValue1");
    assertTrue(f1.get());
    
    assertEquals("myStringValue", client.get("key"));
    assertEquals("myStringValue1", client.get("key1"));
    assertNull(client.get("nonExistentkey"));
    cache.close();
  }

  @Test
  public void testMemcachedBindAddress() throws Exception {
    Properties props = new Properties();
    final int port = AvailablePortHelper.getRandomAvailableTCPPort();
    props.setProperty("memcached-port", port+"");
    props.setProperty("memcached-bind-address", "127.0.0.1");
    props.put(DistributionConfig.MCAST_PORT_NAME, "0");
    CacheFactory cf = new CacheFactory(props);
    Cache cache = cf.create();

    MemcachedClient client = new MemcachedClient(
        new InetSocketAddress("127.0.0.1", port));
    Future<Boolean> f = client.add("key", 10, "myStringValue");
    assertTrue(f.get());
    Future<Boolean> f1 = client.add("key1", 10, "myStringValue1");
    assertTrue(f1.get());
    
    assertEquals("myStringValue", client.get("key"));
    assertEquals("myStringValue1", client.get("key1"));
    assertNull(client.get("nonExistentkey"));
    cache.close();
  }
}
