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

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.MEMCACHED_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.MEMCACHED_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Future;

import net.spy.memcached.MemcachedClient;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.AvailablePortHelper;

public class IntegrationJUnitTest {

  @Test
  public void testGemFireProperty() throws Exception {
    Properties props = new Properties();
    final int port = AvailablePortHelper.getRandomAvailableTCPPort();
    props.setProperty(MEMCACHED_PORT, port + "");
    props.setProperty(MCAST_PORT, "0");
    CacheFactory cf = new CacheFactory(props);
    Cache cache = cf.create();

    MemcachedClient client = new MemcachedClient(new ConnectionWithOneMinuteTimeoutFactory(),
        Collections.singletonList(new InetSocketAddress(InetAddress.getLocalHost(), port)));
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
    props.setProperty(MEMCACHED_PORT, port + "");
    props.setProperty(MEMCACHED_BIND_ADDRESS, "127.0.0.1");
    props.put(MCAST_PORT, "0");
    CacheFactory cf = new CacheFactory(props);
    Cache cache = cf.create();

    MemcachedClient client = new MemcachedClient(new ConnectionWithOneMinuteTimeoutFactory(),
        Collections.singletonList(new InetSocketAddress("127.0.0.1", port)));
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
