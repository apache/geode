/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

import junit.framework.TestCase;

/**
 * 
 * @author sbawaska
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
