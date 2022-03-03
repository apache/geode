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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.Future;

import net.spy.memcached.MemcachedClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.util.internal.GeodeGlossary;

public class DomainObjectsAsValuesJUnitTest {

  private int PORT;

  private GemFireMemcachedServer server;

  @Before
  public void setUp() throws Exception {
    System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + MCAST_PORT, "0");
    PORT = AvailablePortHelper.getRandomAvailableTCPPort();
    server = new GemFireMemcachedServer(PORT);
    server.start();
  }

  @After
  public void tearDown() throws Exception {
    server.shutdown();
    System.getProperties().remove(GeodeGlossary.GEMFIRE_PREFIX + MCAST_PORT);
  }

  private static class Customer implements java.io.Serializable {
    private static final long serialVersionUID = 4238572216598708877L;
    private String name;
    private String address;

    public Customer() {}

    public Customer(String name, String addr) {
      setName(name);
      setAddress(addr);
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public void setAddress(String address) {
      this.address = address;
    }

    public String getAddress() {
      return address;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof Customer) {
        Customer other = (Customer) obj;
        return compareStrings(name, other.name) && compareStrings(address, other.address);
      }
      return false;
    }

    private boolean compareStrings(String str1, String str2) {
      if (str1 == null && str2 == null) {
        return true;
      } else if (str1 == null || str2 == null) {
        return false;
      }
      return str1.equals(str2);
    }

    @Override
    public String toString() {
      return getClass() + "@" + System.identityHashCode(this)
          + "name:" + name + "address:" + address;
    }
  }

  @Test
  public void testGetPutDomainObject() throws Exception {
    MemcachedClient client = new MemcachedClient(new ConnectionWithOneMinuteTimeoutFactory(),
        Collections.singletonList(new InetSocketAddress(InetAddress.getLocalHost(), PORT)));
    Customer c = new Customer("name0", "addr0");
    Customer c1 = new Customer("name1", "addr1");
    Future<Boolean> f = client.add("keyObj", 10, c);
    assertTrue(f.get());
    Future<Boolean> f1 = client.add("key1", 10, c1);
    assertTrue(f1.get());
    assertEquals(c, client.get("keyObj"));
    assertEquals(c1, client.get("key1"));
    assertNull(client.get("nonExistentkey"));
  }
}
