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
package org.apache.geode.cache30;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.MirrorType;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.internal.cache.xmlcache.CacheTransactionManagerCreation;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.internal.cache.xmlcache.RegionAttributesCreation;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * Tests the declarative caching functionality introduced in GemFire
 * 4.0. 
 *
 * @since GemFire 4.0
 */
@Category(DistributedTest.class)
public class CacheXml40DUnitTest extends CacheXml30DUnitTest {

  protected String getGemFireVersion() {
    return CacheXml.VERSION_4_0;
  }

  /**
   * Tests the cache server attribute
   *
   * @since GemFire 4.0
   */
  @Test
  public void testServer() {
    CacheCreation cache = new CacheCreation();
    cache.setIsServer(true);
    assertTrue(cache.isServer());

    testXml(cache);
  }

  /**
   * Tests declarative bridge servers
   *
   * @since GemFire 4.0
   */
  @Test
  public void testBridgeServers() {
    CacheCreation cache = new CacheCreation();

    CacheServer bridge1 = cache.addCacheServer();
    setBridgeAttributes(bridge1);

    CacheServer bridge2 = cache.addCacheServer();
    setBridgeAttributes(bridge2);

    testXml(cache);
  }

  /**
   * Used by testBridgeServers to set version specific attributes
   * @param bridge1 the bridge server to set attributes upon
   */
  public void setBridgeAttributes(CacheServer bridge1)
  {
    //@see http://docs.oracle.com/javase/7/docs/api/java/net/InetSocketAddress.html#InetSocketAddress(int)
    bridge1.setPort(0);
  }

  /**
   * Tests the is-lock-grantor attribute in xml.
   */
  @Test
  public void testIsLockGrantorAttribute() throws CacheException {

    CacheCreation cache = new CacheCreation();
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);

    attrs.setLockGrantor(true);
    attrs.setScope(Scope.GLOBAL);
    attrs.setMirrorType(MirrorType.KEYS_VALUES);

    cache.createRegion("root", attrs);

    testXml(cache);
    assertEquals(true, cache.getRegion("root").getAttributes().isLockGrantor());
  }

  /**
   * Tests a cache listener with no parameters
   *
   * @since GemFire 4.0
   */
  @Test
  public void testTransactionListener() {
    CacheCreation cache = new CacheCreation();
    CacheTransactionManagerCreation txMgrCreation = new CacheTransactionManagerCreation();
    txMgrCreation.setListener(new MyTestTransactionListener());
    cache.addCacheTransactionManagerCreation(txMgrCreation);
    testXml(cache);
  }

  /**
   * Tests transaction manager with no listener
   *
   * @since GemFire 4.0
   */
  @Test
  public void testCacheTransactionManager() {
    CacheCreation cache = new CacheCreation();
    CacheTransactionManagerCreation txMgrCreation = new CacheTransactionManagerCreation();
    cache.addCacheTransactionManagerCreation(txMgrCreation);
    testXml(cache);
  }

  /**
   * Tests the value constraints region attribute that was added in
   * GemFire 4.0.
   *
   * @since GemFire 4.1
   */
  @Test
  public void testConstrainedValues() throws CacheException {
    CacheCreation cache = new CacheCreation();
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setValueConstraint(String.class);

    cache.createRegion("root", attrs);

    testXml(cache);
  }

}
