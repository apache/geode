/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache30;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.xmlcache.*;

/**
 * Tests the declarative caching functionality introduced in GemFire
 * 4.0. 
 *
 * @author David Whitlock
 * @since 4.0
 */
public class CacheXml40DUnitTest extends CacheXml30DUnitTest {

  ////////  Constructors

  public CacheXml40DUnitTest(String name) {
    super(name);
  }

  ////////  Helper methods

  protected String getGemFireVersion() {
    return CacheXml.VERSION_4_0;
  }

  ////////  Test methods

  /**
   * Tests the cache server attribute
   *
   * @since 4.0
   */
  public void testServer() {
    CacheCreation cache = new CacheCreation();
    cache.setIsServer(true);
    assertTrue(cache.isServer());

    testXml(cache);
  }

  /**
   * Tests declarative bridge servers
   *
   * @since 4.0
   */
  public void testBridgeServers() {
    CacheCreation cache = new CacheCreation();

    CacheServer bridge1 = cache.addCacheServer();
    setBridgeAttributes(bridge1);
    CacheServer bridge2 = cache.addCacheServer();
    bridge2.setPort(AvailablePortHelper.getRandomAvailableTCPPort());

    testXml(cache);
  }

  /**
   * Used by testBridgeServers to set version specific attributes
   * @param bridge1 the bridge server to set attributes upon
   */
  public void setBridgeAttributes(CacheServer bridge1)
  {
    bridge1.setPort(AvailablePortHelper.getRandomAvailableTCPPort());
  }

  /**
   * Tests the is-lock-grantor attribute in xml.
   */
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
   * @since 4.0
   */
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
   * @since 4.0
   */
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
   * @since 4.1
   */
  public void testConstrainedValues() throws CacheException {
    CacheCreation cache = new CacheCreation();
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setValueConstraint(String.class);

    cache.createRegion("root", attrs);

    testXml(cache);
  }

}
