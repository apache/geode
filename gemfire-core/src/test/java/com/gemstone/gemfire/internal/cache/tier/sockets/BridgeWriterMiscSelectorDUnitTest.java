/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;


/**
 * Just like BridgeWriterMiscDUnitTest but enables bridge thread pool
 * (ie. selector)
 *
 * @author darrel
 *
 */
public class BridgeWriterMiscSelectorDUnitTest extends BridgeWriterMiscDUnitTest
{
  public BridgeWriterMiscSelectorDUnitTest(String name) {
    super(name);
  }

  protected int getMaxThreads() {
    return 2; 
  }
}
