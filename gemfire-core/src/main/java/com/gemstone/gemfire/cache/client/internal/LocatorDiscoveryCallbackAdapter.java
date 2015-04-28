/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import java.util.List;

/**
 * A locator discovery callback that does nothing.
 * @author dsmith
 * @since 5.7
 *
 */
public class LocatorDiscoveryCallbackAdapter implements
    LocatorDiscoveryCallback {

  public void locatorsDiscovered(List locators) {
  }

  public void locatorsRemoved(List locators) {
  }

}
