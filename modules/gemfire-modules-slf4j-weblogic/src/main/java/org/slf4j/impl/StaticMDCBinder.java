/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package org.slf4j.impl;

import org.slf4j.helpers.BasicMDCAdapter;
import org.slf4j.spi.MDCAdapter;

/**
 * This implementation is bound to BasicMDCAdapter.
 */
public class StaticMDCBinder {

  /**
   * The unique instance of this class.
   */
  public static final StaticMDCBinder SINGLETON = new StaticMDCBinder();

  private StaticMDCBinder() {
  }

  public MDCAdapter getMDCA() {
    return new BasicMDCAdapter();
  }

  public String getMDCAdapterClassStr() {
    return BasicMDCAdapter.class.getName();
  }
}
