/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.query.internal.Support;

/**
 * This class is intended to hold a single 'observer' which will receive
 * callbacks. There can be only one such observer at a time. If no observer is
 * needed, this member variable should point to an object with 'do-nothing'
 * methods, such as BridgeObserverAdapter.
 * 
 * @author Yogesh Mahajan
 * @since 5.1
 */
public class BridgeObserverHolder
  {

  /**
   * The default 'do-nothing' bridge observer *
   */
  private static final BridgeObserver NO_OBSERVER = new BridgeObserverAdapter();

  /**
   * The current observer which will be notified of all query events.
   */
  private static BridgeObserver _instance = NO_OBSERVER;

  /**
   * Set the given observer to be notified of events. Returns the current
   * observer.
   */
  public static final BridgeObserver setInstance(BridgeObserver observer)
  {
    Support.assertArg(observer != null,
        "setInstance expects a non-null argument!");
    BridgeObserver oldObserver = _instance;
    _instance = observer;
    return oldObserver;
  }

  /** Return the current BridgeObserver instance */
  public static final BridgeObserver getInstance()
  {
    return _instance;
  }

}
