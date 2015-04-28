/*
 * =========================================================================
 * Copyright (c) 2011-2014 Pivotal Software, Inc. All Rights Reserved. This product is
 * protected by U.S. and international copyright and intellectual property laws.
 * Pivotal products are covered by one or more patents listed at
 * http://www.pivotal.io/patents.
 * =========================================================================
 */
package com.gemstone.gemfire.internal.cache.vmotion;

import com.gemstone.gemfire.cache.query.internal.Support;

/**
 * This class is intended to hold a single 'observer' which will receive
 * callbacks. There can be only one such observer at a time. If no observer is
 * needed, this member variable should point to an object with 'do-nothing'
 * methods, such as vMotionObserverAdapter.
 */

public class VMotionObserverHolder {

  /**
   * The default 'do-nothing' vMotion observer *
   */
  public static final VMotionObserver NO_OBSERVER = new VMotionObserverAdapter();

  /**
   * The current observer which will be notified of all query events.
   */
  private static VMotionObserver _instance = NO_OBSERVER;

  /**
   * Set the given observer to be notified of events. Returns the current
   * observer.
   */
  public static final VMotionObserver setInstance(VMotionObserver observer) {
    Support.assertArg(observer != null,
        "setInstance expects a non-null argument!");
    VMotionObserver oldObserver = _instance;
    _instance = observer;
    return oldObserver;
  }

  /** Return the current VMotionObserver instance */
  public static final VMotionObserver getInstance() {
    return _instance;
  }

}
