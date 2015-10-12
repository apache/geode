/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.partitioned;

import com.gemstone.gemfire.cache.query.internal.Support;

/**
 * This class is intended to hold a single 'observer' which will receive
 * callbacks. There can be only one such observer at a time. If no observer is
 * needed, this member variable should point to an object with 'do-nothing'
 * methods, such as PartitionedRegionObserverAdapter.
 * 
 * @author Kishor Bachhav
 */

public class PartitionedRegionObserverHolder {


  /**
   * The default 'do-nothing' bridge observer *
   */
  private static final PartitionedRegionObserver NO_OBSERVER = new PartitionedRegionObserverAdapter();

  /**
   * The current observer which will be notified of all query events.
   */
  private static PartitionedRegionObserver _instance = NO_OBSERVER;

  /**
   * Set the given observer to be notified of events. Returns the current
   * observer.
   */
  public static final PartitionedRegionObserver setInstance(PartitionedRegionObserver observer)
  {
    Support.assertArg(observer != null,
        "setInstance expects a non-null argument!");
    PartitionedRegionObserver oldObserver = _instance;
    _instance = observer;
    return oldObserver;
  }

  /** Return the current ClientServerObserver instance */
  public static final PartitionedRegionObserver getInstance()
  {
    return _instance;
  }


}
