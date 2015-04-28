/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * Created on Sep 12, 2005
 *
 * 
 */
package com.gemstone.gemfire.internal.cache;

/**
 * This class is intended to hold a single 'observer' which will receive
 * callbacks from the Distributed/Local Region when events like clear take
 * place. There can be only one such observer at a time. If no observer is
 * needed, this member variable should point to an object with 'do-nothing'
 * methods, such as CacheObserverAdapter.
 * 
 * Code which wishes to observe events during Region clear should do so using
 * the following technique:
 * 
 * class MyCacheObserver extends CacheObserverAdapter { // ... override methods
 * of interest ... }
 * 
 * CacheObserver old = CacheObserverHolder.setInstance(new MyCacheObserver());
 * com.gemstone.gemfire.internal.cache.LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER=true;
 * 
 * try { Call region methods here } finally { // reset to the original
 * CacheObserver. CacheObserverHolder.setInstance(old); }
 * 
 * The Region code will call methods on this static member using the following
 * technique:
 * 
 * CacheObserver observer = CacheObserverHolder.getInstance(); try {
 * observer.startMethod(arguments); doSomething(); } finally {
 * observer.stopMethod(arguments); }
 * 
 * @author ashahid
 */
public class CacheObserverHolder  {

  /**
   * The default 'do-nothing' query observer *
   */
  private static final CacheObserver NO_OBSERVER = new CacheObserverAdapter();
  /**
   * The current observer which will be notified of all query events.
   */
  private static CacheObserver _instance = NO_OBSERVER;

  /**
   * Set the given observer to be notified of query events. Returns the current
   * observer.
   */
  public static final CacheObserver setInstance(CacheObserver observer) {
    if (observer == null) observer = NO_OBSERVER;
    CacheObserver oldObserver = _instance;
    _instance = observer;
    return oldObserver;
  }

  /** Return the current QueryObserver instance */
  public static final CacheObserver getInstance() {
    return _instance;
  }
}
