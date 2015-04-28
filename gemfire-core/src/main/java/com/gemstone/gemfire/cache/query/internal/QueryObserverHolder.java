/*=========================================================================
 * Copyright Copyright (c) 2000-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * $Id: QueryObserverHolder.java,v 1.1 2005/01/27 06:26:33 vaibhav Exp $
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.internal;

/**
 * This class is intended to hold a single 'observer' which will receive
 * callbacks from the query subsystem when various events take place. There can
 * be only one such observer at a time. If no observer is needed, this member
 * variable should point to an object with 'do-nothing' methods, such as
 * QueryObserverAdapter.
 * 
 * Code which wishes to observe events during a query should do so using the
 * following technique:
 * 
 * class MyQueryObserver extends QueryObserverAdapter { // ... override methods
 * of interest ... }
 * 
 * QueryObserver old = QueryObserverHolder.setInstance(new MyQueryObserver());
 * try { //... call query methods here ... } finally { // reset to the original
 * QueryObserver. QueryObserverHolder.setInstance(old); }
 * 
 * The query code will call methods on this static member using the following
 * technique:
 * 
 * QueryObserver observer = QueryObserverHolder.getInstance(); try {
 * observer.startMethod(arguments); doSomething(); } finally {
 * observer.stopMethod(arguments); }
 * 
 * @version $Revision: 1.1 $
 * @author derekf
 */
public final class QueryObserverHolder  {

  /**
   * The default 'do-nothing' query observer *
   */
  private static final QueryObserver NO_OBSERVER = new QueryObserverAdapter();
  /**
   * The current observer which will be notified of all query events.
   */
  private static QueryObserver _instance = NO_OBSERVER;

  /**
   * Set the given observer to be notified of query events. Returns the current
   * observer.
   */
  public static final QueryObserver setInstance(QueryObserver observer) {
    Support.assertArg(observer != null,
        "setInstance expects a non-null argument!");
    QueryObserver oldObserver = _instance;
    _instance = observer;
    return oldObserver;
  }
  
  public static boolean hasObserver() {
    return _instance != NO_OBSERVER;
  }

  /** Return the current QueryObserver instance */
  public static final QueryObserver getInstance() {
    return _instance;
  }

  /**
   * Only for test purposes.
   */
  public static final void reset() {
    _instance = NO_OBSERVER;
  }
}
