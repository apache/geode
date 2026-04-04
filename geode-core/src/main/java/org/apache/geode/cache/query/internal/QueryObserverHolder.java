/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.cache.query.internal;


import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.internal.MakeNotStatic;

/**
 * This class is intended to hold a single 'observer' which will receive callbacks from the query
 * subsystem when various events take place. There can be only one such observer at a time. If no
 * observer is needed, this member variable should point to an object with 'do-nothing' methods,
 * such as QueryObserverAdapter.
 *
 * Code which wishes to observe events during a query should do so using the following technique:
 *
 * class MyQueryObserver extends QueryObserverAdapter { // ... override methods of interest ... }
 *
 * QueryObserver old = QueryObserverHolder.setInstance(new MyQueryObserver()); try { //... call
 * query methods here ... } finally { // reset to the original QueryObserver.
 * QueryObserverHolder.setInstance(old); }
 *
 * The query code will call methods on this static member using the following technique:
 *
 * QueryObserver observer = QueryObserverHolder.getInstance(); try {
 * observer.startMethod(arguments); doSomething(); } finally { observer.stopMethod(arguments); }
 *
 * @version $Revision: 1.1 $
 */
public class QueryObserverHolder {

  /**
   * The default 'do-nothing' query observer *
   */
  @Immutable
  private static final QueryObserver NO_OBSERVER = new QueryObserverAdapter();
  /**
   * The threadlocal current observer which will be notified of all query events.
   */
  private static final ThreadLocal<QueryObserver> _instance = new ThreadLocal<>();

  /**
   * The current observer which will be notified of all query events.
   */
  @MakeNotStatic
  private static volatile QueryObserver _globalInstance = NO_OBSERVER;

  /**
   * Set the given observer to be notified of query events. Returns the current observer.
   */
  public static QueryObserver setInstance(QueryObserver observer) {
    Support.assertArg(observer != null, "setInstance expects a non-null argument!");
    QueryObserver oldObserver = _globalInstance;
    _instance.set(observer);
    _globalInstance = observer;
    return oldObserver;
  }

  public static boolean hasObserver() {
    if (_instance.get() != null) {
      return _instance.get() != NO_OBSERVER;
    }
    return _globalInstance != NO_OBSERVER;
  }

  /** Return the current QueryObserver instance */
  public static QueryObserver getInstance() {
    if (_instance.get() == null) {
      _instance.set(_globalInstance);
    }
    return _instance.get();
  }

  /**
   * Only for test purposes.
   */
  public static void reset() {
    _instance.set(NO_OBSERVER);
    _globalInstance = NO_OBSERVER;
  }
}
