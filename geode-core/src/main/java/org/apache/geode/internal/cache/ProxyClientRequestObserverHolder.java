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
package org.apache.geode.internal.cache;

import java.util.concurrent.atomic.AtomicReference;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.internal.MakeNotStatic;

/**
 * This class is intended to hold a single 'observer' which will receive callbacks from the
 * Distributed/Local Region when events like clear take place. There can be only one such observer
 * at a time. If no observer is needed, this member variable should point to an object with
 * 'do-nothing' methods, such as PutGetMessageObserverAdapter.
 *
 * Code which wishes to observe events during Region clear should do so using the following
 * technique:
 *
 * class MyPutGetMessageObserver extends PutGetMessageObserverAdapter { // ... override methods of
 * interest ... }
 *
 * PutGetMessageObserver old = PutGetMessageObserverHolder.setInstance(new
 * MyPutGetMessageObserver());
 * org.apache.geode.internal.cache.LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER=true;
 *
 * try { Call region methods here } finally { // reset to the original PutGetMessageObserver.
 * PutGetMessageObserverHolder.setInstance(old); }
 *
 * The Region code will call methods on this static member using the following technique:
 *
 * PutGetMessageObserver observer = PutGetMessageObserverHolder.getInstance(); try {
 * observer.startMethod(arguments); doSomething(); } finally { observer.stopMethod(arguments); }
 *
 */
public class ProxyClientRequestObserverHolder {

  /**
   * The default 'do-nothing' observer *
   */
  @Immutable
  private static final ProxyClientRequestObserver NO_OBSERVER =
      new ProxyClientRequestObserverAdapter();
  /**
   * The current observer which will be notified of all query events.
   */
  @MakeNotStatic
  private static final AtomicReference<ProxyClientRequestObserver> _instance =
      new AtomicReference<>(NO_OBSERVER);

  /**
   * Set the given observer to be notified of query events. Returns the current observer.
   */
  public static ProxyClientRequestObserver setInstance(
      @NotNull ProxyClientRequestObserver observer) {
    return _instance.getAndSet(observer);
  }

  /** Return the current observer instance */
  public static ProxyClientRequestObserver getInstance() {
    return _instance.get();
  }
}
