/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.cache;

import org.apache.geode.cache.query.internal.Support;

/**
 * This class is intended to hold a single 'observer' which will receive
 * callbacks. There can be only one such observer at a time. If no observer is
 * needed, this member variable should point to an object with 'do-nothing'
 * methods, such as ClientServerObserverAdapter.
 * 
 * @since GemFire 5.1
 */
public class ClientServerObserverHolder
  {

  /**
   * The default 'do-nothing' bridge observer *
   */
  private static final ClientServerObserver NO_OBSERVER = new ClientServerObserverAdapter();

  /**
   * The current observer which will be notified of all query events.
   */
  private static ClientServerObserver _instance = NO_OBSERVER;

  /**
   * Set the given observer to be notified of events. Returns the current
   * observer.
   */
  public static final ClientServerObserver setInstance(ClientServerObserver observer)
  {
    Support.assertArg(observer != null,
        "setInstance expects a non-null argument!");
    ClientServerObserver oldObserver = _instance;
    _instance = observer;
    return oldObserver;
  }

  /** Return the current BridgeObserver instance */
  public static final ClientServerObserver getInstance()
  {
    return _instance;
  }

}
