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
