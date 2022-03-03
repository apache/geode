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

import org.apache.geode.cache.query.CqState;

/**
 * Offers methods to get CQ state.
 *
 * @since GemFire 5.5
 */

public class CqStateImpl implements CqState {

  public static final int STOPPED = 0;

  public static final int RUNNING = 1;

  public static final int CLOSED = 2;

  public static final int CLOSING = 3;

  public static final int INIT = 4;

  private volatile int state = INIT;


  /**
   * Returns true if the CQ is in Running state.
   */
  @Override
  public boolean isRunning() {
    return (state == RUNNING);
  }

  /**
   * Returns true if the CQ is in Stopped or Initializing state.
   */
  @Override
  public boolean isStopped() {
    return (state == STOPPED || state == INIT);
  }

  /**
   * Returns true if the CQ is in Closed state.
   */
  @Override
  public boolean isClosed() {
    return (state == CLOSED);
  }

  /**
   * Returns true if the CQ is in the Closing state.
   */
  @Override
  public boolean isClosing() {
    return (state == CLOSING);
  }

  /**
   * Sets the state of CQ.
   *
   */
  public void setState(int state) {
    this.state = state;
  }

  /**
   * Returns the integer state of CQ.
   */
  public int getState() {
    return state;
  }

  /**
   * Returns the state in string form.
   */
  @Override
  public String toString() {
    switch (state) {
      case STOPPED:
        return "STOPPED";
      case RUNNING:
        return "RUNNING";
      case CLOSED:
        return "CLOSED";
      default:
        return "UNKNOWN";
    }
  }

}
