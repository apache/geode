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
/*
 * File: ConditionVariable.java Originally written by Doug Lea and released into the public domain.
 * This may be used for any purposes whatsoever without acknowledgment. Thanks for the assistance
 * and support of Sun Microsystems Labs, and everyone contributing, testing, and using this code.
 * History: Date Who What 11Jun1998 dl Create public version
 */

package org.apache.geode.internal.util.concurrent;


import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import org.apache.geode.CancelCriterion;
import org.apache.geode.internal.Assert;

/**
 * This class is functionally equivalent to {@link java.util.concurrent.locks.Condition}; however it
 * only implements the acquire(long) method. Its purpose is to perform a cancellation check
 */
public class StoppableCondition implements java.io.Serializable {
  private static final long serialVersionUID = -7091681525970431937L;

  /** The underlying condition **/
  private final Condition condition;

  /** The cancellation object */
  private final CancelCriterion stopper;

  public static final long TIME_TO_WAIT = 15000;

  /**
   * Create a new StoppableCondition based on given condition and cancellation criterion
   *
   * @param c the underlying condition
   **/
  StoppableCondition(Condition c, CancelCriterion stopper) {
    Assert.assertTrue(stopper != null);
    condition = c;
    this.stopper = stopper;
  }

  public boolean await(long timeoutMs) throws InterruptedException {
    stopper.checkCancelInProgress(null);
    return condition.await(timeoutMs, TimeUnit.MILLISECONDS);
  }

  public synchronized void signal() {
    condition.signal();
  }

  public synchronized void signalAll() {
    condition.signalAll();
  }
}
