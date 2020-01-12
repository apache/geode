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
package org.apache.geode.internal.util.concurrent;

import org.apache.geode.CancelCriterion;

/**
 * This is a type of {@link StoppableReentrantLock} that does not allow recursion
 *
 */
public class StoppableNonReentrantLock extends StoppableReentrantLock {
  /**
   * Creates an instance.
   *
   * @param stopper the cancellation object
   */
  public StoppableNonReentrantLock(CancelCriterion stopper) {
    super(stopper);
  }

  /**
   * Creates an instance with the given fairness policy.
   *
   * @param fair <code>true</code> if this lock should use a fair ordering policy
   * @param stopper the cancellation object
   */
  public StoppableNonReentrantLock(boolean fair, CancelCriterion stopper) {
    super(fair, stopper);
  }

  /**
   * @throws IllegalStateException if reentry is detected
   */
  private void checkForRentry() {
    if (isHeldByCurrentThread()) {
      throw new IllegalStateException(
          "Lock reentry is not allowed");
    }
  }

  /**
   * @throws IllegalStateException if the lock is already held by the current thread
   */
  @Override
  public void lock() {
    checkForRentry();
    super.lock();
  }

  /**
   * @throws IllegalStateException if the lock is already held by the current thread
   */
  @Override
  public void lockInterruptibly() throws InterruptedException {
    checkForRentry();
    super.lockInterruptibly();
  }

  /**
   * @throws IllegalStateException if the lock is already held by the current thread
   */
  @Override
  public boolean tryLock() {
    checkForRentry();
    return super.tryLock();
  }

  /**
   * @throws IllegalStateException if the lock is already held by the current thread
   */
  @Override
  public boolean tryLock(long timeoutMs) throws InterruptedException {
    checkForRentry();
    return super.tryLock(timeoutMs);
  }
}
