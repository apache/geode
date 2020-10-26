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
package org.apache.geode.util.internal;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Collection of utilities for changing simple Java util structures to a state of completion or
 * default.
 *
 * <p>
 * All utilities herein throw checked exceptions wrapped within a runtime exception.
 */
public class CompletionUtils {

  /**
   * Closes the {@code AutoCloseable}.
   */
  public static void close(AutoCloseable autoCloseable) {
    try {
      autoCloseable.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Opens the {@code CountDownLatch} by counting it down.
   */
  public static void close(CountDownLatch countDownLatch) {
    while (countDownLatch.getCount() > 0) {
      countDownLatch.countDown();
    }
  }

  /**
   * Sets the {@code AtomicBoolean} to false.
   */
  public static void close(AtomicBoolean atomicBoolean) {
    atomicBoolean.set(false);
  }

  /**
   * Sets the {@code AtomicReference} to null.
   */
  public static void close(AtomicReference<?> atomicReference) {
    atomicReference.set(null);
  }
}
