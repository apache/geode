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
package org.apache.geode.cache30;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheRuntimeException;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;

/**
 * A helper class that provides the {@link SerializableRunnable} class, but uses a {@link #run2}
 * method instead that throws {@link CacheException}. This way, we don't need to have a lot of
 * try/catch code in the tests.
 *
 * @since GemFire 3.0
 */
public abstract class CacheSerializableRunnable extends SerializableRunnable {

  /**
   * Creates a new {@code CacheSerializableRunnable} with the given name
   */
  public CacheSerializableRunnable() {
    // super
  }

  /**
   * Creates a new {@code CacheSerializableRunnable} with the given name
   *
   * @deprecated Please pass name as the first argument to {@link VM} invoke or asyncInvoke.
   */
  @Deprecated
  public CacheSerializableRunnable(String name) {
    super(name);
  }

  /**
   * Invokes the {@link #run2} method and will wrap any {@link CacheException} thrown by
   * {@code run2} in a {@link CacheSerializableRunnableException}.
   */
  @Override
  public void run() {
    try {
      run2();
    } catch (CacheException exception) {
      String message = "While invoking \"" + this + "\"";
      throw new CacheSerializableRunnableException(message, exception);
    }
  }

  /**
   * A {@link SerializableRunnable#run run} method that may throw a {@link CacheException}.
   */
  public abstract void run2() throws CacheException;

  /**
   * An exception that wraps a {@link CacheException}
   */
  public static class CacheSerializableRunnableException extends CacheRuntimeException {

    public CacheSerializableRunnableException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
