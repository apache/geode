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
package com.gemstone.gemfire.internal.util;

import java.io.InterruptedIOException;
import java.util.concurrent.Callable;

/**
 * This class serializes execution of concurrent tasks. This is
 * useful when we want only one thread to perform an expensive
 * operation such that if the operation fails, all other threads
 * fail with the same exception without having to perform the
 * expensive operation.
 * 
 *
 * @param <T> the type of return value
 */
public class SingletonCallable<T> {
  private Object lock = new Object();
  private volatile boolean running = false;
  private Exception ex = null;

  public T runSerially(Callable<T> c) throws Exception {
    T retval = null;
    synchronized (lock) {
      while (running) {
        try {
          lock.wait();
          if (ex != null) {
            throw ex;
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new InterruptedIOException();
        }
      }
      ex = null;
      running = true;
    }

    Exception exception = null;
    try {
      retval = c.call();
    } catch (Exception e) {
      exception = e;
    } finally {
      synchronized (lock) {
        if (exception != null && !ignoreException(exception)) {
          ex = exception;
        }
        running = false;
        lock.notifyAll();
      }
    }

    if (exception != null) {
      throw exception;
    }
    return retval;
  }

  /**
   * Override this method for handling exception encountered while
   * running the Callable c.
   * @return false if we want the waiting threads to fail immediately<br/>
   * true if we want the waiting threads to invoke the callable
   * 
   */
  public boolean ignoreException(Exception e) {
    return false;
  }
}
