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
package org.apache.geode.internal.util;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A builder that caches the singleton value. 
 * 
 * The result is created and cached by the first thread that invokes the underlying
 * {@code Callable}.  Concurrent requests will block and be notified once the
 * originating request has completed.  Once the result has been cached, all 
 * subsequent invocations will return the cached result until it has been cleared.
 * When the value type acquires resources, it is recommended that the type implement
 * {@link Closeable}.  This will allow the resources to be freed if a temporary
 * value is created by a concurrent clear operation.  In all other cases the user
 * is responsible for clearing an resources held by the cached value.
 * <p>
 * The caching technique is most useful as a defense against a call that is invoked by
 * multiple threads but executes blocking operations serially.  Even when the call
 * supports timeouts, the serial execution penalty can cause "timeout stacking"
 * and thus unbounded delays on the invoking threads.
 * 
 *
 * @param <T> the type of the singleton
 */
public class SingletonValue<T extends Closeable> {
  /**
   * Constructs a singleton value.
   *
   * @param <T> the value type
   */
  public interface SingletonBuilder<T extends Closeable> {
    /**
     * Constructs the value.
     * 
     * @return the value
     * @throws IOException unable to construct the value
     */
    T create() throws IOException;
    
    /**
     * Invoked after the value is successfully created and stored.
     */
    void postCreate();
    
    /**
     * Invoked when a thread is waiting for the result of {@link #create}.
     */
    void createInProgress();
  }
  
  /** 
   * Defines the value state.  Allowable transitions are:
   * <ul>
   *  <li>{@code NOT_SET} -> {@code IN_PROGRESS}
   *  <li>{@code IN_PROGRESS} -> {@code NOT_SET}, {@code SET}, {@code CLEARED}
   *  <li>{@code SET} -> {@code NOT_SET}, {@code CLEARED}
   * </ul>
   */
  private enum ValueState { NOT_SET, IN_PROGRESS, SET, CLEARED }
  
  /** the delegated invocation */
  private final SingletonBuilder<T> builder;
  
  /** protects access to {@code state} and {@code value} */
  private final ReentrantLock sync;
  
  /** notified during state change */
  private final Condition change;
  
  /** the state of the cached value */
  private ValueState state;

  /** the originating thread, temporarily used during invocation */
  private Thread current;
  
  /** the cached value, null when not or cleared */
  private T value;
  
  /** the invocation error, null if successful or cleared */
  private IOException error;
  
  public SingletonValue(SingletonBuilder<T> builder) {
    this.builder = builder;
    
    state = ValueState.NOT_SET;
    sync = new ReentrantLock();
    change = sync.newCondition();
  }
  /**
   * Returns true if the value is cached.
   * @return true if cached
   */
  public boolean hasCachedValue() {
    sync.lock();
    try {
      return state == ValueState.SET;
    } finally {
      sync.unlock();
    }
  }
  
  /**
   * Returns true if the value is cached.
   * @return true if cached
   */
  public boolean isCleared() {
    sync.lock();
    try {
      return state == ValueState.CLEARED;
    } finally {
      sync.unlock();
    }
  }

  /**
   * Returns the cached result, or null if the value is not cached.
   * @return the cached result
   */
  public T getCachedValue() {
    sync.lock();
    try {
      return value;
    } finally {
      sync.unlock();
    }
  }
  
  /**
   * Clears the cached result.  Any threads waiting for the result will retry unless
   * {@code allowReset} is {@code false}.
   * 
   * @param allowReset true if the value is allowed to be cached subsequently
   * @return the old value, or null if not set.
   */
  public T clear(boolean allowReset) {
    sync.lock();
    try {
      // nothing to do
      if (state == ValueState.NOT_SET) {
        return null;
      }
      
      T result = value;
      ValueState old = state;
      
      // reset internal state
      state = allowReset ? ValueState.NOT_SET : ValueState.CLEARED;
      current = null;
      value = null;
      error = null;
      
      // interrupt waiting threads
      if (old == ValueState.IN_PROGRESS) {
        change.signalAll();
      }
      return result;
    } finally {
      sync.unlock();
    }
  }
  
  /**
   * Clears the cached value as long it matches the expected value.
   * 
   * @param expect the expected value
   * @param allowReset true if the value is allowed be be cached subsequently
   * @return true if the value was cleared
   */
  public boolean clear(T expect, boolean allowReset) {
    sync.lock();
    try {
      if (expect != value) {
        return false;
      }
      
      ValueState prev = state;
      clear(allowReset);
      return prev == ValueState.SET;
      
    } finally {
      sync.unlock();
    }
  }
  
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="UL_UNRELEASED_LOCK", 
      justification="findbugs is wrong and Darrel agrees")
  public T get() throws IOException {
    assert sync.getHoldCount() == 0;
    
    sync.lock();
    boolean doUnlock = true;
    try {
      switch (state) {
      case NOT_SET:
        assert current == null;
        current = Thread.currentThread();
        state = ValueState.IN_PROGRESS;
        
        doUnlock = false;
        sync.unlock();
        
        // invoke the task while NOT locked
        return acquireValue();
        
      case IN_PROGRESS:
        builder.createInProgress();
        while (state == ValueState.IN_PROGRESS) {
          try {
            change.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException();
          }
        }
  
        if (error != null) {
          throw error;
        }
        
        doUnlock = false;
        sync.unlock();

        // try again
        return get();
        
      case SET:
        return value;
        
      case CLEARED:
        throw new IOException("Value has been cleared and cannot be reset");
        
      default:
        throw new IllegalStateException("Unknown ValueState: " + state);
      }
    } finally {
      if (doUnlock) {
        sync.unlock();
      }
    }
  }
  
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="UL_UNRELEASED_LOCK", 
      justification="findbugs is wrong and Darrel agrees")
  private T acquireValue() throws IOException {
    T result = null;
    IOException err = null;
    try {
      // only one thread invokes this at a time (except possibly during a concurrent clear)
      result = builder.create();
    } catch (IOException e) {
      err = e;
    }
    
    // update the state and signal waiting threads
    sync.lock();
    boolean doUnlock = true;
    try {
      if (state != ValueState.IN_PROGRESS || current != Thread.currentThread()) {
        // oops, we got cleared and need to clean up the tmp value
        if (result != null) {
          try {
            result.close();
          } catch (IOException e) {
          }
        }
        
        sync.unlock();
        doUnlock = false;
        
        return get();
      }
      
      state = (err == null) ? ValueState.SET : ValueState.NOT_SET;
      current = null;
      value = result;
      error = err;
      
      try {
        builder.postCreate();
      } finally {
        change.signalAll();
      }
    } finally {
      if (doUnlock) {
        sync.unlock();
      }
    }
  
    // all done, return the result
    if (err != null) {
      throw err;
    }
    return result;
  }
}
