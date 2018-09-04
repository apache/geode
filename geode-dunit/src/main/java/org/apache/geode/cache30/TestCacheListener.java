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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.geode.SystemFailure;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.RegionEvent;

/**
 * A <code>CacheListener</code> used in testing. Its callback methods are implemented to thrown
 * {@link UnsupportedOperationException} unless the user overrides the "2" methods.
 *
 * @see #wasInvoked
 *
 * @since GemFire 3.0
 */
public abstract class TestCacheListener<K, V> extends TestCacheCallback
    implements CacheListener<K, V> {
  private final Object lock = new Object();
  private List<CacheEvent<K, V>> eventHistory = null;

  /**
   * Should be called for every event delivered to this listener
   */
  private void addEvent(CacheEvent<K, V> e, boolean setInvoked) {
    synchronized (lock) {
      if (setInvoked) {
        this.invoked = true;
      }
      if (this.eventHistory != null) {
        this.eventHistory.add(e);
      }
    }
  }

  private void addEvent(CacheEvent<K, V> e) {
    addEvent(e, true);
  }

  /**
   * Enables collection of event history.
   *
   * @since GemFire 5.0
   */
  public void enableEventHistory() {
    synchronized (lock) {
      if (this.eventHistory == null) {
        this.eventHistory = new ArrayList<>();
      }
    }
  }

  /**
   * Disables collection of events.
   *
   * @since GemFire 5.0
   */
  public void disableEventHistory() {
    synchronized (lock) {
      this.eventHistory = null;
    }
  }

  /**
   * Returns a copy of the list of events collected in this listener's history. Also clears the
   * current history.
   *
   * @since GemFire 5.0
   */
  public List<CacheEvent<K, V>> getEventHistory() {
    synchronized (lock) {
      if (this.eventHistory == null) {
        return Collections.emptyList();
      } else {
        List<CacheEvent<K, V>> result = this.eventHistory;
        this.eventHistory = new ArrayList<>();
        return result;
      }
    }
  }

  @Override
  public void afterCreate(EntryEvent<K, V> event) {
    addEvent(event);
    try {
      afterCreate2(event);
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      this.callbackError = t;
    }
  }

  public void afterCreate2(EntryEvent<K, V> event) {
    String s = "Unexpected callback invocation";
    throw new UnsupportedOperationException(s);
  }

  @Override
  public void afterUpdate(EntryEvent<K, V> event) {
    addEvent(event);
    try {
      afterUpdate2(event);
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      this.callbackError = t;
    }
  }

  public void afterUpdate2(EntryEvent<K, V> event) {
    String s = "Unexpected callback invocation";
    throw new UnsupportedOperationException(s);
  }

  @Override
  public void afterInvalidate(EntryEvent<K, V> event) {
    addEvent(event);
    try {
      afterInvalidate2(event);
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      this.callbackError = t;
    }
  }

  public void afterInvalidate2(EntryEvent<K, V> event) {
    String s = "Unexpected callback invocation";
    throw new UnsupportedOperationException(s);
  }

  @Override
  public void afterDestroy(EntryEvent<K, V> event) {
    afterDestroyBeforeAddEvent(event);
    addEvent(event);
    try {
      afterDestroy2(event);
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      this.callbackError = t;
    }
  }

  public void afterDestroyBeforeAddEvent(EntryEvent<K, V> event) {
    // do nothing by default
  }

  public void afterDestroy2(EntryEvent<K, V> event) {
    String s = "Unexpected callback invocation";
    throw new UnsupportedOperationException(s);
  }

  @Override
  public void afterRegionInvalidate(RegionEvent<K, V> event) {
    addEvent(event);
    try {
      afterRegionInvalidate2(event);
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      this.callbackError = t;
    }
  }

  public void afterRegionInvalidate2(RegionEvent<K, V> event) {
    String s = "Unexpected callback invocation";
    throw new UnsupportedOperationException(s);
  }

  @Override
  public void afterRegionDestroy(RegionEvent<K, V> event) {
    // check argument to see if this is during tearDown
    if ("teardown".equals(event.getCallbackArgument()))
      return;
    afterRegionDestroyBeforeAddEvent(event);
    addEvent(event);
    try {
      afterRegionDestroy2(event);
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      this.callbackError = t;
    }
  }

  public void afterRegionDestroyBeforeAddEvent(RegionEvent<K, V> ignored) {}

  public void afterRegionDestroy2(RegionEvent<K, V> event) {
    if (!event.getOperation().isClose()) {
      String s = "Unexpected callback invocation";
      throw new UnsupportedOperationException(s);
    }
  }

  public void afterRegionClear(RegionEvent<K, V> event) {
    addEvent(event, false);
  }

  public void afterRegionCreate(RegionEvent<K, V> event) {
    addEvent(event, false);
  }

  public void afterRegionLive(RegionEvent<K, V> event) {
    addEvent(event, false);
  }
}
