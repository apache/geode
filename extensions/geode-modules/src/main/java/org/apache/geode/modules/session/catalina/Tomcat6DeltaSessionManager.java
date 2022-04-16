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
package org.apache.geode.modules.session.catalina;

import org.apache.catalina.LifecycleListener;
import org.apache.catalina.util.LifecycleSupport;

/**
 * @deprecated Tomcat 6 has reached its end of life and support for Tomcat 6 will be removed
 *             from a future Geode release.
 */
@Deprecated
public class Tomcat6DeltaSessionManager extends DeltaSessionManager<Tomcat6CommitSessionValve> {

  /**
   * The <code>LifecycleSupport</code> for this component.
   */
  private final LifecycleSupport lifecycle = new LifecycleSupport(this);

  /**
   * Prepare for the beginning of active use of the public methods of this component. This method
   * should be called after <code>configure()</code>, and before any of the public methods of the
   * component are utilized.
   *
   */
  @Override
  public synchronized void start() {
    if (getLogger().isDebugEnabled()) {
      getLogger().debug(this + ": Starting");
    }
    if (started.get()) {
      return;
    }
    lifecycle.fireLifecycleEvent(START_EVENT, null);
    try {
      init();
    } catch (Throwable t) {
      getLogger().error(t.getMessage(), t);
    }

    // Register our various valves
    registerJvmRouteBinderValve();

    if (isCommitValveEnabled()) {
      registerCommitSessionValve();
    }

    // Initialize the appropriate session cache interface
    initializeSessionCache();

    // Create the timer and schedule tasks
    scheduleTimerTasks();

    started.set(true);
  }

  /**
   * Gracefully terminate the active use of the public methods of this component. This method should
   * be the last one called on a given instance of this component.
   *
   */
  @Override
  public synchronized void stop() {
    if (getLogger().isDebugEnabled()) {
      getLogger().debug(this + ": Stopping");
    }
    started.set(false);
    lifecycle.fireLifecycleEvent(STOP_EVENT, null);

    // StandardManager expires all Sessions here.
    // All Sessions are not known by this Manager.

    // Require a new random number generator if we are restarted
    random = null;

    // Remove from RMI registry
    if (initialized) {
      destroy();
    }

    // Clear any sessions to be touched
    getSessionsToTouch().clear();

    // Cancel the timer
    cancelTimer();

    // Unregister the JVM route valve
    unregisterJvmRouteBinderValve();

    if (isCommitValveEnabled()) {
      unregisterCommitSessionValve();
    }
  }

  /**
   * Add a lifecycle event listener to this component.
   *
   * @param listener The listener to add
   */
  @Override
  public void addLifecycleListener(LifecycleListener listener) {
    lifecycle.addLifecycleListener(listener);
  }

  /**
   * Get the lifecycle listeners associated with this lifecycle. If this Lifecycle has no listeners
   * registered, a zero-length array is returned.
   */
  @Override
  public LifecycleListener[] findLifecycleListeners() {
    return lifecycle.findLifecycleListeners();
  }

  /**
   * Remove a lifecycle event listener from this component.
   *
   * @param listener The listener to remove
   */
  @Override
  public void removeLifecycleListener(LifecycleListener listener) {
    lifecycle.removeLifecycleListener(listener);
  }

  @Override
  protected Tomcat6CommitSessionValve createCommitSessionValve() {
    return new Tomcat6CommitSessionValve();
  }
}
