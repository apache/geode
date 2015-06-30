/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.session.catalina;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleState;

import java.io.IOException;

public class Tomcat7DeltaSessionManager extends DeltaSessionManager {

  /**
   * Prepare for the beginning of active use of the public methods of this
   * component.  This method should be called after <code>configure()</code>,
   * and before any of the public methods of the component are utilized.
   *
   * @throws LifecycleException if this component detects a fatal error that
   *                            prevents this component from being used
   */
  @Override
  public void startInternal() throws LifecycleException {
    super.startInternal();
    if (getLogger().isDebugEnabled()) {
      getLogger().debug(this + ": Starting");
    }
    if (this.started.get()) {
      return;
    }

    this.lifecycle.fireLifecycleEvent(START_EVENT, null);

    // Register our various valves
    registerJvmRouteBinderValve();

    if (isCommitValveEnabled()) {
      registerCommitSessionValve();
    }

    // Initialize the appropriate session cache interface
    initializeSessionCache();

    try {
      load();
    } catch (ClassNotFoundException e) {
      throw new LifecycleException("Exception starting manager", e);
    } catch (IOException e) {
      throw new LifecycleException("Exception starting manager", e);
    }

    // Create the timer and schedule tasks
    scheduleTimerTasks();

    this.started.set(true);
    this.setState(LifecycleState.STARTING);
  }

  /**
   * Gracefully terminate the active use of the public methods of this
   * component.  This method should be the last one called on a given instance
   * of this component.
   *
   * @throws LifecycleException if this component detects a fatal error that
   *                            needs to be reported
   */
  @Override
  public void stopInternal() throws LifecycleException {
    super.stopInternal();
    if (getLogger().isDebugEnabled()) {
      getLogger().debug(this + ": Stopping");
    }

    try {
      unload();
    } catch (IOException e) {
      getLogger().error("Unable to unload sessions", e);
    }

    this.started.set(false);
    this.lifecycle.fireLifecycleEvent(STOP_EVENT, null);

    // StandardManager expires all Sessions here.
    // All Sessions are not known by this Manager.

    super.destroyInternal();

    // Clear any sessions to be touched
    getSessionsToTouch().clear();

    // Cancel the timer
    cancelTimer();

    // Unregister the JVM route valve
    unregisterJvmRouteBinderValve();

    if (isCommitValveEnabled()) {
      unregisterCommitSessionValve();
    }

    this.setState(LifecycleState.STOPPING);
  }

}
