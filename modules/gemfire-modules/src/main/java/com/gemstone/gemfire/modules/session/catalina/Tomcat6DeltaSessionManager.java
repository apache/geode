/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.session.catalina;

import org.apache.catalina.LifecycleException;

public class Tomcat6DeltaSessionManager extends DeltaSessionManager {

  /**
   * Prepare for the beginning of active use of the public methods of this
   * component.  This method should be called after <code>configure()</code>,
   * and before any of the public methods of the component are utilized.
   *
   * @exception LifecycleException if this component detects a fatal error
   *  that prevents this component from being used
   */ 
  @Override
  public void start() throws LifecycleException {
    if (getLogger().isDebugEnabled()) {
      getLogger().debug(this + ": Starting");
    }
    if (this.started.get()) {
      return;
    }
    this.lifecycle.fireLifecycleEvent(START_EVENT, null);
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

    this.started.set(true);
  }

  /**
   * Gracefully terminate the active use of the public methods of this
   * component.  This method should be the last one called on a given
   * instance of this component.
   *
   * @exception LifecycleException if this component detects a fatal error
   *  that needs to be reported
   */
  @Override
  public void stop() throws LifecycleException {
    if (getLogger().isDebugEnabled()) {
      getLogger().debug(this + ": Stopping");
    }
    this.started.set(false);
    this.lifecycle.fireLifecycleEvent(STOP_EVENT, null);

    // StandardManager expires all Sessions here.
    // All Sessions are not known by this Manager.
    
    // Require a new random number generator if we are restarted
    this.random = null;

    // Remove from RMI registry
    if (this.initialized) {
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
}
