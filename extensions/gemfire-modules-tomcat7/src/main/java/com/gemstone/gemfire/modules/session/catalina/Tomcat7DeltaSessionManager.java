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
package com.gemstone.gemfire.modules.session.catalina;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleState;

import java.io.IOException;

public class Tomcat7DeltaSessionManager extends DeltaSessionManager {

  /**
   * Prepare for the beginning of active use of the public methods of this component.  This method should be called
   * after <code>configure()</code>, and before any of the public methods of the component are utilized.
   *
   * @throws LifecycleException if this component detects a fatal error that prevents this component from being used
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
   * Gracefully terminate the active use of the public methods of this component.  This method should be the last one
   * called on a given instance of this component.
   *
   * @throws LifecycleException if this component detects a fatal error that needs to be reported
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
