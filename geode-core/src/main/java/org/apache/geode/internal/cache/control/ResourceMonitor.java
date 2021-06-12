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
package org.apache.geode.internal.cache.control;

import java.util.Set;

import org.apache.geode.internal.cache.control.ResourceAdvisor.ResourceManagerProfile;

/**
 * Implemented by classes that the ResourceManager creates in order to monitor a specific type of
 * resource (heap memory, off-heap memory, disk, etc.).
 *
 * @since Geode 1.0
 */
interface ResourceMonitor {

  /**
   * Ask the monitor to notify the given listeners of the given event.
   *
   * @param listeners Set of listeners of notify.
   * @param event Event to send to the listeners.
   */
  void notifyListeners(final Set<ResourceListener<?>> listeners,
      final ResourceEvent event);

  /**
   * Ask the monitor to stop monitoring.
   */
  void stopMonitoring();

  /**
   * Populate the fields in the profile that are appropriate for this monitor.
   *
   * @param profile The profile to populate.
   */
  void fillInProfile(final ResourceManagerProfile profile);
}
