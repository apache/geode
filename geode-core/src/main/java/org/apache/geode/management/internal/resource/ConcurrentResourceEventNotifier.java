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
package org.apache.geode.management.internal.resource;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.ManagementException;
import org.apache.geode.security.GemFireSecurityException;

/**
 * This is an implementation of {@link ResourceEventNotifier} that handles resource events in the
 * thread that triggers them, except that a cache create or destroy stops other events from
 * happening concurrently.
 */
public class ConcurrentResourceEventNotifier implements ResourceEventNotifier {
  private static final Logger logger = LogService.getLogger();

  private final Set<ResourceEventListener> resourceListeners;

  ConcurrentResourceEventNotifier() {
    resourceListeners = Collections.synchronizedSet(new HashSet<>());
  }

  @Override
  public void addResourceListener(ResourceEventListener listener) {
    resourceListeners.add(listener);
  }

  @Override
  public Set<ResourceEventListener> getResourceListeners() {
    return Collections.unmodifiableSet(resourceListeners);
  }

  @Override
  public void handleResourceEvent(ResourceEvent event, Object resource) {
    if (resourceListeners.size() == 0) {
      return;
    }
    notifyResourceEventListeners(event, resource);
  }

  @Override
  public void close() {
    resourceListeners.clear();
  }

  /**
   * Notifies all resource event listeners. All exceptions are caught here and only a warning
   * message is printed in the log
   *
   * @param event Enumeration depicting particular resource event
   * @param resource the actual resource object.
   */
  private void notifyResourceEventListeners(ResourceEvent event, Object resource) {
    for (ResourceEventListener listener : resourceListeners) {
      try {
        listener.handleEvent(event, resource);
      } catch (GemFireSecurityException | ManagementException ex) {
        if (event == ResourceEvent.CACHE_CREATE) {
          throw ex;
        } else {
          logger.warn(ex.getMessage(), ex);
        }
      }
    }
  }
}
