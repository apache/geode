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
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.TestingOnly;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.ManagementException;
import org.apache.geode.security.GemFireSecurityException;

public class QueuedResourceEventNotifier implements ResourceEventNotifier {
  private static final Logger logger = LogService.getLogger();

  private final ExecutorService executorService;
  private final Set<ResourceEventListener> resourceListeners;

  private boolean running = true;

  QueuedResourceEventNotifier() {
    executorService = Executors.newCachedThreadPool();
    resourceListeners = new CopyOnWriteArraySet<>();
  }

  @Override
  public void addResourceEventListener(ResourceEventListener listener) {
    resourceListeners.add(listener);
  }

  @Override
  public Set<ResourceEventListener> getResourceEventListeners() {
    return Collections.unmodifiableSet(resourceListeners);
  }

  @Override
  public void handleResourceEvent(ResourceEvent event, Object resource) {
    if (false) {
      notifyResourceEventListeners(event, resource);
    } else {
      try {
        handleResourceEventAsync(event, resource).get();

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof RuntimeException) {
          throw (RuntimeException) cause;
        } else if (cause instanceof Error) {
          throw (Error) cause;
        } else {
          throw new RuntimeException("Exception while handling ResourceEvent " + event, cause);
        }
      }
    }
  }

  @Override
  public synchronized Future<Void> handleResourceEventAsync(ResourceEvent event, Object resource) {
    if (running) {
      return CompletableFuture.runAsync(() -> notifyResourceEventListeners(event, resource),
          executorService);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public synchronized void close() {
    running = false;
    resourceListeners.clear();
    shutdown();
  }

  @TestingOnly
  List<Runnable> shutdown() {
    return executorService.shutdownNow();
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
