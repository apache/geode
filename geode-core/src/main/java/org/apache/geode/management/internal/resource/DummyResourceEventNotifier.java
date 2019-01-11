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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * No-op implementation of {@link ResourceEventNotifier}.
 */
public class DummyResourceEventNotifier implements ResourceEventNotifier {

  DummyResourceEventNotifier() {
    // nothing
  }

  @Override
  public void addResourceEventListener(ResourceEventListener listener) {
    // nothing
  }

  @Override
  public Set<ResourceEventListener> getResourceEventListeners() {
    return Collections.emptySet();
  }

  @Override
  public void handleResourceEvent(ResourceEvent event, Object resource) {}

  @Override
  public Future<Void> handleResourceEventAsync(ResourceEvent event, Object resource) {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public void close() {
    // nothing
  }
}
