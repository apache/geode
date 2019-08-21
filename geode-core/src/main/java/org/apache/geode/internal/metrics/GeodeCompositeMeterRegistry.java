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
package org.apache.geode.internal.metrics;

import java.util.Set;

import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

public class GeodeCompositeMeterRegistry extends CompositeMeterRegistry {

  private final Set<MeterBinder> binders;

  public GeodeCompositeMeterRegistry(Set<MeterBinder> binders) {
    super();
    this.binders = binders;
  }

  void registerBinders() {
    for (MeterBinder binder : binders) {
      binder.bindTo(this);
    }
  }

  @Override
  public void close() {
    for (MeterBinder binder : binders) {
      if (binder instanceof AutoCloseable) {
        AutoCloseable autoCloseable = (AutoCloseable) binder;
        try {
          autoCloseable.close();
        } catch (Exception ignored) {
          // Shutting down anyway.
        }
      }
    }

    super.close();
  }
}
