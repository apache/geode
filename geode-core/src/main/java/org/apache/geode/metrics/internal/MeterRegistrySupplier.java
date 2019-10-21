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
package org.apache.geode.metrics.internal;

import java.util.function.Supplier;

import io.micrometer.core.instrument.MeterRegistry;

import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;

public class MeterRegistrySupplier implements Supplier<MeterRegistry> {

  private final Supplier<InternalDistributedSystem> internalDistributedSystemSupplier;

  public MeterRegistrySupplier(
      Supplier<InternalDistributedSystem> internalDistributedSystemSupplier) {
    this.internalDistributedSystemSupplier = internalDistributedSystemSupplier;
  }

  @Override
  public MeterRegistry get() {
    InternalDistributedSystem system = internalDistributedSystemSupplier.get();
    if (system == null) {
      return null;
    }

    InternalCache internalCache = system.getCache();
    if (internalCache == null) {
      return null;
    }

    return internalCache.getMeterRegistry();
  }
}
