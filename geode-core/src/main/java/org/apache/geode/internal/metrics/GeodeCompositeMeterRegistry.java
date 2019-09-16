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

import static java.util.Arrays.asList;

import java.util.Collection;

import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.internal.logging.LogService;

public class GeodeCompositeMeterRegistry extends CompositeMeterRegistry {
  private final Logger logger;
  private final Collection<AutoCloseable> ownedResources;

  GeodeCompositeMeterRegistry(AutoCloseable... ownedResources) {
    this(LogService.getLogger(), ownedResources);
  }

  @VisibleForTesting
  GeodeCompositeMeterRegistry(Logger logger, AutoCloseable... ownedResources) {
    super();
    this.logger = logger;
    this.ownedResources = asList(ownedResources);
  }

  @Override
  public void close() {
    ownedResources.forEach(this::closeResource);
    super.close();
  }

  private void closeResource(AutoCloseable resource) {
    try {
      resource.close();
    } catch (Exception thrown) {
      logger.warn("Exception while closing resource " + resource, thrown);
    }
  }
}
