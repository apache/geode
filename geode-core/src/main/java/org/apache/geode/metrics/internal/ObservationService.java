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

import io.micrometer.observation.ObservationRegistry;

import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.metrics.ObservationSession;

/**
 * An observation session that can be started and stopped, and that manages an observation registry.
 */
public interface ObservationService extends ObservationSession {

  /**
   * Starts this observation service and loads configured publishing services.
   */
  void start();

  /**
   * Stops this observation service, freeing any resources.
   */
  void stop();

  /**
   * Returns this service's observation registry.
   *
   * @return the observation registry
   */
  ObservationRegistry getObservationRegistry();

  /**
   * Returns the builder that built this observation service. The builder can be used during
   * reconnect to create an observation service configured similarly to this one.
   *
   * @return the builder that built this observation service
   */
  Builder getRebuilder();

  interface Builder {
    /**
     * Informs this builder whether it is building an observation service on behalf of a client.
     *
     * @return this builder
     */
    Builder setIsClient(boolean isClient);

    /**
     * Builds an observation service associated with the given system.
     */
    ObservationService build(InternalDistributedSystem system);
  }
}
