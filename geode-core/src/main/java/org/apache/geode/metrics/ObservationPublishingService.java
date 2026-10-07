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
package org.apache.geode.metrics;

import java.util.ServiceLoader;

import io.micrometer.observation.ObservationHandler;

import org.apache.geode.annotations.Experimental;

/**
 * Configures observation publishing when an {@link ObservationSession} starts.
 *
 * <p>
 * Geode discovers {@code ObservationPublishingService}s during system creation, using the standard
 * Java {@link ServiceLoader} mechanism.
 *
 * <p>
 * A typical implementation registers {@link ObservationHandler}s, predicates, filters, or
 * conventions with {@link ObservationSession#getObservationRegistry()}.
 *
 * <p>
 * Experimental: Micrometer Observation is a new addition to Geode and the API may change.
 */
@Experimental("Micrometer Observation is a new addition to Geode and the API may change")
public interface ObservationPublishingService {

  /**
   * Invoked when an observation session starts.
   *
   * @param session the observation session to configure
   */
  void start(ObservationSession session);

  /**
   * Invoked when an observation session stops.
   *
   * @param session the observation session this publishing service configured
   */
  void stop(ObservationSession session);
}
