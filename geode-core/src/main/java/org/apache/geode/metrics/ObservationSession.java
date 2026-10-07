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

import io.micrometer.observation.ObservationRegistry;

import org.apache.geode.annotations.Experimental;

/**
 * A session that manages Micrometer Observation instrumentation for Geode.
 *
 * <p>
 * Experimental: Micrometer Observation is a new addition to Geode and the API may change.
 */
@Experimental("Micrometer Observation is a new addition to Geode and the API may change")
public interface ObservationSession {

  /**
   * Returns the registry used by this session to create observations.
   *
   * @return the observation registry
   */
  ObservationRegistry getObservationRegistry();
}
