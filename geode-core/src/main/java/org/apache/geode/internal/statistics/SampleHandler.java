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
package org.apache.geode.internal.statistics;

import java.util.List;

/**
 * Defines the operations required to handle statistics samples and receive notifications of
 * ResourceTypes and ResourceInstances.
 *
 * @since GemFire 7.0
 */
public interface SampleHandler {

  /**
   * Notification that a statistics sample has occurred.
   * <p/>
   * The timeStamp is an arbitrary nanoseconds time stamp only used for archiving to stats files.
   * The initial time in system milliseconds and the first NanoTimer timeStamp are both written to
   * the archive file. Thereafter only the NanoTimer timeStamp is written to the archive file for
   * each sample. The delta in nanos can be determined by comparing any nano timeStamp to the first
   * nano timeStamp written to the archive file. Adding this delta to the recorded initial time in
   * milliseconds provides the actual (non-arbitrary) time for each sample.
   *
   * @param nanosTimeStamp an arbitrary nanoseconds time stamp for this sample
   * @param resourceInstances the statistics resource instances captured in this sample
   */
  void sampled(long nanosTimeStamp, List<ResourceInstance> resourceInstances);

  /**
   * Notification that a new statistics {@link ResourceType} has been created.
   *
   * @param resourceType the new statistics ResourceType that was created
   */
  void allocatedResourceType(ResourceType resourceType);

  /**
   * Notification that a new statistics {@link ResourceInstance} has been created.
   *
   * @param resourceInstance the new statistics ResourceInstance that was created
   */
  void allocatedResourceInstance(ResourceInstance resourceInstance);

  /**
   * Notification that an existing statistics {@link ResourceInstance} has been destroyed.
   *
   * @param resourceInstance the existing statistics ResourceInstance that was destroyed
   */
  void destroyedResourceInstance(ResourceInstance resourceInstance);
}
