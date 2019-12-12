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
package org.apache.geode.distributed.internal;

import org.apache.geode.CancelCriterion;
import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;

/**
 * Resource which uses a {@link DistributionAdvisor}.
 *
 * @since GemFire 5.7
 */
public interface DistributionAdvisee {

  /**
   * Returns the underlying {@code DistributionManager} used for messaging.
   *
   * @return the underlying {@code DistributionManager}
   */
  DistributionManager getDistributionManager();

  /**
   * @return the cancellation object for the advisee
   */
  CancelCriterion getCancelCriterion();

  /**
   * Returns the {@code DistributionAdvisor} that provides advice for this advisee.
   *
   * @return the {@code DistributionAdvisor}
   */
  DistributionAdvisor getDistributionAdvisor();

  /**
   * Returns the {@code Profile} representing this member in the
   * {@code DistributionAdvisor}.
   *
   * @return the {@code Profile} representing this member
   */
  Profile getProfile();

  /**
   * Returns this advisee's parent or null if no parent exists.
   *
   * @return parent advisee
   */
  DistributionAdvisee getParentAdvisee();

  /**
   * Returns the underlying {@code InternalDistributedSystem} connection.
   *
   * @return the underlying {@code InternalDistributedSystem}
   */
  InternalDistributedSystem getSystem();

  /**
   * Returns the simple name of this resource.
   *
   * @return the simple name of this resource
   */
  String getName();

  /**
   * Returns the full path of this resource.
   *
   * @return the full path of this resource.
   */
  String getFullPath();

  /**
   * Fill in the {@code Profile} with details from the advisee.
   *
   * @param profile the {@code Profile} to fill in with details
   */
  void fillInProfile(Profile profile);

  /**
   * @return the serial number which identifies the static order in which this region was created in
   *         relation to other regions or other instances of this region during the life of this
   *         JVM.
   */
  int getSerialNumber();

}
