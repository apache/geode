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
package org.apache.geode.cache;

import java.util.Collections;
import java.util.Set;

/**
 * Indicates that an attempt to notify required participants of a transaction involving one or more
 * regions that are configured with {@link MembershipAttributes} may have failed. The commit itself
 * was completed but one or more regions affected by the transaction have one or more required roles
 * that were not successfully notified. Failure may be caused by departure of one or more required
 * roles while sending the operation to them. This exception will contain one
 * {@link RegionDistributionException} for every region that had a reliability failure. Details of
 * the failed {@link MembershipAttributes#getRequiredRoles required roles} are provided in each
 * RegionDistributionException.
 *
 * @since GemFire 5.0
 */
public class CommitDistributionException extends TransactionException {
  private static final long serialVersionUID = -3517820638706581823L;
  /**
   * The RegionDistributionExceptions for every region that had a reliability failure during
   * distribution of the operation.
   */
  private Set regionDistributionExceptions = Collections.EMPTY_SET;

  /**
   * Constructs a <code>CommitDistributionException</code> with a message.
   *
   * @param s the String message
   */
  public CommitDistributionException(String s) {
    super(s);
  }

  /**
   * Constructs a <code>CommitDistributionException</code> with a message and a cause.
   *
   * @param s the String message
   * @param regionDistributionExceptions set of RegionDistributionExceptions for each region that
   *        had a reliability failure
   */
  public CommitDistributionException(String s, Set regionDistributionExceptions) {
    super(s);
    this.regionDistributionExceptions = regionDistributionExceptions;
    if (this.regionDistributionExceptions == null) {
      this.regionDistributionExceptions = Collections.EMPTY_SET;
    }
  }

  /**
   * Returns set of RegionDistributionExceptions for each region that had a reliability failure
   * during distribution of the operation.
   *
   * @return set of RegionDistributionExceptions for each region that had a reliability failure
   *         during distribution of the operation
   */
  public Set getRegionDistributionExceptions() {
    return regionDistributionExceptions;
  }

}
