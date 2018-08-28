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

package org.apache.geode.distributed.internal.membership;

import org.apache.geode.distributed.internal.membership.gms.messenger.MembershipInformation;

/**
 * A QuorumChecker is created after a forced-disconnect in order to probe the network to see if
 * there is a quorum of members that can be contacted.
 *
 *
 */
public interface QuorumChecker {

  /**
   * Check to see if a quorum of the old members are reachable
   *
   * @param timeoutMS time to wait for responses, in milliseconds
   */
  boolean checkForQuorum(long timeoutMS) throws InterruptedException;

  /**
   * suspends the quorum checker for an attempt to connect to the distributed system
   */
  void suspend();

  /**
   * resumes the quorum checker after having invoked suspend();
   */
  void resume();

  /**
   * closes the quorum checker and releases resources. Use this if the distributed system is not
   * going to be reconnected and you want to release resources.
   */
  void close();

  /**
   * Get the membership info from the old system that needs to be passed to the one that is
   * reconnecting.
   */
  MembershipInformation getMembershipInfo();

  /**
   * Returns the membership view that is being used to establish a quorum
   */
  NetView getView();
}
