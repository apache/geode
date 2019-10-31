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
package org.apache.geode.distributed.internal.membership.gms.interfaces;


import org.apache.geode.distributed.internal.membership.gms.api.MemberIdentifier;

public interface HealthMonitor extends Service {

  /**
   * Note that this member has been contacted by the given member
   *
   */
  void contactedBy(MemberIdentifier sender);

  /**
   * initiate, asynchronously, suspicion that the member is no longer available
   *
   */
  void suspect(MemberIdentifier mbr, String reason);

  /**
   * Check on the health of the given member, initiating suspicion if it fails. Return true if the
   * member is found to be available, false if it isn't.
   *
   * @param reason the reason this check is being performed
   * @param initiateRemoval if the member should be removed if it is not available
   */
  boolean checkIfAvailable(MemberIdentifier mbr, String reason, boolean initiateRemoval);

  /**
   * Invoked by the Manager, this notifies the HealthMonitor that a ShutdownMessage has been
   * received from the given member
   */
  void memberShutdown(MemberIdentifier mbr, String reason);

  /**
   * Returns the failure detection port for this member, or -1 if there is no such port
   */
  int getFailureDetectionPort();

}
