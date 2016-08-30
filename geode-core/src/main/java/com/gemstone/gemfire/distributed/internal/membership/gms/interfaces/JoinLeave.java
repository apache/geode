/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.distributed.internal.membership.gms.interfaces;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.NetMember;
import com.gemstone.gemfire.distributed.internal.membership.NetView;

public interface JoinLeave extends Service {

  /**
   * joins the distributed system and returns true if successful, false if not.
   * Throws SystemConnectException and GemFireConfigException
   */
  boolean join();

  /**
   * leaves the distributed system.  Should be invoked before stop()
   */
  void leave();

  /**
   * force another member out of the system
   */
  void remove(InternalDistributedMember m, String reason);
  
  /**
   * Invoked by the Manager, this notifies the HealthMonitor that a
   * ShutdownMessage has been received from the given member
   */
  public void memberShutdown(DistributedMember mbr, String reason);
  
  /**
   * returns the local address
   */
  InternalDistributedMember getMemberID();
  
  /**
   * Get "InternalDistributedMember" from current view or prepared view.
   */
  InternalDistributedMember getMemberID(NetMember m);
  
  /**
   * returns the current membership view
   */
  NetView getView();
  
  
  /**
   * returns the last known view prior to close - for reconnecting
   */
  NetView getPreviousView();
  
  /**
   * check to see if a member is already in the process of leaving or
   * being removed (in the next view)
   */
  boolean isMemberLeaving(DistributedMember mbr);
  
  /**
   * test hook
   */
  void disableDisconnectOnQuorumLossForTesting();
}
