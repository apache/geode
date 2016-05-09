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

import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.distributed.internal.membership.QuorumChecker;

public interface Messenger extends Service {
  /**
   * adds a handler for the given class/interface of messages
   */
  void addHandler(Class c, MessageHandler h);

  /**
   * sends an asynchronous message when the membership view may not have
   * been established.  Returns destinations that did not
   * receive the message due to no longer being in the view
   */
  Set<InternalDistributedMember> send(DistributionMessage m, NetView alternateView);

  /**
   * sends an asynchronous message.  Returns destinations that did not
   * receive the message due to no longer being in the view
   */
  Set<InternalDistributedMember> send(DistributionMessage m);

  /**
   * sends an asynchronous message.  Returns destinations that did not
   * receive the message due to no longer being in the view.  Does
   * not guarantee delivery of the message (no retransmissions)
   */
  Set<InternalDistributedMember> sendUnreliably(DistributionMessage m);

  /**
   * returns the endpoint ID for this member
   */
  InternalDistributedMember getMemberID();
  
  /**
   * retrieves the quorum checker that is used during auto-reconnect attempts
   */
  QuorumChecker getQuorumChecker();
  
  /**
   * test whether multicast is not only turned on but is working
   * @return true multicast is enabled and working
   */
  boolean testMulticast(long timeout) throws InterruptedException;

  /**
   * For the state-flush algorithm we need to be able to record
   * the state of outgoing messages to the given member.  If multicast
   * is being used for region operations we also need to record its
   * state.
   * 
   * @param member the target member
   * @param state messaging state is stored in this map
   * @param includeMulticast whether to record multicast state
   */
  void getMessageState(InternalDistributedMember member, Map state, boolean includeMulticast);
  
  /**
   * The flip-side of getMessageState, this method takes the state it recorded
   * and waits for messages from the given member to be received.
   * 
   * @param member the member flushing operations to this member
   * @param state the state of that member's outgoing messaging to this member
   */
  void waitForMessageState(InternalDistributedMember member, Map state) throws InterruptedException;
}
