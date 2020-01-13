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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.api.Message;
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipView;
import org.apache.geode.distributed.internal.membership.gms.messenger.GMSQuorumChecker;

/**
 * The Messenger service fulfills the role of message sending/receiving for a Membership.
 * It must provide both reliable and unreliable (connectionless) messaging. MessageHandlers
 * may be installed to inform a Messenger of who should consume messages it receives. The default
 * handler of messages received by a Messenger is the Manager service.
 */
public interface Messenger<ID extends MemberIdentifier> extends Service<ID> {
  /**
   * adds a handler for the given class/interface of messages
   */
  <T extends Message<ID>> void addHandler(Class<T> c, MessageHandler<T> h);

  /**
   * sends an asynchronous message when the membership view may not have been established. Returns
   * destinations that did not receive the message due to no longer being in the view
   */
  Set<ID> send(Message<ID> m, GMSMembershipView<ID> alternateView);

  /**
   * sends an asynchronous message. Returns destinations that did not receive the message due to no
   * longer being in the view
   */
  Set<ID> send(Message<ID> m);

  /**
   * sends an asynchronous message. Returns destinations that did not receive the message due to no
   * longer being in the view. Does not guarantee delivery of the message (no retransmissions)
   */
  Set<ID> sendUnreliably(Message<ID> m);

  /**
   * returns the endpoint ID for this member
   */
  ID getMemberID();

  /**
   * retrieves the quorum checker that is used during auto-reconnect attempts
   */
  GMSQuorumChecker<ID> getQuorumChecker();

  /**
   * test whether multicast is not only turned on but is working
   *
   * @return true multicast is enabled and working
   */
  boolean testMulticast(long timeout) throws InterruptedException;

  /**
   * For the state-flush algorithm we need to be able to record the state of outgoing messages to
   * the given member. If multicast is being used for region operations we also need to record its
   * state.
   *
   * @param member the target member
   * @param state messaging state is stored in this map
   * @param includeMulticast whether to record multicast state
   */
  void getMessageState(ID member, Map<String, Long> state,
      boolean includeMulticast);

  /**
   * The flip-side of getMessageState, this method takes the state it recorded and waits for
   * messages from the given member to be received.
   *
   * @param member the member flushing operations to this member
   * @param state the state of that member's outgoing messaging to this member
   */
  void waitForMessageState(ID member, Map<String, Long> state)
      throws InterruptedException, TimeoutException;

  /**
   * Get the public key of member.
   *
   * @return byte[] public key for member
   */
  byte[] getPublicKey(ID mbr);

  /**
   * Set public key of member.
   *
   */

  void setPublicKey(byte[] publickey, ID mbr);

  /**
   * Set cluster key in local member.Memebr calls when it gets cluster key in join response
   *
   */
  void setClusterSecretKey(byte[] clusterSecretKey);

  /**
   * To retrieve the cluster key. This needs to send cluster key to new memebr.
   *
   * @return byte[] cluster key
   */
  byte[] getClusterSecretKey();

  /**
   * To set requestId in request. This requestId comes back in response to match the request.
   *
   * @return int request id
   */
  int getRequestId();

  /**
   * Initialize the cluster key, this happens when member becomes coordinator.
   */
  void initClusterKey();
}
