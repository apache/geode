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

import java.util.Collection;

import org.apache.geode.distributed.internal.membership.gms.GMSMember;
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipView;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.internal.serialization.DataSerializableFixedID;

/**
 * Manager presents the GMS services to the outside world and handles startup/shutdown race
 * conditions. It is also the default MessageHandler
 */
public interface Manager extends Service, MessageHandler<GMSMessage> {

  /**
   * After all services have been started this is used to join the distributed system
   */
  void joinDistributedSystem();

  /**
   * initiates a Forced Disconnect, shutting down the distributed system and closing the cache
   *
   */
  void forceDisconnect(String reason);

  /**
   * notifies the manager that membership quorum has been lost
   */
  void quorumLost(Collection<GMSMember> failures, GMSMembershipView view);

  /**
   * sometimes we cannot perform multicast messaging, such as during a rolling upgrade.
   *
   * @return true if multicast messaging can be performed
   */
  boolean isMulticastAllowed();

  /**
   * Returns true if a shutdown is in progress or has been completed . When it returns true,
   * shutdown message is already sent.
   */
  boolean shutdownInProgress();

  /**
   * Returns true if a distributed system close is started. And shutdown msg has not sent yet,its in
   * progress.
   */
  boolean isShutdownStarted();

  /**
   * Indicate whether we are attempting a reconnect
   */
  boolean isReconnectingDS();

  /**
   * When Messenger receives a message from another node it may be in a form that
   * Messenger can't deal with, depending on what payload was serialized. It may
   * be a GMSMessage already or it may be a message wrapped in an adapter class
   * that serializes a non-GMSMessage payload. (See GMSMessageAdapter, which
   * wraps Geode DistributionMessages)
   */
  GMSMessage wrapMessage(Object receivedMessage);

  /**
   * When Messenger is going to transmit a message it gets the actual payload to serialize
   * from this method
   */
  DataSerializableFixedID unwrapMessage(GMSMessage messageToSend);

  /**
   * Return the Services object owning this Manager service
   */
  Services getServices();

}
