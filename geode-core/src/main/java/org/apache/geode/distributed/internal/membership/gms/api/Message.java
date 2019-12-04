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
package org.apache.geode.distributed.internal.membership.gms.api;

import java.util.Collection;
import java.util.List;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.serialization.DataSerializableFixedID;

/**
 * The membership module is capable of sending messages to other members of the
 * cluster. Those messages must implement this interface and must be serializable
 * using DataSerializableFixedID from the geode-serialization module.
 * <p>
 * There are currently two implemementation hierarchies of Message.
 * One is AbstractGMSMessage, which is used in the membership-module for membership-
 * related messages. The other is DistributionMessage, which is the base class for
 * messages in geode-core and other higher-level modules.
 */

public interface Message extends DataSerializableFixedID {

  /**
   * Indicates that a distribution message should be sent to all other distribution managers.
   */
  @Immutable
  MemberIdentifier ALL_RECIPIENTS = null;

  /**
   * Establishes the destination of a message
   */
  void setRecipient(MemberIdentifier member);

  /**
   * Establishes one or more destinations of a message
   */
  void setRecipients(Collection recipients);

  /**
   * is this a high priority message that should be sent out-of-band?
   */
  boolean isHighPriority();


  /**
   * Register any reply processor prior to transmission, if necessary. The "processor"
   * is a ReplyProcessor that has an identifier that is serialized with the message being
   * sent. The identifier is used to match up any reply message with the ReplyProcessor
   * that is waiting for the response.
   */
  void registerProcessor();

  /**
   * Returns the recipients of a message
   */
  List<MemberIdentifier> getRecipients();

  /**
   * is this message intended for all members of the cluster? (note: this does not send
   * the message to the node initiating the message)
   */
  boolean forAll();

  /**
   * If multicast is enabled, should this message be sent to all members using multicast?
   * The default implementation returns false.
   */
  default boolean getMulticast() {
    return false;
  }

  /**
   * If multicast is enabled and the message implementation allows it, send this message
   * over multicast to all members of the cluster.
   */
  default void setMulticast(boolean useMulticast) {
    // no-op by default
  }

  /** establishes the sender of a message on the receiving side of a communications channel */
  void setSender(MemberIdentifier sender);

  /**
   * Returns the sender of this message. Note that this may be null in the member that
   * constructs a message to be sent to other members
   */
  MemberIdentifier getSender();

  /**
   * In messages supporting it, this resets the message's millisecond-clock timestamp
   */
  long resetTimestamp();

  /**
   * In messages supporting it, this establishes the number of bytes read from the
   * communications transport when receiving a message.
   */
  void setBytesRead(int amount);

  /**
   * In messages supporting it, this returns the number of bytes read from the
   * communications transport when receiving a message.
   */
  int getBytesRead();
}
