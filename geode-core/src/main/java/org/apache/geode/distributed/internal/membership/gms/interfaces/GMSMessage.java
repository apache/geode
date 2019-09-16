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

import java.util.List;

import org.apache.geode.distributed.internal.membership.gms.GMSMember;

public interface GMSMessage {

  void setRecipient(GMSMember member);

  void setRecipients(List<GMSMember> recipients);

  /** is this a high priority message that should be sent out-of-band? */
  boolean isHighPriority();


  /** register any reply processor prior to transmission, if necessary */
  void registerProcessor();

  List<GMSMember> getRecipients();

  /** from DataSerializableFixedID */
  int getDSFID();

  boolean forAll();

  default boolean getMulticast() {
    return false;
  }

  default void setMulticast(boolean useMulticast) {
    // no-op by default
  }

  /** establishes the sender of a message on the receiving side of a communications channel */
  void setSender(GMSMember sender);

  GMSMember getSender();

  void resetTimestamp();

  void setBytesRead(int amount);
}
