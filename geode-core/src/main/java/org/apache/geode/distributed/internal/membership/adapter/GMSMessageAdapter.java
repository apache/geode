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
package org.apache.geode.distributed.internal.membership.adapter;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.HighPriorityDistributionMessage;
import org.apache.geode.distributed.internal.OperationExecutors;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.gms.GMSMember;
import org.apache.geode.distributed.internal.membership.gms.interfaces.GMSMessage;
import org.apache.geode.internal.cache.DirectReplyMessage;
import org.apache.geode.internal.serialization.DataSerializableFixedID;

/**
 * GMSMessageAdapter wraps a Geode DistributionMessage to be sent via the GMS Messenger
 */

public class GMSMessageAdapter implements GMSMessage {
  DistributionMessage geodeMessage;

  public GMSMessageAdapter(DistributionMessage geodeMessage) {
    this.geodeMessage = geodeMessage;
  }

  @Override
  public void setRecipient(GMSMember member) {
    geodeMessage.setRecipient(new InternalDistributedMember(new GMSMemberAdapter(member)));
  }

  @Override
  public void setRecipients(List<GMSMember> recipients) {
    geodeMessage.setRecipients(
        recipients.stream().map(GMSMemberAdapter::new).map(InternalDistributedMember::new).collect(
            Collectors.toList()));
  }

  @Override
  public boolean isHighPriority() {
    return geodeMessage instanceof HighPriorityDistributionMessage ||
        geodeMessage.getProcessorType() == OperationExecutors.HIGH_PRIORITY_EXECUTOR;
  }

  @Override
  public void registerProcessor() {
    if (geodeMessage instanceof DirectReplyMessage) {
      ((DirectReplyMessage) geodeMessage).registerProcessor();
    }
  }

  @Override
  public List<GMSMember> getRecipients() {
    InternalDistributedMember[] recipients = geodeMessage.getRecipients();
    if (recipients == null
        || recipients.length == 1 && recipients[0] == DistributionMessage.ALL_RECIPIENTS) {
      return Collections.singletonList(null);
    }
    return Arrays.asList(recipients).stream()
        .map(recipient -> (GMSMemberAdapter) recipient.getNetMember())
        .map(GMSMemberAdapter::getGmsMember).collect(
            Collectors.toList());
  }

  @Override
  public int getDSFID() {
    return geodeMessage.getDSFID();
  }

  @Override
  public boolean forAll() {
    return geodeMessage.forAll();
  }

  @Override
  public boolean getMulticast() {
    return geodeMessage.getMulticast();
  }

  @Override
  public void setMulticast(boolean useMulticast) {
    geodeMessage.setMulticast(useMulticast);
  }

  @Override
  public void setSender(GMSMember sender) {
    geodeMessage.setSender(new InternalDistributedMember(new GMSMemberAdapter(sender)));
  }

  @Override
  public GMSMember getSender() {
    return ((GMSMemberAdapter) geodeMessage.getSender().getNetMember()).getGmsMember();
  }

  @Override
  public void resetTimestamp() {
    geodeMessage.resetTimestamp();
  }

  @Override
  public void setBytesRead(int amount) {
    geodeMessage.setBytesRead(amount);
  }

  @Override
  public String toString() {
    return geodeMessage.toString();
  }

  public DataSerializableFixedID getGeodeMessage() {
    return geodeMessage;
  }
}
