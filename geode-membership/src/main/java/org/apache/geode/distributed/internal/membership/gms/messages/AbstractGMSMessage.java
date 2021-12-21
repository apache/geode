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
package org.apache.geode.distributed.internal.membership.gms.messages;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.api.Message;

/**
 * AbstractGMSMessage is the superclass of most Membership messages. These messages
 * may be sent via the Messenger service or via a TcpClient.
 *
 * @param <ID>
 */
public abstract class AbstractGMSMessage<ID extends MemberIdentifier> implements Message<ID> {
  private List<ID> recipients;
  private ID sender;

  @Override
  public void registerProcessor() {
    // no-op
  }

  @Override
  public boolean isHighPriority() {
    return true;
  }

  @Override
  public void setRecipient(ID member) {
    recipients = Collections.singletonList(member);
  }

  @Override
  public void setRecipients(Collection<ID> recipients) {
    if (recipients instanceof List) {
      this.recipients = (List<ID>) recipients;
    } else {
      this.recipients = new ArrayList<>(recipients);
    }
  }

  @Override
  public List<ID> getRecipients() {
    if (getMulticast()) {
      return (List<ID>) Collections.singletonList(ALL_RECIPIENTS);
    } else if (recipients != null) {
      return recipients;
    } else {
      return (List<ID>) Collections.singletonList(ALL_RECIPIENTS);
    }
  }

  @Override
  public boolean forAll() {
    if (getMulticast()) {
      return true;
    }
    List<ID> recipients = getRecipients();
    return recipients == ALL_RECIPIENTS ||
        (recipients.size() == 1 && recipients.get(0) == ALL_RECIPIENTS);
  }

  @Override
  public void setSender(ID sender) {
    this.sender = sender;
  }

  @Override
  public ID getSender() {
    return sender;
  }

  String getShortClassName() {
    return getClass().getSimpleName();
  }

  @Override
  public long resetTimestamp() {
    return 0;
  }

  @Override
  public int getBytesRead() {
    return 0;
  }

  @Override
  public void setBytesRead(int amount) {
    // no-op in GMS messages
  }
}
