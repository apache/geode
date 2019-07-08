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

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.geode.distributed.internal.membership.NetMember;
import org.apache.geode.distributed.internal.membership.NetMessage;
import org.apache.geode.distributed.internal.membership.gms.GMSMember;
import org.apache.geode.internal.DataSerializableFixedID;

public abstract class GMSMessage implements DataSerializableFixedID, NetMessage {
  private List<GMSMember> recipients;
  private GMSMember sender;

  @Override
  public boolean isHighPriority() {
    return true;
  }

  public void setRecipient(GMSMember member) {
    recipients = Collections.singletonList(member);
  }

  public void setRecipients(List<GMSMember> recipients) {
    this.recipients = recipients;
  }

  public Collection<GMSMember> getRecipients() {
    return recipients;
  }

  @Override
  public List<NetMember> getNetRecipients() {
    return (List<NetMember>) (List<?>) recipients;
  }

  @Override
  public void setNetSender(NetMember netSender) {
    setSender((GMSMember) netSender);
  }

  @Override
  public NetMember getNetSender() {
    return sender;
  }

  public void setSender(GMSMember sender) {
    this.sender = sender;
  }

  public GMSMember getSender() {
    return sender;
  }

  String getShortClassName() {
    return getClass().getSimpleName();
  }
}
