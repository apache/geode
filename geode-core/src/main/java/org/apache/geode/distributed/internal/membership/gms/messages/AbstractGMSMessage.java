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

import java.util.Collections;
import java.util.List;

import org.apache.geode.distributed.internal.membership.gms.GMSMember;
import org.apache.geode.distributed.internal.membership.gms.interfaces.GMSMessage;
import org.apache.geode.internal.DataSerializableFixedID;

public abstract class AbstractGMSMessage implements DataSerializableFixedID, GMSMessage {
  private List<GMSMember> recipients;
  private GMSMember sender;

  @Override
  public void registerProcessor() {
    // no-op
  }

  @Override
  public boolean isHighPriority() {
    return true;
  }

  @Override
  public void setRecipient(GMSMember member) {
    recipients = Collections.singletonList(member);
  }

  @Override
  public void setRecipients(List<GMSMember> recipients) {
    this.recipients = recipients;
  }

  @Override
  public List<GMSMember> getRecipients() {
    return recipients;
  }

  @Override
  public void setSender(GMSMember sender) {
    this.sender = sender;
  }

  @Override
  public GMSMember getSender() {
    return sender;
  }

  String getShortClassName() {
    return getClass().getSimpleName();
  }

}
