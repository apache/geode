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
package org.apache.geode.distributed.internal.membership.gms.messenger;

import java.util.Queue;

import org.jgroups.JChannel;
import org.jgroups.Message;

import org.apache.geode.distributed.internal.membership.api.MembershipInformation;

/**
 * Class MembershipInformationImpl is used to pass membership data from a GMS that was
 * kicked out of the cluster to a new one during auto-reconnect operations.
 */
public class MembershipInformationImpl implements MembershipInformation {
  private final JChannel channel;
  private final Queue<Message> queuedMessages;
  private final GMSEncrypt encrypt;

  protected MembershipInformationImpl(JChannel channel,
      Queue<Message> queuedMessages,
      GMSEncrypt encrypt) {

    this.channel = channel;
    this.queuedMessages = queuedMessages;
    this.encrypt = encrypt;
  }


  public GMSEncrypt getEncrypt() {
    return encrypt;
  }

  public JChannel getChannel() {
    return channel;
  }

  Queue<Message> getQueuedMessages() {
    return queuedMessages;
  }
}
