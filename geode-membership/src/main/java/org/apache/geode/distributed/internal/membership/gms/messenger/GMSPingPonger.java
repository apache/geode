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

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;

import org.apache.geode.internal.serialization.KnownVersion;

/**
 * GMSPingPonger is used to detect whether a member exists by sending UDP Ping and Pong
 * messages. A member can send a Ping message to another member and receive a Pong message
 * in response. GMSPingPonger is largely used to detect whether a quorum of the cluster
 * is reachable.
 */
public class GMSPingPonger {
  private final byte[] pingInBytes = new byte[] {'p', 'i', 'n', 'g'};
  private final byte[] pongInBytes = new byte[] {'p', 'o', 'n', 'g'};

  public boolean isPingMessage(byte[] buffer) {
    return buffer.length == 4
        && (buffer[0] == 'p' && buffer[1] == 'i' && buffer[2] == 'n' && buffer[3] == 'g');
  }

  public boolean isPongMessage(byte[] buffer) {
    return buffer.length == 4
        && (buffer[0] == 'p' && buffer[1] == 'o' && buffer[2] == 'n' && buffer[3] == 'g');
  }

  public void sendPongMessage(JChannel channel, Address src, Address dest) throws Exception {
    channel.send(createPongMessage(src, dest));
  }

  public Message createPongMessage(Address src, Address dest) {
    return createJGMessage(pongInBytes, src, dest,
        KnownVersion.getCurrentVersion().ordinal());
  }

  public Message createPingMessage(Address src, Address dest) {
    return createJGMessage(pingInBytes, src, dest,
        KnownVersion.getCurrentVersion().ordinal());
  }

  public void sendPingMessage(JChannel channel, Address src, JGAddress dest) throws Exception {
    channel.send(createPingMessage(src, dest));
  }

  private Message createJGMessage(byte[] msgBytes, Address src, Address dest, short version) {
    Message msg = new Message();
    msg.setDest(dest);
    msg.setSrc(src);
    msg.setObject(msgBytes);
    msg.setFlag(Message.Flag.NO_RELIABILITY);
    msg.setFlag(Message.Flag.NO_FC);
    msg.setFlag(Message.Flag.DONT_BUNDLE);
    msg.setFlag(Message.Flag.OOB);
    return msg;
  }

}
