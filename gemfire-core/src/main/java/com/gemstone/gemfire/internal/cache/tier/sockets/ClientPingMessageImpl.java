/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.tier.sockets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.tier.MessageType;

/**
 * Class <code>ClientPingMessageImpl</code> is a ping message that is
 * periodically placed in the <code>CacheClientProxy</code>'s queue to verify
 * the client connection is still alive.
 * 
 * @author Barry Oglesby
 * 
 * @since 6.6.2.x
 */
public final class ClientPingMessageImpl implements ClientMessage {
  
  private static final long serialVersionUID = 5423895238521508743L;

  /**
   * Default constructor.
   */
  public ClientPingMessageImpl() {
  }

  public Message getMessage(CacheClientProxy proxy, boolean notify) throws IOException {
    Version clientVersion = proxy.getVersion();
    Message message = null;
    if (clientVersion.compareTo(Version.GFE_6622) >= 0) {
      message = getGFEMessage();
    } else {
      throw new IOException(
          "Unsupported client version for server-to-client message creation: "
              + clientVersion);
    }
      
    return message;
  }
  
  protected Message getGFEMessage() throws IOException {
    Message message = new Message(0, Version.CURRENT);
    message.setMessageType(MessageType.SERVER_TO_CLIENT_PING);
    message.setTransactionId(0);
    return message;
  }

  public boolean shouldBeConflated() {
    return true;
  }

  public void toData(DataOutput out) throws IOException {
  }

  public int getDSFID() {
    return CLIENT_PING_MESSAGE_IMPL;
  }

  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
  }

  public EventID getEventId() {
    return null;
  }

  public String getRegionToConflate() {
    return "gemfire_reserved_region_name_for_client_ping";
  }

  public Object getKeyToConflate() {
    // This method can be called by HARegionQueue.
    // Use this to identify the message type.
    return "ping";
  }

  public Object getValueToConflate() {
    // This method can be called by HARegionQueue
    // Use this to identify the message type.
    return "ping";
  }

  public void setLatestValue(Object value){
    return;
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
