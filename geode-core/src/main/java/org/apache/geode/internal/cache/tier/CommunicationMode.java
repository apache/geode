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
package org.apache.geode.internal.cache.tier;

/**
 * The following are communications "mode" bytes sent as the first byte of a client/server
 * handshake. They must not be larger than 1 byte.
 */
public enum CommunicationMode {
  /**
   * The first byte of any locator connection will be the high order byte of its gossip version,
   * which will always be 0. Communication modes should not collide with this value.
   */
  ReservedForGossip((byte) 0, "Locator gossip version"),
  /**
   * Byte meaning that the Socket is being used for 'client to server' communication.
   */
  ClientToServer((byte) 100, "client"),
  /**
   * Byte meaning that the Socket is being used for 'primary server to client' communication.
   */
  PrimaryServerToClient((byte) 101, "primary server to client"),
  /**
   * Byte meaning that the Socket is being used for 'secondary server to client' communication.
   */
  SecondaryServerToClient((byte) 102, "secondary server to client"),
  /**
   * Byte meaning that the Socket is being used for 'gateway to gateway' communication.
   */
  GatewayToGateway((byte) 103, "gateway"),
  /**
   * Byte meaning that the Socket is being used for 'monitor to gateway' communication.
   */
  MonitorToServer((byte) 104, "monitor"),
  /**
   * Byte meaning that the connection between the server and client was successful. This is not
   * actually a communication mode but an acknowledgement byte.
   */
  SuccessfulServerToClient((byte) 105, "successful server to client"),
  /**
   * Byte meaning that the connection between the server and client was unsuccessful. This is not
   * actually a communication mode but an error byte.
   */
  UnsuccessfulServerToClient((byte) 106, "unsucessful server to client"),
  /**
   * Byte meaning that the Socket is being used for 'client to server' messages related to a client
   * queue (register interest, create cq, etc.).
   */
  ClientToServerForQueue((byte) 107, "clientToServerForQueue");

  /**
   * is this a client-initiated operations connection?
   */
  public boolean isClientOperations() {
    return this == ClientToServer;
  }

  /**
   * is this any type of client/server connection?
   */
  public boolean isClientToServerOrSubscriptionFeed() {
    return this == ClientToServer || this == PrimaryServerToClient
        || this == SecondaryServerToClient || this == ClientToServerForQueue;
  }

  /**
   * be the first to describe this method
   */
  public boolean isSubscriptionFeed() {
    return this == PrimaryServerToClient || this == SecondaryServerToClient;
  }

  /**
   * is this connection counted in the ClientServerCnxCount statistic?
   */
  public boolean isCountedAsClientServerConnection() {
    return this == ClientToServer || this == MonitorToServer;
  }

  /**
   * is this a WAN connection?
   */
  public boolean isWAN() {
    return this == GatewayToGateway;
  }

  /**
   * The modeNumber is the byte written on-wire that indicates this connection mode
   */
  private final byte modeNumber;

  private final String description;

  CommunicationMode(byte mode, String description) {
    modeNumber = mode;
    this.description = description;
  }

  public byte getModeNumber() {
    return modeNumber;
  }

  /**
   * check the given mode to see if it is assigned to one of the enumeration's instances
   */
  public static boolean isValidMode(int mode) {
    return (100 <= mode && mode <= 107) || mode == 10;
  }

  public static CommunicationMode fromModeNumber(byte modeNumber) {
    switch (modeNumber) {
      case 100:
        return ClientToServer;
      case 101:
        return PrimaryServerToClient;
      case 102:
        return SecondaryServerToClient;
      case 103:
        return GatewayToGateway;
      case 104:
        return MonitorToServer;
      case 105:
        return SuccessfulServerToClient;
      case 106:
        return UnsuccessfulServerToClient;
      case 107:
        return ClientToServerForQueue;
      default:
        throw new IllegalArgumentException("unknown communications mode: " + modeNumber);
    }
  }

  public String toString() {
    return description;
  }

}
