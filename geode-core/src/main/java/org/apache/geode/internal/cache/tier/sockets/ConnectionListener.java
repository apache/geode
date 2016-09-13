/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

/**
 * A listener which can be registered on {@link AcceptorImpl} 
 * in order to receive events about connections created
 * or destroyed for this acceptor.
 * @since GemFire 5.7
 *
 */
public interface ConnectionListener {
  /**
   * Indicates that a new connection has been opened
   * to this acceptor.
   * @param firstConnection true if this is the first connection
   * from this client.
   * @param communicationMode the communication mode of this
   * connection.
   */
  void connectionOpened(boolean firstConnection, byte communicationMode);
  /**
   * Indicates that the a connection to this acceptor has been
   * closed.
   * @param lastConnection indicates that this was the last
   * connection from this client.
   * @param communicationMode of this connection. 
   */
  void connectionClosed(boolean lastConnection, byte communicationMode);
  
  /**
   * Indicates that a new queue was created on this Acceptor.
   */
  void queueAdded(ClientProxyMembershipID id);
  
  /**
   * Indicates that a queue was removed from this Acceptor.
   */
  void queueRemoved();
}
