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
package com.gemstone.gemfire.cache.client;

import com.gemstone.gemfire.cache.OperationAbortedException;
import com.gemstone.gemfire.distributed.DistributedMember;

/**
 * A <code>ServerRefusedConnectionException</code> indicates a client attempted
 * to connect to a server, but the handshake was rejected.
 *
 *
 * @since 5.7
 */
public class ServerRefusedConnectionException extends OperationAbortedException {
private static final long serialVersionUID = 1794959225832197946L;
  /**
   * Constructs an instance of <code>ServerRefusedConnectionException</code> with the
   * specified detail message.
   * @param server the server that rejected the connection
   * @param msg the detail message
   */
  public ServerRefusedConnectionException(DistributedMember server, String msg) {
    super(server + " refused connection: " + msg);
  }
}
