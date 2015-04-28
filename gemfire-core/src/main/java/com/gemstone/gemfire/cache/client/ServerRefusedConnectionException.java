/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache.client;

import com.gemstone.gemfire.cache.OperationAbortedException;
import com.gemstone.gemfire.distributed.DistributedMember;

/**
 * A <code>ServerRefusedConnectionException</code> indicates a client attempted
 * to connect to a server, but the handshake was rejected.
 *
 * @author darrel
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
