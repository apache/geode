/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache.util;

import com.gemstone.gemfire.distributed.DistributedMember;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A <code>ServerRefusedConnectionException</code> indicates a client attempted
 * to connect to a server, but the handshake was rejected.
 *
 * @author Barry Oglesby
 *
 * @since 5.5
 * @deprecated as of 5.7 use {@link com.gemstone.gemfire.cache.client.ServerRefusedConnectionException} from the <code>client</code> package instead.
 */
@Deprecated
@SuppressFBWarnings(value="NM_SAME_SIMPLE_NAME_AS_SUPERCLASS", justification="class deprecated")
public class ServerRefusedConnectionException extends com.gemstone.gemfire.cache.client.ServerRefusedConnectionException {
private static final long serialVersionUID = -4996327025772566931L;
  /**
   * Constructs an instance of <code>ServerRefusedConnectionException</code> with the
   * specified detail message.
   * @param server the server that rejected the connection
   * @param msg the detail message
   */
  public ServerRefusedConnectionException(DistributedMember server, String msg) {
    super(server, msg);
  }
}
