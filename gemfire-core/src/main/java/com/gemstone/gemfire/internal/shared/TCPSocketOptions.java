/*=========================================================================
 * Copyright (c) 2009-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.shared;

import java.net.Socket;


/**
 * Extended TCP socket options to set socket-specific KEEPALIVE settings etc.
 * Passed to {@link NativeCalls} API to set these options on the Java
 * {@link Socket} using native OS specific calls.
 * 
 * @author swale
 * @since 8.0
 */
public enum TCPSocketOptions {

  /**
   * TCP keepalive time between two transmissions on socket in idle condition
   * (in seconds)
   */
  OPT_KEEPIDLE,
  /**
   * TCP keepalive duration between successive transmissions on socket if no
   * reply to packet sent after idle timeout (in seconds)
   */
  OPT_KEEPINTVL,
  /**
   * number of retransmissions to be sent before declaring the other end to be
   * dead
   */
  OPT_KEEPCNT
}
