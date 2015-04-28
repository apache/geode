/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.tier;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.Principal;

import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;

/**
 * <code>ClientHandShake</code> represents a handshake from the client.
 *   
 * @since 5.7
 */
public interface ClientHandShake {
  public boolean isOK();
  
  public byte getCode();
  
  public ClientProxyMembershipID getMembership();

  public int getClientReadTimeout();
  
  public Version getVersion();
  
  public void accept(OutputStream out, InputStream in, byte epType, int qSize,
      byte communicationMode, Principal principal) throws IOException;
}
