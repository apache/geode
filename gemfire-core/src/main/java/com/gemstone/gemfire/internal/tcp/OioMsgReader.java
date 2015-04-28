/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.tcp;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.gemstone.gemfire.internal.Version;

/**
 * A message reader which reads from the socket using
 * the old io.
 * @author dsmith
 *
 */
public class OioMsgReader extends MsgReader {

  public OioMsgReader(Connection conn, Version version) {
    super(conn, version);
  }

  @Override
  public ByteBuffer readAtLeast(int bytes) throws IOException {
    byte[] buffer = new byte[bytes];
    conn.readFully(conn.getSocket().getInputStream(), buffer, bytes);
    return ByteBuffer.wrap(buffer);
  }

}
