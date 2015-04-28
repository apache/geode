/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.gemstone.gemfire.distributed.internal.tcpserver;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

/**
 * A response from the TCP server with information
 * about the server
 * @author dsmith
 * @since 5.7
 */
public class InfoResponse implements DataSerializable {
  private static final long serialVersionUID = 6249492407448855032L;
  private String[] info;
  public InfoResponse(String[] info) {
    this.info = info;
  }
  
  /** Used by DataSerializer */
  public InfoResponse() {
  }
  
  public String[] getInfo() {
    return info;
  }

  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    info = DataSerializer.readStringArray(in);
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeStringArray(info, out);
  }
}
