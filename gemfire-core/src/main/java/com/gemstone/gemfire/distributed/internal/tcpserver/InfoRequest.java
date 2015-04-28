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

/**
 * A request to the TCP server to provide information
 * about the server
 * @author dsmith
 * @since 5.7
 *
 */
public class InfoRequest implements DataSerializable {
  private static final long serialVersionUID = -9129777520477738699L;
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
  }
  
  public void toData(DataOutput out) throws IOException {
  }
}
