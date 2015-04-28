/*
 * ========================================================================= 
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */
package com.gemstone.gemfire.cache.client.internal.locator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.internal.DataSerializableFixedID;
/**
 * 
 * @author ymahajan
 *
 */

public class GetAllServersRequest extends ServerLocationRequest {

  public GetAllServersRequest() {

  }

  public GetAllServersRequest(String serverGroup) {
    super(serverGroup);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
  }

  @Override
  public String toString() {
    return "GetAllServersRequest{group=" + getServerGroup();
  }

  public int getDSFID() {
    return DataSerializableFixedID.GET_ALL_SERVERS_REQUEST;
  }
}
