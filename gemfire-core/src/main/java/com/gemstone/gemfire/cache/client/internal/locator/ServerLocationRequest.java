/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal.locator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;

/**
 * @author dsmith
 *
 */
public abstract class ServerLocationRequest implements DataSerializableFixedID {

 private String serverGroup;
  
  public ServerLocationRequest(String serverGroup) {
    super();
    this.serverGroup = serverGroup;
  }
  
  /** Used by DataSerializer */
  public ServerLocationRequest() {
    
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    serverGroup = DataSerializer.readString(in);
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(serverGroup, out);
  }

  public String getServerGroup() {
    return serverGroup;
  }
  
  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}