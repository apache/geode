/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal.locator.wan;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;
/**
 * 
 * @author Suranjan Kumar
 * @author Yogesh Mahajan
 * @author Kishor Bachhav
 *
 */
public class RemoteLocatorRequest implements DataSerializableFixedID{
  private int distributedSystemId ;

  public RemoteLocatorRequest() {
    super();
  }
  public RemoteLocatorRequest(int dsId, String serverGroup) {
    this.distributedSystemId = dsId;
  }
  
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.distributedSystemId = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.distributedSystemId);
  }

  public int getDsId() {
    return this.distributedSystemId;
  }
  
  public int getDSFID() {
    return DataSerializableFixedID.REMOTE_LOCATOR_REQUEST;
  }

  @Override
  public String toString() {
    return "RemoteLocatorRequest{dsName=" + distributedSystemId + "}";
  }
  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
