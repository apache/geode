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

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.client.internal.locator.ServerLocationRequest;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.admin.remote.DistributionLocatorId;

/**
 * Requests remote locators of a remote WAN site
 * 
 * @author Suranjan Kumar
 * @author Yogesh Mahajan
 * @author Kishor Bachhav
 * 
 * @since 6.6
 * 
 */
public class RemoteLocatorJoinRequest implements DataSerializableFixedID {

  private DistributionLocatorId locator = null;
 
  private int distributedSystemId = -1;

  public RemoteLocatorJoinRequest() {
    super();
  }

  public RemoteLocatorJoinRequest(int distributedSystemId, DistributionLocatorId locator,
      String serverGroup) {
    this.distributedSystemId = distributedSystemId;
    this.locator = locator;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.locator = DataSerializer.readObject(in);
    this.distributedSystemId = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(locator, out);
    out.writeInt(this.distributedSystemId);
  }

  public DistributionLocatorId getLocator() {
    return this.locator;
  }
  
  public int getDistributedSystemId() {
    return distributedSystemId;
  }
  
  public int getDSFID() {
    return DataSerializableFixedID.REMOTE_LOCATOR_JOIN_REQUEST;
  }

  @Override
  public String toString() {
    return "RemoteLocatorJoinRequest{locator=" + locator + "}";
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

}
