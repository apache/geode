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
 * @author Kishor Bachhav
 *
 */

public class RemoteLocatorPingRequest implements DataSerializableFixedID{

  public RemoteLocatorPingRequest() {
    super();
  }

  public RemoteLocatorPingRequest(String serverGroup) {
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
  }

  public void toData(DataOutput out) throws IOException {
  }

  public int getDSFID() {
    return DataSerializableFixedID.REMOTE_LOCATOR_PING_REQUEST;
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

}
