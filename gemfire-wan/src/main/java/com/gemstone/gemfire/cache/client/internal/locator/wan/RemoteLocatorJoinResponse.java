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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.CopyOnWriteHashSet;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.admin.remote.DistributionLocatorId;

/**
 * List of remote locators as a response
 * 
 * @author Suranjan Kumar
 * @author Yogesh Mahajan
 * @author Kishor Bachhav
 * 
 * 
 */
public class RemoteLocatorJoinResponse implements DataSerializableFixedID{

  private HashMap<Integer, Set<DistributionLocatorId>> locators = new HashMap<Integer, Set<DistributionLocatorId>>();
  
  /** Used by DataSerializer */
  public RemoteLocatorJoinResponse() {
    super();
  }

  public RemoteLocatorJoinResponse(
      Map<Integer, Set<DistributionLocatorId>> locators) {
    super();
    this.locators = new HashMap<Integer, Set<DistributionLocatorId>>();
    for (Map.Entry<Integer, Set<DistributionLocatorId>> entry : locators
        .entrySet()) {
      this.locators.put(entry.getKey(), new CopyOnWriteHashSet<DistributionLocatorId>(
          entry.getValue()));
    }
  }
  
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.locators = DataSerializer.readHashMap(in);
    
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeHashMap(locators, out);
  }

  public Map<Integer, Set<DistributionLocatorId>> getLocators() {
    return this.locators;
  }

  @Override
  public String toString() {
    return "RemoteLocatorJoinResponse{locators=" + locators + "}";
  }

  public int getDSFID() {
    return DataSerializableFixedID.REMOTE_LOCATOR_JOIN_RESPONSE;
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

}
