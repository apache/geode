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
import java.util.Set;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;

/**
 * 
 * @author Suranjan Kumar
 * @author Yogesh Mahajan
 * @author Kishor Bachhav
 *
 */
public class RemoteLocatorResponse implements DataSerializableFixedID{

  private Set<String> locators ;

  /** Used by DataSerializer */
  public RemoteLocatorResponse() {
    super();
  }
  
  public RemoteLocatorResponse(Set<String> locators) {
    this.locators = locators;
  }
  
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.locators = DataSerializer.readObject(in);
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(this.locators, out);
  }

  public Set<String> getLocators() {
    return this.locators;
  }
  
  @Override
  public String toString() {
    return "RemoteLocatorResponse{locators=" + locators +"}";
  }

  public int getDSFID() {
    return DataSerializableFixedID.REMOTE_LOCATOR_RESPONSE;
  }

  @Override
  public Version[] getSerializationVersions() {
     return null;
  }

}
