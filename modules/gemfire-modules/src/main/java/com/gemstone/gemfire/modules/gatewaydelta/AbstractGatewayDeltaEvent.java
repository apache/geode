/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.gatewaydelta;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;

@SuppressWarnings("serial")
public abstract class AbstractGatewayDeltaEvent implements GatewayDeltaEvent, DataSerializable {

  protected String regionName;
  protected String key;

  public AbstractGatewayDeltaEvent() {
  }

  public AbstractGatewayDeltaEvent(String regionName, String key) {
    this.regionName = regionName;
    this.key = key;
  }
  
  public String getRegionName() {
    return this.regionName;
  }

  public String getKey() {
    return this.key;
  }
  
  @SuppressWarnings("unchecked")
  public Region getRegion(Cache cache) {
    return cache.getRegion(this.regionName);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.regionName = DataSerializer.readString(in);
    this.key = DataSerializer.readString(in);
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.regionName, out);
    DataSerializer.writeString(this.key, out);
  }
}
