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
import com.gemstone.gemfire.Instantiator;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;

import com.gemstone.gemfire.internal.cache.CachedDeserializable;
import com.gemstone.gemfire.internal.cache.CachedDeserializableFactory;

@SuppressWarnings("serial")
public class GatewayDeltaCreateEvent extends AbstractGatewayDeltaEvent {

  private byte[] gatewayDelta;
  
  public GatewayDeltaCreateEvent() {
  }

  public GatewayDeltaCreateEvent(String regionName, String key, byte[] gatewayDelta) {
    super(regionName, key);
    this.gatewayDelta = gatewayDelta;
  }

  public byte[] getGatewayDelta() {
    return this.gatewayDelta;
  }
  
  public void apply(Cache cache) {
    Region<String,CachedDeserializable> region = getRegion(cache);
    region.put(this.key, CachedDeserializableFactory.create(this.gatewayDelta), true);
    if (cache.getLogger().fineEnabled()) {
      StringBuilder builder = new StringBuilder();
        builder
          .append("Applied ")
          .append(this);
      cache.getLogger().fine(builder.toString());
    }
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.gatewayDelta = DataSerializer.readByteArray(in);
  }

  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeByteArray(this.gatewayDelta, out);
  }
  
  public static void registerInstantiator(int id) {
    Instantiator.register(new Instantiator(GatewayDeltaCreateEvent.class, id) {
      public DataSerializable newInstance() {
        return new GatewayDeltaCreateEvent();
      }
    });
  }
  
  public String toString() {
    return new StringBuilder()
      .append("GatewayDeltaCreateEvent[")
      .append("regionName=")
      .append(this.regionName)
      .append("; key=")
      .append(this.key)
      .append("; gatewayDelta=")
      .append(this.gatewayDelta)
      .append("]")
      .toString();
  }
}

