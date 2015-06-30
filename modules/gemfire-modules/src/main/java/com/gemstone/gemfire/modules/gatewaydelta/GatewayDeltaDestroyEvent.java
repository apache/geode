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
import com.gemstone.gemfire.Instantiator;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.modules.session.catalina.DeltaSession;

@SuppressWarnings("serial")
public class GatewayDeltaDestroyEvent extends AbstractGatewayDeltaEvent {

  public GatewayDeltaDestroyEvent() {
  }

  public GatewayDeltaDestroyEvent(String regionName, String key) {
    super(regionName, key);
  }

  public void apply(Cache cache) {
    Region<String,DeltaSession> region = getRegion(cache);
    try {
      region.destroy(this.key);
      if (cache.getLogger().fineEnabled()) {
        StringBuilder builder = new StringBuilder();
        builder
          .append("Applied ")
          .append(this);
        cache.getLogger().fine(builder.toString());
      }
    } catch (EntryNotFoundException e) {
      StringBuilder builder = new StringBuilder();
      builder
        .append(this)
        .append(": Session ")
        .append(this.key)
        .append(" was not found");
      cache.getLogger().warning(builder.toString());
    }
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
  }

  public void toData(DataOutput out) throws IOException {
    super.toData(out);
  }
  
  public static void registerInstantiator(int id) {
    Instantiator.register(new Instantiator(GatewayDeltaDestroyEvent.class, id) {
      public DataSerializable newInstance() {
        return new GatewayDeltaDestroyEvent();
      }
    });
  }
  
  public String toString() {
    return new StringBuilder()
      .append("GatewayDeltaDestroyEvent[")
      .append("regionName=")
      .append(this.regionName)
      .append("; key=")
      .append(this.key)
      .append("]")
      .toString();
  }
}

