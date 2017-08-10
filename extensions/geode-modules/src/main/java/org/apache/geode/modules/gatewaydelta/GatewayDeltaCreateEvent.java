/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.modules.gatewaydelta;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Instantiator;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.CachedDeserializableFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@SuppressWarnings("serial")
public class GatewayDeltaCreateEvent extends AbstractGatewayDeltaEvent {

  private byte[] gatewayDelta;

  public GatewayDeltaCreateEvent() {}

  public GatewayDeltaCreateEvent(String regionName, String key, byte[] gatewayDelta) {
    super(regionName, key);
    this.gatewayDelta = gatewayDelta;
  }

  public byte[] getGatewayDelta() {
    return this.gatewayDelta;
  }

  public void apply(Cache cache) {
    Region<String, CachedDeserializable> region = getRegion(cache);
    region.put(this.key, CachedDeserializableFactory.create(this.gatewayDelta), true);
    if (cache.getLogger().fineEnabled()) {
      StringBuilder builder = new StringBuilder();
      builder.append("Applied ").append(this);
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

  public String toString() {
    return new StringBuilder().append("GatewayDeltaCreateEvent[").append("regionName=")
        .append(this.regionName).append("; key=").append(this.key).append("; gatewayDelta=")
        .append(this.gatewayDelta).append("]").toString();
  }
}

