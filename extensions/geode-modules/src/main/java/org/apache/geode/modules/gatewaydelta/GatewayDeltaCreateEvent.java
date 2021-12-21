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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.CachedDeserializableFactory;
import org.apache.geode.internal.cache.InternalCache;

@SuppressWarnings("serial")
public class GatewayDeltaCreateEvent extends AbstractGatewayDeltaEvent {

  private byte[] gatewayDelta;

  GatewayDeltaCreateEvent(String regionName, String key, byte[] gatewayDelta) {
    super(regionName, key);
    this.gatewayDelta = gatewayDelta;
  }

  @Override
  public void apply(Cache cache) {
    @SuppressWarnings("unchecked")
    Region<String, CachedDeserializable> region = getRegion(cache);
    region.put(key,
        CachedDeserializableFactory.create(gatewayDelta, (InternalCache) cache), true);

    if (cache.getLogger().fineEnabled()) {
      cache.getLogger().fine("Applied " + this);
    }
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    gatewayDelta = DataSerializer.readByteArray(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeByteArray(gatewayDelta, out);
  }

  public String toString() {
    return "GatewayDeltaCreateEvent[" + "regionName="
        + regionName + "; key=" + key + "; gatewayDelta="
        + Arrays.toString(gatewayDelta) + "]";
  }
}
