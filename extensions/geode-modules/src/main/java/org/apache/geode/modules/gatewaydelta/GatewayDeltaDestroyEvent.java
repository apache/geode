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

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.modules.session.catalina.DeltaSessionInterface;

@SuppressWarnings("serial")
public class GatewayDeltaDestroyEvent extends AbstractGatewayDeltaEvent {

  GatewayDeltaDestroyEvent(String regionName, String key) {
    super(regionName, key);
  }

  @Override
  public void apply(Cache cache) {
    @SuppressWarnings("unchecked")
    Region<String, DeltaSessionInterface> region = getRegion(cache);

    try {
      region.destroy(key);
      if (cache.getLogger().fineEnabled()) {
        cache.getLogger().fine("Applied " + this);
      }
    } catch (EntryNotFoundException e) {
      cache.getLogger().warning(this + ": Session " + key + " was not found");
    }
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
  }

  public String toString() {
    return "GatewayDeltaDestroyEvent[" + "regionName="
        + regionName + "; key=" + key + "]";
  }
}
