/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.gemstone.gemfire.modules.gatewaydelta;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.Instantiator;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.modules.session.catalina.DeltaSession;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@SuppressWarnings("serial")
public class GatewayDeltaDestroyEvent extends AbstractGatewayDeltaEvent {

  public GatewayDeltaDestroyEvent() {
  }

  public GatewayDeltaDestroyEvent(String regionName, String key) {
    super(regionName, key);
  }

  public void apply(Cache cache) {
    Region<String, DeltaSession> region = getRegion(cache);
    try {
      region.destroy(this.key);
      if (cache.getLogger().fineEnabled()) {
        StringBuilder builder = new StringBuilder();
        builder.append("Applied ").append(this);
        cache.getLogger().fine(builder.toString());
      }
    } catch (EntryNotFoundException e) {
      StringBuilder builder = new StringBuilder();
      builder.append(this).append(": Session ").append(this.key).append(" was not found");
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
    return new StringBuilder().append("GatewayDeltaDestroyEvent[")
        .append("regionName=")
        .append(this.regionName)
        .append("; key=")
        .append(this.key)
        .append("]")
        .toString();
  }
}

