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
package org.apache.geode.modules.session.catalina.internal;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.modules.gatewaydelta.AbstractGatewayDeltaEvent;
import org.apache.geode.modules.session.catalina.DeltaSession;
import org.apache.geode.modules.session.catalina.DeltaSessionInterface;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("serial")
public class DeltaSessionAttributeEventBatch extends AbstractGatewayDeltaEvent {

  private List<DeltaSessionAttributeEvent> eventQueue;

  public DeltaSessionAttributeEventBatch() {}

  public DeltaSessionAttributeEventBatch(String regionName, String sessionId,
      List<DeltaSessionAttributeEvent> eventQueue) {
    super(regionName, sessionId);
    this.eventQueue = eventQueue;
  }

  public List<DeltaSessionAttributeEvent> getEventQueue() {
    return this.eventQueue;
  }

  public void apply(Cache cache) {
    Region<String, DeltaSessionInterface> region = getRegion(cache);
    DeltaSessionInterface session = region.get(this.key);
    if (session == null) {
      StringBuilder builder = new StringBuilder();
      builder.append("Session ").append(this.key)
          .append(" was not found while attempting to apply ").append(this);
      cache.getLogger().warning(builder.toString());
    } else {
      session.applyAttributeEvents(region, this.eventQueue);
      if (cache.getLogger().fineEnabled()) {
        StringBuilder builder = new StringBuilder();
        builder.append("Applied ").append(this);
        cache.getLogger().fine(builder.toString());
      }
    }
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.eventQueue = DataSerializer.readArrayList(in);
  }

  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeArrayList((ArrayList) this.eventQueue, out);
  }

  public String toString() {
    return new StringBuilder().append("DeltaSessionAttributeEventBatch[").append("regionName=")
        .append(this.regionName).append("; sessionId=").append(this.key).append("; numberOfEvents=")
        .append(this.eventQueue.size()).append("]").toString();
  }
}

