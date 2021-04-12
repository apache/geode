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
package org.apache.geode.cache.client.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.server.ServerLoad;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.SerialDistributionMessage;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.ServerLocator;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * A message from a server to a locator to update the locator with new load information from the
 * server. Also includes the id of any clients whose estimate is no longer needed on the
 * server-locator.
 *
 * @since GemFire 5.7
 *
 */
public class CacheServerLoadMessage extends SerialDistributionMessage {
  protected ServerLoad load;
  protected ServerLocation location;
  protected ArrayList clientIds;

  public CacheServerLoadMessage() {
    super();
  }

  public CacheServerLoadMessage(ServerLoad load, ServerLocation location, ArrayList clientIds) {
    super();
    this.load = load;
    this.location = location;
    this.clientIds = clientIds;
  }

  @Override
  protected void process(ClusterDistributionManager dm) {
    updateLocalLocators();
  }

  public void updateLocalLocators() {
    List locators = Locator.getLocators();
    for (int i = 0; i < locators.size(); i++) {
      InternalLocator l = (InternalLocator) locators.get(i);
      ServerLocator serverLocator = l.getServerLocatorAdvisee();
      if (serverLocator != null) {
        serverLocator.updateLoad(location, this.getSender().getUniqueId(), load, this.clientIds);
      }
    }
  }

  @Override
  public int getDSFID() {
    return CACHE_SERVER_LOAD_MESSAGE;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    load = new ServerLoad();
    context.getDeserializer().invokeFromData(load, in);
    location = new ServerLocation();
    context.getDeserializer().invokeFromData(location, in);
    this.clientIds = DataSerializer.readArrayList(in);
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    context.getSerializer().invokeToData(load, out);
    context.getSerializer().invokeToData(location, out);
    DataSerializer.writeArrayList(this.clientIds, out);
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    return super.clone();
  }



}
