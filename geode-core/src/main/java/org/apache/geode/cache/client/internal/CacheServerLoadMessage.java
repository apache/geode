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
package com.gemstone.gemfire.cache.client.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.server.ServerLoad;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.SerialDistributionMessage;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.distributed.internal.ServerLocator;
import com.gemstone.gemfire.internal.InternalDataSerializer;

/**
 * A message from a server to a locator to update the locator
 * with new load information from the server.
 * Also includes the id of any clients whose estimate is no
 * longer needed on the server-locator.
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
  
  public CacheServerLoadMessage(ServerLoad load, ServerLocation location,
                                 ArrayList clientIds) {
    super();
    this.load = load;
    this.location = location;
    this.clientIds = clientIds;
  }

  @Override
  protected void process(DistributionManager dm) {
    updateLocalLocators();
  }

  public void updateLocalLocators() {
    List locators = Locator.getLocators();
    for (int i=0; i < locators.size(); i++) {
      InternalLocator l = (InternalLocator)locators.get(i);
      ServerLocator serverLocator = l.getServerLocatorAdvisee();
      if(serverLocator != null) {
        serverLocator.updateLoad(location, load, this.clientIds);
      }
    }
  }
  
  

  public int getDSFID() {
   return CACHE_SERVER_LOAD_MESSAGE;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    load = new ServerLoad();
    InternalDataSerializer.invokeFromData(load, in);
    location = new ServerLocation();
    InternalDataSerializer.invokeFromData(location, in);
    this.clientIds = DataSerializer.readArrayList(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    InternalDataSerializer.invokeToData(load, out);
    InternalDataSerializer.invokeToData(location, out);
    DataSerializer.writeArrayList(this.clientIds, out);
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    return super.clone();
  }
  
  

}
