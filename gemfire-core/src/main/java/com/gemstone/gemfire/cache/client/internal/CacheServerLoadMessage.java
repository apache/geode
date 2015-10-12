/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
 * @author dsmith
 * @since 5.7
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
