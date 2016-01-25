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
package com.gemstone.gemfire.cache.client.internal.locator.wan;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.client.internal.locator.ServerLocationRequest;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.admin.remote.DistributionLocatorId;

public class LocatorJoinMessage extends ServerLocationRequest {

  private DistributionLocatorId locator;
  
  private int distributedSystemId;
  
  private DistributionLocatorId sourceLocator;

  public LocatorJoinMessage() {
    super();
  }

  public LocatorJoinMessage(int distributedSystemId, DistributionLocatorId locator,
      DistributionLocatorId sourceLocator, String serverGroup) {
    super(serverGroup);
    this.locator = locator;
    this.distributedSystemId = distributedSystemId;
    this.sourceLocator = sourceLocator;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.locator = DataSerializer.readObject(in);
    this.distributedSystemId = in.readInt();
    this.sourceLocator = DataSerializer.readObject(in);
  }

  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(locator, out);
    out.writeInt(this.distributedSystemId);
    DataSerializer.writeObject(sourceLocator, out);
  }

  public DistributionLocatorId getLocator() {
    return this.locator;
  }

  public int getDistributedSystemId() {
    return distributedSystemId;
  }
  
  public DistributionLocatorId getSourceLocator() {
    return sourceLocator;
  }
  
  public int getDSFID() {
    return DataSerializableFixedID.LOCATOR_JOIN_MESSAGE;
  }

  @Override
  public String toString() {
    return "LocatorJoinMessage{distributedSystemId="+ distributedSystemId +" locators=" + locator + " Source Locator : " + sourceLocator +"}";
  }

  @Override
  public boolean equals(Object obj){
    if ( this == obj ) return true;
    if ( !(obj instanceof LocatorJoinMessage) ) return false;
    LocatorJoinMessage myObject = (LocatorJoinMessage)obj;
    if((this.distributedSystemId == myObject.getDistributedSystemId()) && this.locator.equals(myObject.getLocator())){
      return true;
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    // it is sufficient for all messages having the same locator to hash to the same bucket
    if (this.locator == null) {
      return 0;
    } else {
      return this.locator.hashCode();
    }
  }


}
