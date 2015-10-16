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
   
   
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.*;
//import com.gemstone.gemfire.internal.*;
//import com.gemstone.gemfire.internal.admin.*;
import com.gemstone.gemfire.distributed.internal.*;
import java.io.*;
//import java.util.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * Responds to {@link RegionAttributesResponse}.
 */
public final class RegionAttributesResponse extends AdminResponse {
  // instance variables 
  private RemoteRegionAttributes attributes;

  /**
   * Returns a <code>RegionAttributesResponse</code> that will be returned to the
   * specified recipient. The message will contains a copy of the local manager's
   * system config.
   */
  public static RegionAttributesResponse create(DistributionManager dm, InternalDistributedMember recipient, Region r) {
    RegionAttributesResponse m = new RegionAttributesResponse();
    m.setRecipient(recipient);
    m.attributes = new RemoteRegionAttributes(r.getAttributes());
    return m;
  }

  // instance methods
  public RegionAttributes getRegionAttributes() {
    return this.attributes;
  }
  
  public int getDSFID() {
    return REGION_ATTRIBUTES_RESPONSE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.attributes, out);
  }

  @Override
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.attributes = (RemoteRegionAttributes)DataSerializer.readObject(in);
  }

  @Override
  public String toString() {
    return "RegionAttributesResponse from " + this.getRecipient();
  }
}
