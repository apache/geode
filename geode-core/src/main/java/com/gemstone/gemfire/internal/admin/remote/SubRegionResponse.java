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
import com.gemstone.gemfire.internal.admin.*;
import com.gemstone.gemfire.distributed.internal.*;
import java.io.*;
import java.util.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * Responds to {@link SubRegionResponse}.
 */
public final class SubRegionResponse extends AdminResponse {
  // instance variables
  String[] subRegionNames;
  String[] userAttributes;
  
  /**
   * Returns a <code>SubRegionResponse</code> that will be returned to the
   * specified recipient. The message will contains a copy of the local manager's
   * system config.
   */
  public static SubRegionResponse create(DistributionManager dm, InternalDistributedMember recipient, Region r) {
    SubRegionResponse m = new SubRegionResponse();
    m.setRecipient(recipient);

    Set subregions = r.subregions(false);

    List subNames = new ArrayList();
    List userAttrs = new ArrayList();
    Iterator it = subregions.iterator();
    while(it.hasNext()) {
      Region reg = (Region)it.next();
      subNames.add(reg.getName());
      userAttrs.
        add(CacheDisplay.
            getCachedObjectDisplay(reg.getUserAttribute(), GemFireVM.LIGHTWEIGHT_CACHE_VALUE));
    }

    String[] temp = new String[0];
    m.subRegionNames = (String[])subNames.toArray(temp);
    m.userAttributes = (String[])userAttrs.toArray(temp);

    return m;
  }
  
  // instance methods
  public Set getRegionSet(AdminRegion parent) {
//    String globalParentName = parent.getFullPath();
    HashSet result = new HashSet(subRegionNames.length);
    for (int i=0; i < subRegionNames.length; i++) {
      result.add(new AdminRegion(subRegionNames[i], parent, userAttributes[i]));
    }
    return new HashSet(result);
  }

  public int getDSFID() {
    return SUB_REGION_RESPONSE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.subRegionNames, out);
    DataSerializer.writeObject(this.userAttributes, out);
  }

  @Override
  public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.subRegionNames = (String[])DataSerializer.readObject(in);
    this.userAttributes = (String[])DataSerializer.readObject(in);
  }

  @Override
  public String toString() {
    return "SubRegionResponse from " + this.getRecipient();
  }
}
