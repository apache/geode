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
   
   
package org.apache.geode.internal.admin.remote;

import org.apache.geode.distributed.internal.*;
import org.apache.geode.internal.i18n.LocalizedStrings;
//import org.apache.geode.*;
//import org.apache.geode.internal.*;
import java.io.*;
//import java.util.*;

public final class FetchResourceAttributesRequest extends AdminRequest {
  
  // instance variables
  private long resourceUniqueId;
  
  public static FetchResourceAttributesRequest create(long id) {
    FetchResourceAttributesRequest m = new FetchResourceAttributesRequest();
    m.resourceUniqueId = id;
    return m;
  }

  public FetchResourceAttributesRequest() {
    friendlyName = LocalizedStrings.FetchResourceAttributesRequest_FETCH_STATISTICS_FOR_RESOURCE.toLocalizedString(); 
  }

  @Override  
  public AdminResponse createResponse(DistributionManager dm){
    return FetchResourceAttributesResponse.create(dm, this.getSender(), resourceUniqueId);
  }

  public int getDSFID() {
    return FETCH_RESOURCE_ATTRIBUTES_REQUEST;
  }

  @Override  
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeLong(resourceUniqueId);
  }

  @Override  
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    super.fromData(in);
    resourceUniqueId = in.readLong();
  }

  @Override  
  public String toString(){
    return LocalizedStrings.FetchResourceAttributesRequest_FETCHRESOURCEATTRIBUTESREQUEST_FOR_0.toLocalizedString(this.getRecipient());
  }
  
}
