/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
   
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
//import com.gemstone.gemfire.*;
//import com.gemstone.gemfire.internal.*;
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
