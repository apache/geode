/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
   
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.*;
//import com.gemstone.gemfire.internal.*;
import com.gemstone.gemfire.internal.admin.*;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.distributed.internal.*;
import java.io.*;
import java.util.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * Responds to {@link RootRegionResponse}.
 */
public final class RootRegionResponse extends AdminResponse {
  // instance variables
  //private boolean hasRoot = false;
  private String[] regions;
  private String[] userAttrs;
  
  /**
   * Returns a <code>RootRegionResponse</code> that will be returned to the
   * specified recipient. The message will contains a copy of the local manager's
   * system config.
   */
  public static RootRegionResponse create(DistributionManager dm, InternalDistributedMember recipient) {
    RootRegionResponse m = new RootRegionResponse();
    try {
      Cache cache = CacheFactory.getInstance(dm.getSystem());
      final Collection roots;
      if (! Boolean.getBoolean("gemfire.PRDebug")) {
        roots = cache.rootRegions();
      } else {
        roots = ((GemFireCacheImpl)cache).rootRegions(true);
      }


      List regionNames = new ArrayList();
      List userAttributes = new ArrayList();
      for (Iterator iter = roots.iterator(); iter.hasNext(); ) {
        Region r = (Region)iter.next();
        regionNames.add(r.getName());
        userAttributes
          .add(CacheDisplay.
               getCachedObjectDisplay(r.getUserAttribute(), GemFireVM.LIGHTWEIGHT_CACHE_VALUE));
      }
      
      String[] temp = new String[0];
      m.regions = (String[])regionNames.toArray(temp);
      m.userAttrs = (String[])userAttributes.toArray(temp);;
      
    } 
    catch (CancelException cce){ /*no cache yet*/ }    
    
    m.setRecipient(recipient);    
    return m;
  }
  
  // instance methods
  
  public Region[] getRegions(RemoteGemFireVM vm) {
    if (regions.length > 0) {      
      Region[] roots = new Region[regions.length];
      for (int i=0; i<regions.length; i++) {
        roots[i] = new AdminRegion(regions[i], vm, userAttrs[i]);
      }
      return roots;
    } else {
      return new Region[0];
    }
  }
  
  public int getDSFID() {
    return ROOT_REGION_RESPONSE;
  }

  @Override  
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(regions, out);
    DataSerializer.writeObject(userAttrs, out);
  }

  @Override  
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    regions = (String[])DataSerializer.readObject(in);
    userAttrs = (String[])DataSerializer.readObject(in);
  }

  @Override  
  public String toString() {
    return "RootRegionResponse from " + this.getRecipient();
  }
}
