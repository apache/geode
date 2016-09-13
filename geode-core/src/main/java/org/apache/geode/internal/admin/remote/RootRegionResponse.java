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

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.GemFireVM;
import org.apache.geode.internal.cache.GemFireCacheImpl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

//import org.apache.geode.internal.*;

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
      if (!Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "PRDebug")) {
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
