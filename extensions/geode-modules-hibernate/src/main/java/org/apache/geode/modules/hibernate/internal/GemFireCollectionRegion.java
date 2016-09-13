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
package com.gemstone.gemfire.modules.hibernate.internal;

import org.hibernate.cache.CacheDataDescription;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.CollectionRegion;
import org.hibernate.cache.access.AccessType;
import org.hibernate.cache.access.CollectionRegionAccessStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.modules.hibernate.GemFireRegionFactory;

public class GemFireCollectionRegion extends GemFireBaseRegion implements CollectionRegion {

  private Logger log = LoggerFactory.getLogger(getClass());
  
  public GemFireCollectionRegion(Region<Object, EntityWrapper> region,
      boolean isClient, CacheDataDescription metadata,
      GemFireRegionFactory regionFactory) {
    super(region, isClient, metadata, regionFactory);
  }

  @Override
  public boolean isTransactionAware() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public CacheDataDescription getCacheDataDescription() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CollectionRegionAccessStrategy buildAccessStrategy(
      AccessType accessType) throws CacheException {
    log.debug("creating collection access for region:"+this.region.getName());
    return new CollectionAccess(this);
  }

}
