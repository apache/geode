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


import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.util.CacheWriterAdapter;

public class EntityRegionWriter extends CacheWriterAdapter implements Declarable {
  
  private Logger log = LoggerFactory.getLogger(getClass());
  
//  @Override
//  public void beforeCreate(EntryEvent event) {
//    event.getRegion().getCache().getLogger().info("GFE:Writer invoked for beforeCreate:"+event);
//      final Object key = event.getKey();
//      EntityWrapper val = (EntityWrapper)event.getNewValue();
//      EntityWrapper oldVal = (EntityWrapper)event.getOldValue();
//      log.debug("beforeCreate: key:"+key+" val:"+val.getEntity()+" ver:"+val.getVersion()+" region:"+event.getRegion().getName()+" oldVal:"+oldVal+" this:"+System.identityHashCode(this));
//  }
  
  @Override
  public void beforeUpdate(EntryEvent event) {
    log.debug("Writer invoked for beforeUpdate:{}",event);
    final Object key = event.getKey();
    EntityWrapper val = (EntityWrapper)event.getNewValue();
    if (val.getVersion() < 0) {
      // no need for version check for NonStrictReadWrite
      // this is needed because CacheEntry does not implement equals
      return;
    }
    EntityWrapper oldVal = (EntityWrapper)event.getOldValue();
    // if same entity was loaded from two different VMs,
    // i.e. version same and entity equal then no need to destroy
    //
    if (oldVal.getVersion() == val.getVersion()) {
      if (val.getEntity().equals(oldVal.getEntity())) {
        // since CacheEntry does not override equals
        // this check is probably of no use
        return;
      }
    } else if (oldVal.getVersion() < val.getVersion()) {
      return;
    }
    log.debug("For key {} old version was {} new version was {}", new Object[] {key, oldVal.getVersion(), val.getVersion()});
    throw new CacheWriterException("key "+key+" had a newer version");
  }
  
  @Override
  public boolean equals(Object obj) {
    // This method is only implemented so that RegionCreator.validateRegion works properly.
    // The CacheWriter comparison fails because two of these instances are not equal.
    if (this == obj) {
      return true;
    }

    if (obj == null || !(obj instanceof EntityRegionWriter)) {
      return false;
    }
    return true;
  }

  @Override
  public void init(Properties arg0) {
  }
}
