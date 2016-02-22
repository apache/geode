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

import org.hibernate.cache.CacheException;
import org.hibernate.cache.access.SoftLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NonStrictReadWriteAccess extends Access {

  private Logger log = LoggerFactory.getLogger(getClass());

  public NonStrictReadWriteAccess(GemFireEntityRegion region) {
    super(region);
  }

  @Override
  public SoftLock lockItem(Object key, Object version) throws CacheException {
    log.debug("lock item called for key {}", key);
    return null;
  }

  @Override
  public boolean afterUpdate(Object key, Object value, Object currentVersion,
      Object previousVersion, SoftLock lock) throws CacheException {
    log.debug("after update called for key: {} value: {}", key, value);
    getGemFireRegion().put(getWrappedKey(key), new EntityWrapper(value, -1L));
    return true;
  }
  
  @Override
  public boolean update(Object key, Object value, Object currentVersion,
      Object previousVersion) throws CacheException {
    log.debug("updating key: {} value: {}", key, value);
    getGemFireRegion().put(getWrappedKey(key), new EntityWrapper(value, -1L));
    return true;
  }
//  
//  @Override
//  public boolean insert(Object key, Object value, Object version)
//      throws CacheException {
//    log.debug("inserting key:{} value:{}", key, value);
//    getGemFireRegion().put(key, new EntityWrapper(value, -1L));
//    return true;
//  }
//  
//  @Override
//  public boolean afterInsert(Object key, Object value, Object version)
//      throws CacheException {
//    log.debug("after insert called for key:{} value:{}", key, value);
//    getGemFireRegion().put(key, new EntityWrapper(value, -1L));
//    return true;
//  }
//  
  @Override
  public boolean putFromLoad(Object key, Object value, long txTimestamp,
      Object version) throws CacheException {
    return putFromLoad(key, value, txTimestamp, version, true);
  }
  
  @Override
  public boolean putFromLoad(Object key, Object value, long txTimestamp,
      Object version, boolean minimalPutOverride) throws CacheException {
    log.debug("putting a new entry from load key:{} value:{}", key, value);
    getGemFireRegion().put(getWrappedKey(key), new EntityWrapper(value, -1L));
    return true;
  }
}
