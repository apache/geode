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
package com.gemstone.gemfire.internal.cache;

import java.util.concurrent.Callable;
import com.gemstone.gemfire.cache.Region;

/**
 * 
 * Takes delta to be evicted and tries to evict the least no of LRU entry which
 * would make evictedBytes more than or equal to the delta
 * 
 * @since GemFire 6.0
 * 
 */
public class BucketRegionEvictior implements Callable<Object> {
  private LocalRegion region;

  private int bytesToEvict;

  public BucketRegionEvictior(LocalRegion region, int bytesToEvict) {
    this.bytesToEvict = bytesToEvict;
    this.region = region;
  }

  public Region getRegion() {
    return this.region;
  }

  public int getDelta() {
    return this.bytesToEvict;
  }

  public void setRegion(Region reg) {
    this.region = (LocalRegion)reg;
  }

  public void setDelta(int bytes) {
    this.bytesToEvict = bytes;
  }
  
  public Object call() throws Exception {
    ((AbstractLRURegionMap)region.entries).lruUpdateCallback(bytesToEvict);
    return null;
  }

}
