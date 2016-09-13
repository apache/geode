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
package org.apache.geode.internal.cache;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;

/**
 * Callback on a cache that receives notifications about region creates.
 * 
 * If there are multiple listeners added to a cache they are invoked in a random
 * order. See {@link GemFireCacheImpl#addRegionListener(RegionListener)}
 * 
 * If any of these callbacks throw an exception, that exception will get thrown
 * out to the user creating the region and the region creation will fail.
 */
public interface RegionListener {
  
  /**
   * Invoked before a region is created. This callback is allowed to modify the region
   * attributes before the region is created. Note that it's generally a bad idea to modify
   * the RegionAttributes in place; a new set of RegionAttributes should be returned that contain
   * the modifications. InternalRegionArguments *may* be modified, but only if you are sure
   * the caller is not going to reuse the InternalRegionArguments for something else.
   */
  public RegionAttributes beforeCreate(Region parent, String regionName, RegionAttributes attrs, InternalRegionArguments internalRegionArgs);

  /**
   * Invoked after a region is created.
   */
  public void afterCreate(Region region);
}
