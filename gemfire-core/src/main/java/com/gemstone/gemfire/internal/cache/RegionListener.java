package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;

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
