/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.io.File;
import java.util.Properties;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.client.ClientNotReadyException;

/**
 * <code>RegionFactoryImpl</code> extends RegionFactory
 * adding {@link RegionShortcut} support.
 * @since 6.5
 */

public class RegionFactoryImpl<K,V> extends RegionFactory<K,V>
{
  public RegionFactoryImpl(GemFireCacheImpl cache) {
    super(cache);
  }

  public RegionFactoryImpl(GemFireCacheImpl cache, RegionShortcut pra) {
    super(cache, pra);
  }

  public RegionFactoryImpl(GemFireCacheImpl cache, RegionAttributes ra) {
    super(cache, ra);
  }

  public RegionFactoryImpl(GemFireCacheImpl cache, String regionAttributesId) {
    super(cache, regionAttributesId);
  }
  
}
