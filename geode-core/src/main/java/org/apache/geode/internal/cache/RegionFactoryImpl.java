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

import java.io.File;
import java.util.Properties;

import org.apache.geode.CancelException;
import org.apache.geode.cache.*;
import org.apache.geode.cache.client.ClientNotReadyException;

/**
 * <code>RegionFactoryImpl</code> extends RegionFactory
 * adding {@link RegionShortcut} support.
 * @since GemFire 6.5
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
