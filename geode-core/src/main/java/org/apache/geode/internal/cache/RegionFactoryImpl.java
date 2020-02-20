/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache;

import java.io.IOException;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.CacheExistsException;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.TimeoutException;

/**
 * {@code RegionFactoryImpl} extends RegionFactory adding {@link RegionShortcut} support.
 * It also supports setting InternalRegionArguments.
 *
 * @since GemFire 6.5
 */
public class RegionFactoryImpl<K, V> extends RegionFactory<K, V> {
  private InternalRegionArguments internalRegionArguments;

  public RegionFactoryImpl(InternalCache cache) {
    super(cache);
  }

  public RegionFactoryImpl(InternalCache cache, RegionShortcut pra) {
    super(cache, pra);
  }

  public RegionFactoryImpl(InternalCache cache, RegionAttributes<K, V> ra) {
    super(cache, ra);
  }

  public RegionFactoryImpl(InternalCache cache, String regionAttributesId) {
    super(cache, regionAttributesId);
  }

  public RegionFactoryImpl(RegionFactory<K, V> regionFactory) {
    super(regionFactory);
  }

  public void setInternalRegionArguments(
      InternalRegionArguments internalRegionArguments) {
    this.internalRegionArguments = internalRegionArguments;
  }

  public static InternalRegionArguments makeInternal(RegionFactory regionFactory) {
    RegionFactoryImpl internalRegionFactory = (RegionFactoryImpl) regionFactory;
    return internalRegionFactory.makeInternal();
  }

  public InternalRegionArguments makeInternal() {
    if (internalRegionArguments == null) {
      internalRegionArguments = new InternalRegionArguments();
    }
    return internalRegionArguments;
  }

  /**
   * Returns the region attributes that would currently be used to create the region.
   */
  public RegionAttributes<K, V> getCreateAttributes() {
    return getRegionAttributes();
  }

  @Override
  public Region<K, V> create(String name)
      throws CacheExistsException, RegionExistsException, CacheWriterException, TimeoutException {
    if (internalRegionArguments == null) {
      return super.create(name);
    }
    try {
      return getCache().createVMRegion(name, getRegionAttributes(), internalRegionArguments);
    } catch (IOException | ClassNotFoundException e) {
      throw new InternalGemFireError("unexpected exception", e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Region<K, V> createSubregion(Region<?, ?> parent, String name)
      throws RegionExistsException {
    if (internalRegionArguments == null) {
      return super.createSubregion(parent, name);
    }
    try {
      return ((InternalRegion) parent).createSubregion(name, getRegionAttributes(),
          internalRegionArguments);
    } catch (IOException | ClassNotFoundException e) {
      throw new InternalGemFireError("unexpected exception", e);
    }
  }
}
