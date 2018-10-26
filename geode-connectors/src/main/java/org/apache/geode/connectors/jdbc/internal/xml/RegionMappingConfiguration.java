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
package org.apache.geode.connectors.jdbc.internal.xml;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.RegionMappingExistsException;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.extension.Extensible;
import org.apache.geode.internal.cache.extension.Extension;
import org.apache.geode.internal.cache.extension.ExtensionPoint;
import org.apache.geode.internal.cache.xmlcache.XmlGenerator;

public class RegionMappingConfiguration implements Extension<Region<?, ?>> {

  private final RegionMapping mapping;

  public RegionMappingConfiguration(RegionMapping mapping) {
    this.mapping = mapping;
  }

  @Override
  public XmlGenerator<Region<?, ?>> getXmlGenerator() {
    return null;
  }

  @Override
  public void beforeCreate(Extensible<Region<?, ?>> source, Cache cache) {
    // nothing
  }

  @Override
  public void onCreate(Extensible<Region<?, ?>> source, Extensible<Region<?, ?>> target) {
    final ExtensionPoint<Region<?, ?>> extensionPoint = target.getExtensionPoint();
    final Region<?, ?> region = extensionPoint.getTarget();
    InternalCache internalCache = (InternalCache) region.getRegionService();
    JdbcConnectorService service = internalCache.getService(JdbcConnectorService.class);
    createRegionMapping(service, mapping);
  }

  private void createRegionMapping(JdbcConnectorService service,
      RegionMapping regionMapping) {
    try {
      service.createRegionMapping(regionMapping);
    } catch (RegionMappingExistsException e) {
      throw new InternalGemFireException(e);
    }
  }
}
