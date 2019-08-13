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

package org.apache.geode.management.internal.configuration.converters;

import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.configuration.RegionType;
import org.apache.geode.management.configuration.Region;

public class RegionConverter extends ConfigurationConverter<Region, RegionConfig> {
  @Override
  protected Region fromNonNullXmlObject(RegionConfig xmlObject) {
    Region region = new Region();
    region.setName(xmlObject.getName());
    region.setType(RegionType.valueOf(xmlObject.getType()));
    RegionAttributesType regionAttributes = xmlObject.getRegionAttributes();
    region.setDiskStoreName(regionAttributes.getDiskStoreName());
    region.setKeyConstraint(regionAttributes.getKeyConstraint());
    region.setValueConstraint(regionAttributes.getValueConstraint());
    return region;
  }

  @Override
  protected RegionConfig fromNonNullConfigObject(Region configObject) {
    RegionConfig region = new RegionConfig();
    region.setName(configObject.getName());
    region.setType(configObject.getType().name());

    RegionAttributesType attributesType = new RegionAttributesType();

    attributesType.setDiskStoreName(configObject.getDiskStoreName());
    attributesType.setKeyConstraint(configObject.getKeyConstraint());
    attributesType.setValueConstraint(configObject.getValueConstraint());
    region.setRegionAttributes(attributesType);
    return region;
  }
}
