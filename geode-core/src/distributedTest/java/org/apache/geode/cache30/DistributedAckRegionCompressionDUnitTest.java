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
package org.apache.geode.cache30;


import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.compression.Compressor;
import org.apache.geode.compression.SnappyCompressor;

/**
 * Tests Distributed Ack Region with compression.
 *
 * @since GemFire 8.0
 */

@SuppressWarnings({"deprecation", "serial"})
public class DistributedAckRegionCompressionDUnitTest extends DistributedAckRegionDUnitTest {

  @Override
  public <K, V> RegionAttributes<K, V> getRegionAttributes() {
    Compressor compressor = null;
    try {
      compressor = SnappyCompressor.getDefaultInstance();
    } catch (Throwable t) {
      // Not a supported OS
      return super.getRegionAttributes();
    }
    RegionAttributes<K, V> attrs = super.getRegionAttributes();
    AttributesFactory<K, V> factory = new AttributesFactory<>(attrs);
    factory.setCompressor(compressor);
    return factory.create();
  }

  @Override
  protected <K, V> RegionAttributes<K, V> getRegionAttributes(String type) {
    Compressor compressor = null;
    try {
      compressor = SnappyCompressor.getDefaultInstance();
    } catch (Throwable t) {
      // Not a supported OS
      return super.getRegionAttributes(type);
    }
    RegionAttributes<K, V> ra = super.getRegionAttributes(type);
    AttributesFactory<K, V> factory = new AttributesFactory<>(ra);
    if (!ra.getDataPolicy().isEmpty()) {
      factory.setCompressor(compressor);
    }
    return factory.create();
  }
}
