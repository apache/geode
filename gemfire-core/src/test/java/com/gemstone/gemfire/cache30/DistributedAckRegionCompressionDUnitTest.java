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
package com.gemstone.gemfire.cache30;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.compression.Compressor;
import com.gemstone.gemfire.compression.SnappyCompressor;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;

/**
 * Tests Distributed Ack Region with compression.
 * 
 * @author rholmes
 * @since 8.0
 */
@SuppressWarnings({ "deprecation", "serial" })
public class DistributedAckRegionCompressionDUnitTest extends
    DistributedAckRegionDUnitTest {
  
  public DistributedAckRegionCompressionDUnitTest(String name) {
    super(name);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  protected RegionAttributes getRegionAttributes() {
    Compressor compressor = null;
    try {
      compressor = SnappyCompressor.getDefaultInstance();
    } catch (Throwable t) {
      // Not a supported OS
      return super.getRegionAttributes();
    }
    RegionAttributes attrs = super.getRegionAttributes();
    AttributesFactory factory = new AttributesFactory(attrs);
    factory.setCompressor(compressor);
    return factory.create();
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  protected RegionAttributes getRegionAttributes(String type) {
    Compressor compressor = null;
    try {
      compressor = SnappyCompressor.getDefaultInstance();
    } catch (Throwable t) {
      // Not a supported OS
      return super.getRegionAttributes(type);
    }
    RegionAttributes ra = super.getRegionAttributes(type);
    AttributesFactory factory = new AttributesFactory(ra);
    if(!ra.getDataPolicy().isEmpty()) {
      factory.setCompressor(compressor);
    }
    return factory.create();
  }
}
