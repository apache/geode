/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache30;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.compression.Compressor;
import com.gemstone.gemfire.compression.SnappyCompressor;

/**
 * Tests Partitioned Region with compression.
 * 
 * @author rholmes
 * @since 8.0
 */
public class PartitionedRegionCompressionDUnitTest extends
    PartitionedRegionDUnitTest {
  
  public PartitionedRegionCompressionDUnitTest(String name) {
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
