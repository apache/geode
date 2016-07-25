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
package com.gemstone.gemfire.internal;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.LOCATORS;
import static com.gemstone.gemfire.distributed.ConfigurationProperties.MCAST_PORT;

@Category(IntegrationTest.class)
public class Bug51616JUnitTest {
  @Test
  public void testBug51616() {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    final Cache cache = (new CacheFactory(props)).create();
    try {
      RegionFactory<Integer, String> rf1 = cache.createRegionFactory(RegionShortcut.PARTITION);    
      FixedPartitionAttributes fpa = FixedPartitionAttributes.createFixedPartition("one", true, 111);
      PartitionAttributesFactory<Integer, String> paf = new PartitionAttributesFactory<Integer, String>();
      paf.setTotalNumBuckets(111).setRedundantCopies(0).addFixedPartitionAttributes(fpa);
      rf1.setPartitionAttributes(paf.create());

      Region<Integer, String> region1 = rf1.create("region1");

      RegionFactory<String, Object> rf2 = cache.createRegionFactory(RegionShortcut.PARTITION);
      PartitionAttributesFactory<String, Object> paf2 = new PartitionAttributesFactory<String,Object>();
      paf2.setColocatedWith(region1.getFullPath()).setTotalNumBuckets(111).setRedundantCopies(0);
      PartitionAttributes<String, Object> attrs2 = paf2.create();
      rf2.setPartitionAttributes(attrs2);
      rf2.create("region2");
    } finally {
      cache.close();
    }
  }
}
