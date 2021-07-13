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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;

public class ColocatedPRJUnitTest {
  @SuppressWarnings("rawtypes")
  @Test
  public void destroyColocatedPRCheckForLeak() {
    PartitionedRegion parent =
        (PartitionedRegion) PartitionedRegionTestHelper.createPartitionedRegion("PARENT");
    List<PartitionedRegion> colocatedList = parent.getColocatedByList();
    assertEquals(0, colocatedList.size());
    PartitionAttributes PRatts =
        new PartitionAttributesFactory().setColocatedWith(SEPARATOR + "PARENT").create();
    PartitionedRegion child =
        (PartitionedRegion) PartitionedRegionTestHelper.createPartitionedRegion("CHILD", PRatts);
    assertTrue(colocatedList.contains(child));
    child.destroyRegion();
    assertFalse(colocatedList.contains(child));
  }
}
