/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.geode.cache.lucene.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.FixedPartitionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.test.dunit.VM;

public class LuceneDistributedTestUtilities extends LuceneTestUtilities {


  public static Region initDataStoreForFixedPR(final Cache cache) throws Exception {
    List<FixedPartitionAttributes> fpaList = new ArrayList<>();
    int vmNum = VM.getCurrentVMNum();
    if (vmNum % 2 == 0) {
      FixedPartitionAttributes fpa1 = FixedPartitionAttributes.createFixedPartition(Quarter1, true);
      FixedPartitionAttributes fpa2 =
          FixedPartitionAttributes.createFixedPartition(Quarter2, false);
      fpaList.clear();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
    } else {
      FixedPartitionAttributes fpa1 =
          FixedPartitionAttributes.createFixedPartition(Quarter1, false);
      FixedPartitionAttributes fpa2 = FixedPartitionAttributes.createFixedPartition(Quarter2, true);
      fpaList.clear();
      fpaList.add(fpa1);
      fpaList.add(fpa2);
    }

    return createFixedPartitionedRegion(cache, REGION_NAME, fpaList, 40);
  }

}
