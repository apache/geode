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
package org.apache.geode.internal.cache.wan;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.admin.remote.DistributionLocatorId;

public class MyDistributedSystemListener implements DistributedSystemListener {

  public int addCount;
  public int removeCount;
  Cache cache;

  public MyDistributedSystemListener() {}

  /**
   * Please note that dynamic addition of the sender id to region is not yet available.
   */
  public void addedDistributedSystem(int remoteDsId) {
    addCount++;
    List<Locator> locatorsConfigured = Locator.getLocators();
    Locator locator = locatorsConfigured.get(0);
    Map<Integer, Set<DistributionLocatorId>> allSiteMetaData =
        ((InternalLocator) locator).getlocatorMembershipListener().getAllLocatorsInfo();
    System.out.println("Added : allSiteMetaData : " + allSiteMetaData);
  }

  public void removedDistributedSystem(int remoteDsId) {
    removeCount++;
    List<Locator> locatorsConfigured = Locator.getLocators();
    Locator locator = locatorsConfigured.get(0);
    Map<Integer, Set<DistributionLocatorId>> allSiteMetaData =
        ((InternalLocator) locator).getlocatorMembershipListener().getAllLocatorsInfo();
    System.out.println("Removed : allSiteMetaData : " + allSiteMetaData);
  }

  public int getAddCount() {
    return addCount;
  }

  public int getRemoveCount() {
    return removeCount;
  }


}
