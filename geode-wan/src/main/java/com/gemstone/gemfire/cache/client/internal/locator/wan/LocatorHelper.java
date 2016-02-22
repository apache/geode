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
package com.gemstone.gemfire.cache.client.internal.locator.wan;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.cache.client.internal.locator.wan.LocatorMembershipListener;
import com.gemstone.gemfire.distributed.internal.WanLocatorDiscoverer;
import com.gemstone.gemfire.internal.CopyOnWriteHashSet;
import com.gemstone.gemfire.internal.admin.remote.DistributionLocatorId;
/**
 * This is a helper class which helps to add the locator information to the allLocatorInfoMap.
 * 
 * @author Kishor Bachhav
 *
 */
public class LocatorHelper {
  
  public final static Object locatorObject = new Object();
  /**
   * 
   * This methods add the given locator to allLocatorInfoMap.
   * It also invokes a locatorlistener to inform other locators in allLocatorInfoMap about this newly added locator.
   * @param distributedSystemId
   * @param locator
   * @param locatorListener
   * @param sourceLocator
   */
  public static boolean addLocator(int distributedSystemId,
      DistributionLocatorId locator, LocatorMembershipListener locatorListener,
      DistributionLocatorId sourceLocator) {
      ConcurrentHashMap<Integer, Set<DistributionLocatorId>> allLocatorsInfo = (ConcurrentHashMap<Integer, Set<DistributionLocatorId>>)locatorListener
          .getAllLocatorsInfo();
      Set<DistributionLocatorId> locatorsSet = new CopyOnWriteHashSet<DistributionLocatorId>();
      locatorsSet.add(locator);
      Set<DistributionLocatorId> existingValue = allLocatorsInfo.putIfAbsent(distributedSystemId, locatorsSet);
      if(existingValue != null){
        if (!existingValue.contains(locator)) {
          existingValue.add(locator);
          addServerLocator(distributedSystemId, locatorListener, locator);
          locatorListener.locatorJoined(distributedSystemId, locator,
              sourceLocator);
        }
        else {
          return false;
        }
      }else{
        addServerLocator(distributedSystemId, locatorListener, locator);
        locatorListener.locatorJoined(distributedSystemId, locator,
          sourceLocator);
      }
    return true;
  }

  /**
   * This methods decides whether the given locator is server locator, if so
   * then add this locator in allServerLocatorsInfo map.
   * 
   * @param distributedSystemId
   * @param locatorListener
   * @param locator
   */
  private static void addServerLocator(Integer distributedSystemId,
      LocatorMembershipListener locatorListener, DistributionLocatorId locator) {
    if (!locator.isServerLocator()) {
      return;
    }
    ConcurrentHashMap<Integer, Set<String>> allServerLocatorsInfo = (ConcurrentHashMap<Integer, Set<String>>)locatorListener
        .getAllServerLocatorsInfo();
    
    Set<String> locatorsSet = new CopyOnWriteHashSet<String>();
    locatorsSet.add(locator.toString());
    Set<String> existingValue = allServerLocatorsInfo.putIfAbsent(distributedSystemId, locatorsSet);
    if(existingValue != null){
      if (!existingValue.contains(locator.toString())) {
        existingValue.add(locator.toString());
      }
    }
  }

  /**
   * This method adds the map of locatorsinfo sent by other locator to this locator's allLocatorInfo
   * 
   * @param locators
   * @param locatorListener
   */
  public static boolean addExchnagedLocators(Map<Integer, Set<DistributionLocatorId>> locators,
      LocatorMembershipListener locatorListener) {

    ConcurrentHashMap<Integer, Set<DistributionLocatorId>> allLocators = (ConcurrentHashMap<Integer, Set<DistributionLocatorId>>)locatorListener
        .getAllLocatorsInfo();
    if (!allLocators.equals(locators)) {
      for (Map.Entry<Integer, Set<DistributionLocatorId>> entry : locators
          .entrySet()) {
        Set<DistributionLocatorId> existingValue = allLocators.putIfAbsent(
            entry.getKey(), new CopyOnWriteHashSet<DistributionLocatorId>(entry
                .getValue()));

        if (existingValue != null) {
          Set<DistributionLocatorId> localLocators = allLocators.get(entry
              .getKey());
          if (!localLocators.equals(entry.getValue())) {
            entry.getValue().removeAll(localLocators);
            for (DistributionLocatorId locator : entry.getValue()) {
              localLocators.add(locator);
              addServerLocator(entry.getKey(), locatorListener, locator);
              locatorListener.locatorJoined(entry.getKey(), locator, null);
            }
          }

        }
        else {
          for (DistributionLocatorId locator : entry.getValue()) {
            addServerLocator(entry.getKey(), locatorListener, locator);
            locatorListener.locatorJoined(entry.getKey(), locator, null);
          }
        }
      }
      return true;
    }
    return false;
  }
  
}
