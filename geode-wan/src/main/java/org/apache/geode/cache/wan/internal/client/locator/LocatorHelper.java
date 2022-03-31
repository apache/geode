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

package org.apache.geode.cache.wan.internal.client.locator;

import static org.apache.geode.internal.lang.utils.JavaWorkarounds.computeIfAbsent;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.cache.client.internal.locator.wan.LocatorMembershipListener;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.CopyOnWriteHashSet;
import org.apache.geode.internal.admin.remote.DistributionLocatorId;

/**
 * This is a helper class which helps to add the locator information to the allLocatorInfoMap.
 */
public class LocatorHelper {

  /**
   * This method adds the given locator to allLocatorInfoMap. It also invokes a locatorListener to
   * inform other locators in allLocatorInfoMap about this newly added locator.
   */
  public static boolean addLocator(int distributedSystemId, DistributionLocatorId locator,
      LocatorMembershipListener locatorListener, DistributionLocatorId sourceLocator) {
    final ConcurrentMap<Integer, Set<DistributionLocatorId>> allLocatorsInfo = locatorListener
        .getAllLocatorsInfo();
    Set<DistributionLocatorId> locatorsSet = new CopyOnWriteHashSet<>();
    locatorsSet.add(locator);
    Set<DistributionLocatorId> existingValue =
        allLocatorsInfo.putIfAbsent(distributedSystemId, locatorsSet);
    if (existingValue != null) {
      if (!locator.getMemberName().equals(DistributionConfig.DEFAULT_NAME)) {
        final DistributionLocatorId existingLocator =
            getLocatorWithSameMemberName(existingValue, locator);

        if (existingLocator != null) {
          // if locator with same name exist, check did all parameters are same
          if (!locator.detailCompare(existingLocator)) {
            // some parameters had changed for existing locator replace it
            if (existingLocator.getTimeStamp() > locator.getTimeStamp()) {
              return false;
            }

            existingValue.remove(existingLocator);
            final ConcurrentMap<Integer, Set<String>> allServerLocatorsInfo = locatorListener
                .getAllServerLocatorsInfo();
            Set<String> allLocators = allServerLocatorsInfo.get(distributedSystemId);
            allLocators.remove(existingLocator.toString());
            existingValue.add(locator);
            addServerLocator(distributedSystemId, locatorListener, locator);
            locatorListener.locatorJoined(distributedSystemId, locator, sourceLocator);
            return true;
          }
          return false;
        } else if (!existingValue.contains(locator)) {
          existingValue.add(locator);
          addServerLocator(distributedSystemId, locatorListener, locator);
          locatorListener.locatorJoined(distributedSystemId, locator, sourceLocator);
          return true;

        } else {
          DistributionLocatorId oldLocator =
              getLocatorFromCollection(existingValue, locator);

          if (oldLocator == null) {
            return false;
          }

          if (oldLocator.getTimeStamp() > locator.getTimeStamp()) {
            return false;
          }

          // for new member name, existing host[port] received
          // replace it, to contain the latest info
          existingValue.remove(oldLocator);
          // member name is not used in equals(), so this is the way to replace old locator
          existingValue.add(locator);
          locatorListener.locatorJoined(distributedSystemId, locator, sourceLocator);
          return true;

        }

      } else if (!existingValue.contains(locator)) {
        existingValue.add(locator);
        addServerLocator(distributedSystemId, locatorListener, locator);
        locatorListener.locatorJoined(distributedSystemId, locator, sourceLocator);

      } else {
        return false;

      }

    } else {
      addServerLocator(distributedSystemId, locatorListener, locator);
      locatorListener.locatorJoined(distributedSystemId, locator, sourceLocator);
    }
    return true;
  }

  /**
   * Add given locator to the locatorListener's allServerLocatorsInfo map.
   */
  static void addServerLocator(final @NotNull Integer distributedSystemId,
      final @NotNull LocatorMembershipListener locatorListener,
      final @NotNull DistributionLocatorId locator) {
    computeIfAbsent(locatorListener.getAllServerLocatorsInfo(), distributedSystemId,
        k -> new CopyOnWriteHashSet<>()).add(locator.marshalForClients());
  }

  /**
   * This method adds the map of locators info sent by other locator to this locator's
   * allLocatorInfo
   */
  public static boolean addExchangedLocators(Map<Integer, Set<DistributionLocatorId>> locators,
      LocatorMembershipListener locatorListener) {

    ConcurrentMap<Integer, Set<DistributionLocatorId>> allLocators =
        locatorListener.getAllLocatorsInfo();
    if (!allLocators.equals(locators)) {
      for (Map.Entry<Integer, Set<DistributionLocatorId>> entry : locators.entrySet()) {
        Set<DistributionLocatorId> existingValue = allLocators.putIfAbsent(entry.getKey(),
            new CopyOnWriteHashSet<>(entry.getValue()));

        if (existingValue != null) {
          Set<DistributionLocatorId> localLocators = allLocators.get(entry.getKey());
          if (!localLocators.equals(entry.getValue())) {
            entry.getValue().removeAll(localLocators);
            for (DistributionLocatorId locator : entry.getValue()) {
              if (!locator.getMemberName().equals(DistributionConfig.DEFAULT_NAME)
                  && !localLocators.isEmpty()) {
                DistributionLocatorId existingLocator =
                    getLocatorWithSameMemberName(localLocators, locator);

                // if locator received in response, is already stored in local collection,
                // ignore this info
                if (existingLocator != null) {
                  continue;
                }

              }
              localLocators.add(locator);
              addServerLocator(entry.getKey(), locatorListener, locator);
              locatorListener.locatorJoined(entry.getKey(), locator, null);
            }
          }

        } else {
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

  /**
   * This method gets locator with specific member name from collection of locators
   */
  private static DistributionLocatorId getLocatorWithSameMemberName(
      Set<DistributionLocatorId> locatorSet, DistributionLocatorId locator) {
    for (DistributionLocatorId locElement : locatorSet) {
      if (locator.getMemberName().equals(locElement.getMemberName())) {
        return locElement;
      }
    }
    return null;
  }

  /**
   * This method gets locator equal to specified from collection of locators
   */
  private static DistributionLocatorId getLocatorFromCollection(
      Set<DistributionLocatorId> locatorSet, DistributionLocatorId locator) {
    for (DistributionLocatorId locElement : locatorSet) {
      if (locator.equals(locElement)) {
        return locElement;
      }
    }
    return null;
  }

}
