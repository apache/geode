/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.management.internal.configuration.realizers;

import java.util.Arrays;
import java.util.List;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.internal.cache.EvictionAttributesImpl;
import org.apache.geode.internal.cache.PartitionAttributesImpl;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.util.RegionPath;
import org.apache.geode.management.internal.configuration.domain.DeclarableTypeInstantiator;

public class RegionConfigRealizer implements ConfigurationRealizer<RegionConfig> {
  public RegionConfigRealizer() {}

  /**
   * this is used to create only root regions.
   *
   * @param regionConfig the name in the regionConfig can not contain sub-regions.
   */
  @Override
  public void create(RegionConfig regionConfig, Cache cache) {
    RegionFactory factory = getRegionFactory(cache, regionConfig.getRegionAttributes());
    try {
      factory.create(regionConfig.getName());
    } catch (RegionExistsException e) {
      // since we are trying to create this region, ignore this exception
    }
  }

  /**
   * this method supports creating root region and sub-regions.
   * We need this because CreateRegionCommand should still
   * support creating sub regions.
   *
   * @param regionConfig the name in regionConfig is ignored.
   * @param regionPath this is the full path of the region
   */
  public void create(RegionConfig regionConfig, String regionPath, Cache cache) {
    RegionFactory factory = getRegionFactory(cache, regionConfig.getRegionAttributes());
    RegionPath regionPathData = new RegionPath(regionPath);
    String regionName = regionPathData.getName();
    String parentRegionPath = regionPathData.getParent();
    if (parentRegionPath == null) {
      factory.create(regionName);
      return;
    }

    Region parentRegion = cache.getRegion(parentRegionPath);
    factory.createSubregion(parentRegion, regionName);
  }

  RegionFactory getRegionFactory(Cache cache, RegionAttributesType regionAttributes) {
    RegionFactory factory = cache.createRegionFactory();

    factory.setDataPolicy(DataPolicy.fromString(regionAttributes.getDataPolicy().name()));

    if (regionAttributes.getScope() != null) {
      factory.setScope(Scope.fromString(regionAttributes.getScope().name()));
    }

    if (regionAttributes.getCacheLoader() != null) {
      ((RegionFactory<Object, Object>) factory)
          .setCacheLoader(DeclarableTypeInstantiator.newInstance(regionAttributes.getCacheLoader(),
              cache));
    }

    if (regionAttributes.getCacheWriter() != null) {
      ((RegionFactory<Object, Object>) factory)
          .setCacheWriter(DeclarableTypeInstantiator.newInstance(regionAttributes.getCacheWriter(),
              cache));
    }

    if (regionAttributes.getCacheListeners() != null) {
      List<DeclarableType> configListeners = regionAttributes.getCacheListeners();
      CacheListener[] listeners = new CacheListener[configListeners.size()];
      for (int i = 0; i < configListeners.size(); i++) {
        listeners[i] = DeclarableTypeInstantiator.newInstance(configListeners.get(i), cache);
      }
      ((RegionFactory<Object, Object>) factory).initCacheListeners(listeners);
    }

    final String keyConstraint = regionAttributes.getKeyConstraint();
    final String valueConstraint = regionAttributes.getValueConstraint();
    if (keyConstraint != null && !keyConstraint.isEmpty()) {
      Class<Object> keyConstraintClass =
          CliUtil.forName(keyConstraint, CliStrings.CREATE_REGION__KEYCONSTRAINT);
      ((RegionFactory<Object, Object>) factory).setKeyConstraint(keyConstraintClass);
    }

    if (valueConstraint != null && !valueConstraint.isEmpty()) {
      Class<Object> valueConstraintClass =
          CliUtil.forName(valueConstraint, CliStrings.CREATE_REGION__VALUECONSTRAINT);
      ((RegionFactory<Object, Object>) factory).setValueConstraint(valueConstraintClass);
    }

    if (regionAttributes.getCompressor() != null) {
      ((RegionFactory<Object, Object>) factory)
          .setCompressor(DeclarableTypeInstantiator.newInstance(regionAttributes.getCompressor()));
    }

    if (regionAttributes.getPartitionAttributes() != null) {
      factory.setPartitionAttributes(
          convertToRegionFactoryPartitionAttributes(regionAttributes.getPartitionAttributes(),
              cache));
    }

    if (regionAttributes.getEntryIdleTime() != null) {
      RegionAttributesType.ExpirationAttributesType eitl = regionAttributes.getEntryIdleTime();
      ((RegionFactory<Object, Object>) factory).setEntryIdleTimeout(
          new ExpirationAttributes(Integer.valueOf(eitl.getTimeout()),
              ExpirationAction.fromXmlString(eitl.getAction())));


      if (eitl.getCustomExpiry() != null) {
        ((RegionFactory<Object, Object>) factory).setCustomEntryIdleTimeout(
            DeclarableTypeInstantiator.newInstance(eitl.getCustomExpiry(),
                cache));
      }
    }

    if (regionAttributes.getEntryTimeToLive() != null) {
      RegionAttributesType.ExpirationAttributesType ettl = regionAttributes.getEntryTimeToLive();
      ((RegionFactory<Object, Object>) factory).setEntryTimeToLive(
          new ExpirationAttributes(Integer.valueOf(ettl.getTimeout()),
              ExpirationAction.fromXmlString(ettl.getAction())));

      if (ettl.getCustomExpiry() != null) {
        ((RegionFactory<Object, Object>) factory)
            .setCustomEntryTimeToLive(DeclarableTypeInstantiator.newInstance(ettl.getCustomExpiry(),
                cache));
      }
    }

    if (regionAttributes.getRegionIdleTime() != null) {
      RegionAttributesType.ExpirationAttributesType ritl = regionAttributes.getRegionIdleTime();
      ((RegionFactory<Object, Object>) factory).setRegionIdleTimeout(
          new ExpirationAttributes(Integer.valueOf(ritl.getTimeout()),
              ExpirationAction.fromXmlString(ritl.getAction())));
    }

    if (regionAttributes.getRegionTimeToLive() != null) {
      RegionAttributesType.ExpirationAttributesType rttl = regionAttributes.getRegionTimeToLive();
      ((RegionFactory<Object, Object>) factory).setRegionTimeToLive(
          new ExpirationAttributes(Integer.valueOf(rttl.getTimeout()),
              ExpirationAction.fromXmlString(rttl.getAction())));
    }

    if (regionAttributes.getEvictionAttributes() != null) {
      try {
        factory.setEvictionAttributes(
            EvictionAttributesImpl.fromConfig(regionAttributes.getEvictionAttributes()));
      } catch (Exception e) {
        throw new IllegalArgumentException(
            CliStrings.CREATE_REGION__MSG__OBJECT_SIZER_MUST_BE_OBJECTSIZER_AND_DECLARABLE);
      }
    }

    if (regionAttributes.getDiskStoreName() != null) {
      factory.setDiskStoreName(regionAttributes.getDiskStoreName());
    }

    if (regionAttributes.isDiskSynchronous() != null) {
      factory.setDiskSynchronous(regionAttributes.isDiskSynchronous());
    }

    if (regionAttributes.isOffHeap() != null) {
      factory.setOffHeap(regionAttributes.isOffHeap());
    }

    if (regionAttributes.isStatisticsEnabled() != null) {
      factory.setStatisticsEnabled(regionAttributes.isStatisticsEnabled());
    }

    if (regionAttributes.isEnableAsyncConflation() != null) {
      factory.setEnableAsyncConflation(regionAttributes.isEnableAsyncConflation());
    }

    if (regionAttributes.isEnableSubscriptionConflation() != null) {
      factory.setEnableSubscriptionConflation(regionAttributes.isEnableSubscriptionConflation());
    }

    if (regionAttributes.getGatewaySenderIds() != null) {
      Arrays.stream(regionAttributes.getGatewaySenderIds().split(","))
          .forEach(gsi -> factory.addGatewaySenderId(gsi));
    }

    if (regionAttributes.getAsyncEventQueueIds() != null) {
      Arrays.stream(regionAttributes.getAsyncEventQueueIds().split(","))
          .forEach(gsi -> factory.addAsyncEventQueueId(gsi));
    }

    factory.setConcurrencyChecksEnabled(regionAttributes.isConcurrencyChecksEnabled());

    if (regionAttributes.getConcurrencyLevel() != null) {
      factory.setConcurrencyLevel(Integer.valueOf(regionAttributes.getConcurrencyLevel()));
    }

    if (regionAttributes.isCloningEnabled() != null) {
      factory.setCloningEnabled(regionAttributes.isCloningEnabled());
    }

    if (regionAttributes.isMulticastEnabled() != null) {
      factory.setMulticastEnabled(regionAttributes.isMulticastEnabled());
    }
    return factory;
  }

  PartitionAttributesImpl convertToRegionFactoryPartitionAttributes(
      RegionAttributesType.PartitionAttributes configAttributes, Cache cache) {
    PartitionAttributesImpl partitionAttributes = new PartitionAttributesImpl();
    if (configAttributes == null) {
      return null;
    }

    if (configAttributes.getRedundantCopies() != null) {
      partitionAttributes
          .setRedundantCopies(Integer.valueOf(configAttributes.getRedundantCopies()));
    }

    if (configAttributes.getTotalMaxMemory() != null) {
      partitionAttributes.setTotalMaxMemory(Integer.valueOf(configAttributes.getTotalMaxMemory()));
    }

    if (configAttributes.getTotalNumBuckets() != null) {
      partitionAttributes
          .setTotalNumBuckets(Integer.valueOf(configAttributes.getTotalNumBuckets()));
    }

    if (configAttributes.getLocalMaxMemory() != null) {
      partitionAttributes.setLocalMaxMemory(Integer.valueOf(configAttributes.getLocalMaxMemory()));
    }

    if (configAttributes.getColocatedWith() != null) {
      partitionAttributes.setColocatedWith(configAttributes.getColocatedWith());
    }

    if (configAttributes.getPartitionResolver() != null) {
      partitionAttributes.setPartitionResolver(
          DeclarableTypeInstantiator.newInstance(configAttributes.getPartitionResolver(), cache));
    }

    if (configAttributes.getRecoveryDelay() != null) {
      partitionAttributes.setRecoveryDelay(Long.valueOf(configAttributes.getRecoveryDelay()));
    }

    if (configAttributes.getStartupRecoveryDelay() != null) {
      partitionAttributes
          .setStartupRecoveryDelay(Long.valueOf(configAttributes.getStartupRecoveryDelay()));
    }

    if (configAttributes.getPartitionListeners() != null) {
      List<DeclarableType> configListeners = configAttributes.getPartitionListeners();
      for (int i = 0; i < configListeners.size(); i++) {
        partitionAttributes.addPartitionListener(
            DeclarableTypeInstantiator.newInstance(configListeners.get(i), cache));
      }
    }

    return partitionAttributes;
  }

  @Override
  public boolean exists(RegionConfig config, Cache cache) {
    return false;
  }

  @Override
  public void update(RegionConfig config, Cache cache) {}

  @Override
  public void delete(RegionConfig config, Cache cache) {}


}
