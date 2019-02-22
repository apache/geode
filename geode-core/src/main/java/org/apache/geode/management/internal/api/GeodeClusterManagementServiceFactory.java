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

package org.apache.geode.management.internal.api;

import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.client.ClusterManagementServiceProvider;
import org.apache.geode.management.internal.ClusterManagementClient;
import org.apache.geode.management.internal.JavaClientClusterManagementFactory;
import org.apache.geode.management.internal.cli.domain.MemberInformation;
import org.apache.geode.management.internal.cli.functions.GetMemberInformationFunction;
import org.apache.geode.management.spi.ClusterManagementServiceFactory;

/**
 * An implementation of {@link ClusterManagementServiceFactory} which can be used in any
 * context where Geode is running (client, server or locator). It will attempt to determine the
 * address of the {@code ClusterManagementService} when using the {@code create()} call. Otherwise
 * an explicit can also be used.
 */
public class GeodeClusterManagementServiceFactory extends
    JavaClientClusterManagementFactory {

  @Immutable
  private static final GetMemberInformationFunction MEMBER_INFORMATION_FUNCTION =
      new GetMemberInformationFunction();

  private static final Logger logger = LogService.getLogger();

  @Override
  public String getContext() {
    return ClusterManagementServiceProvider.GEODE_CONTEXT;
  }

  @Override
  public ClusterManagementService create() throws IllegalStateException {
    if (InternalLocator.getLocator() != null) {
      return InternalLocator.getLocator().getClusterManagementService();
    }

    Cache cache = CacheFactory.getAnyInstance();
    if (cache != null && cache.isServer()) {
      GemFireCacheImpl cacheImpl = (GemFireCacheImpl) cache;

      Set<InternalDistributedMember> locatorsWithClusterConfig =
          cacheImpl.getDistributionManager().getAllHostedLocatorsWithSharedConfiguration()
              .keySet();
      String serviceAddress = getHttpServiceAddress(locatorsWithClusterConfig);

      return new ClusterManagementClient(serviceAddress);
    }

    ClientCache clientCache = ClientCacheFactory.getAnyInstance();
    if (clientCache != null) {
      throw new IllegalStateException(
          "Under construction. To retrieve an instance of ClusterManagementService from a Geode client, please use either create(clusterUrl) or create(requestFactory) methods");
    }
    // } catch( CacheClosedException e) {
    throw new IllegalStateException("ClusterManagementService.create() " +
        "must be executed on one of locator, server or client cache VMs");
  }

  private String getHttpServiceAddress(Set<InternalDistributedMember> locators) {
    for (InternalDistributedMember locator : locators) {

      try {
        ResultCollector resultCollector =
            FunctionService.onMember(locator).execute(MEMBER_INFORMATION_FUNCTION);
        List<MemberInformation> memberInformation =
            (List<MemberInformation>) resultCollector.getResult();
        // Is this even possible?
        if (memberInformation.isEmpty()) {
          continue;
        }

        // What address to use
        String host;
        if (StringUtils.isNotBlank(memberInformation.get(0).getHttpServiceBindAddress())) {
          host = memberInformation.get(0).getHttpServiceBindAddress();
        } else if (StringUtils.isNotBlank(memberInformation.get(0).getServerBindAddress())) {
          host = memberInformation.get(0).getServerBindAddress();
        } else {
          host = memberInformation.get(0).getHost();
        }

        return String.format("http://%s:%d", host, memberInformation.get(0).getHttpServicePort());
      } catch (FunctionException e) {
        logger.warn("Unable to execute GetMemberInformationFunction on " + locator.getId());
        throw new IllegalStateException(e);
      }
    }

    throw new IllegalStateException("Unable to determine ClusterManagementService endpoint");
  }
}
