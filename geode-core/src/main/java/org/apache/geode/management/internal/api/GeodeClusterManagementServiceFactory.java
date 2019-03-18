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

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
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
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.client.ClusterManagementServiceProvider;
import org.apache.geode.management.internal.ClusterManagementServiceFactory;
import org.apache.geode.management.internal.JavaClientClusterManagementServiceFactory;
import org.apache.geode.management.internal.SSLUtil;
import org.apache.geode.management.internal.cli.domain.MemberInformation;
import org.apache.geode.management.internal.cli.functions.GetMemberInformationFunction;

/**
 * An implementation of {@link ClusterManagementServiceFactory} which can be used in any
 * context where Geode is running (client, server or locator). It will attempt to determine the
 * address of the {@code ClusterManagementService} when using the {@code create()} call. Otherwise
 * an explicit can also be used.
 */
public class GeodeClusterManagementServiceFactory
    extends JavaClientClusterManagementServiceFactory {

  @Immutable
  private static final GetMemberInformationFunction MEMBER_INFORMATION_FUNCTION =
      new GetMemberInformationFunction();

  private static final Logger logger = LogService.getLogger();

  @Override
  public String getContext() {
    return ClusterManagementServiceProvider.GEODE_CONTEXT;
  }

  public ClusterManagementService create() {
    return create(null, null);
  }

  @Override
  public ClusterManagementService create(String username, String password) {
    if (InternalLocator.getLocator() != null) {
      return InternalLocator.getLocator().getClusterManagementService();
    }

    Cache cache = CacheFactory.getAnyInstance();
    if (cache != null && cache.isServer()) {
      GemFireCacheImpl cacheImpl = (GemFireCacheImpl) cache;

      Set<InternalDistributedMember> locatorsWithClusterConfig =
          cacheImpl.getDistributionManager().getAllHostedLocatorsWithSharedConfiguration()
              .keySet();

      MemberInformation memberInformation = getHttpServiceAddress(locatorsWithClusterConfig);

      SSLContext sslContext = null;
      HostnameVerifier hostnameVerifier = null;
      if (memberInformation.isWebSSL()) {
        SSLConfig sslConfig = SSLConfigurationFactory.getSSLConfigForComponent(
            ((GemFireCacheImpl) cache).getSystem().getConfig(), SecurableCommunicationChannel.WEB);
        if (!sslConfig.useDefaultSSLContext() && sslConfig.getTruststore() == null) {
          throw new IllegalStateException(
              "The server needs to have truststore specified in order to use cluster management service.");
        }

        sslContext = SSLUtil.createAndConfigureSSLContext(sslConfig, false);
        hostnameVerifier = new NoopHostnameVerifier();
      }

      return create(getHostName(memberInformation), memberInformation.getHttpServicePort(),
          sslContext, hostnameVerifier, username, password);
    }

    ClientCache clientCache = ClientCacheFactory.getAnyInstance();
    if (clientCache != null) {
      throw new IllegalStateException(
          "Under construction. To retrieve an instance of ClusterManagementService from a Geode client, please use other methods");
    }
    // } catch( CacheClosedException e) {
    throw new IllegalStateException("ClusterManagementService.create() " +
        "must be executed on one of locator, server or client cache VMs");
  }


  private MemberInformation getHttpServiceAddress(Set<InternalDistributedMember> locators) {
    for (InternalDistributedMember locator : locators) {
      try {
        ResultCollector resultCollector =
            FunctionService.onMember(locator).execute(MEMBER_INFORMATION_FUNCTION);
        List<MemberInformation> memberInformations =
            (List<MemberInformation>) resultCollector.getResult();
        // Is this even possible?
        if (memberInformations.isEmpty()) {
          continue;
        }

        // return the first available one. Later for HA, we can return the entire list
        return memberInformations.get(0);
      } catch (FunctionException e) {
        logger.warn("Unable to execute GetMemberInformationFunction on " + locator.getId());
        throw new IllegalStateException(e);
      }
    }

    throw new IllegalStateException("Unable to determine ClusterManagementService endpoint");
  }

  private String getHostName(MemberInformation memberInformation) {
    String host;
    if (StringUtils.isNotBlank(memberInformation.getHttpServiceBindAddress())) {
      host = memberInformation.getHttpServiceBindAddress();
    } else if (StringUtils.isNotBlank(memberInformation.getServerBindAddress())) {
      host = memberInformation.getServerBindAddress();
    } else {
      host = memberInformation.getHost();
    }
    return host;
  }
}
