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

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Properties;
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
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
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
import org.apache.geode.management.internal.configuration.messages.ClusterManagementServiceInfo;
import org.apache.geode.management.internal.configuration.messages.ClusterManagementServiceInfoRequest;
import org.apache.geode.security.AuthInitialize;

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
      return getClusterManagementServiceOnServer(username, password, (GemFireCacheImpl) cache);
    }

    ClientCache clientCache = ClientCacheFactory.getAnyInstance();
    if (clientCache != null) {
      return getClusterManagementServiceOnClient(username, password, clientCache);
    }

    throw new IllegalStateException("ClusterManagementService.create() " +
        "must be executed on one of locator, server or client cache VMs");
  }

  private ClusterManagementService getClusterManagementServiceOnServer(String username,
      String password,
      GemFireCacheImpl cache) {
    Set<InternalDistributedMember> locatorsWithClusterConfig =
        cache.getDistributionManager().getAllHostedLocatorsWithSharedConfiguration()
            .keySet();

    ClusterManagementServiceInfo cmsInfo =
        getClusterManagementServiceInfo(locatorsWithClusterConfig);

    return createClusterManagementService(username, password, cache.getSystem().getConfig(),
        cmsInfo);
  }

  private ClusterManagementService getClusterManagementServiceOnClient(String username,
      String password,
      ClientCache clientCache) {
    List<InetSocketAddress> locators = clientCache.getDefaultPool().getLocators();

    if (locators.size() == 0) {
      throw new IllegalStateException(
          "the client needs to have a client pool connected with a locator.");
    }
    DistributionConfig config = ((GemFireCacheImpl) clientCache).getSystem().getConfig();
    TcpClient client = new TcpClient(config);
    ClusterManagementServiceInfo cmsInfo = null;
    for (InetSocketAddress locator : locators) {
      try {
        cmsInfo =
            (ClusterManagementServiceInfo) client.requestToServer(locator,
                new ClusterManagementServiceInfoRequest(), 1000, true);

        // do not try anymore if we found one that has cms running
        if (cmsInfo.isRunning()) {
          break;
        }
      } catch (Exception e) {
        logger.warn(
            "unable to discover the ClusterManagementService on locator " + locator.toString());
      }
    }

    // if cmsInfo is still null at this point, i.e. we failed to retrieve the cms information from
    // any locator
    if (cmsInfo == null || !cmsInfo.isRunning()) {
      throw new IllegalStateException(
          "Unable to discover a locator that has ClusterManagementService running.");
    }
    return createClusterManagementService(username, password, config, cmsInfo);

  }

  private ClusterManagementService createClusterManagementService(String username, String password,
      DistributionConfig config,
      ClusterManagementServiceInfo cmsInfo) {
    // if user didn't pass in a username and the locator requires credentials, use the credentials
    // user used to create the client cache
    if (cmsInfo.isSecured() && username == null) {
      Properties securityProps = config.getSecurityProps();
      username = securityProps.getProperty(AuthInitialize.SECURITY_USERNAME);
      password = securityProps.getProperty(AuthInitialize.SECURITY_PASSWORD);
      if (StringUtils.isBlank(username)) {
        String message = String.format("You will need to either call getService with username and "
            + "password or specify security-username and security-password in the properties when "
            + "starting this geode server/client.");
        throw new IllegalStateException(message);
      }
    }

    SSLContext sslContext = null;
    HostnameVerifier hostnameVerifier = null;
    if (cmsInfo.isSSL()) {
      SSLConfig sslConfig = SSLConfigurationFactory.getSSLConfigForComponent(
          config, SecurableCommunicationChannel.WEB);
      if (!sslConfig.useDefaultSSLContext() && sslConfig.getTruststore() == null) {
        throw new IllegalStateException(
            "This server/client needs to have ssl-truststore or ssl-use-default-context specified in order to use cluster management service.");
      }

      sslContext = SSLUtil.createAndConfigureSSLContext(sslConfig, false);
      hostnameVerifier = new NoopHostnameVerifier();
    }

    return create(cmsInfo.getHostName(), cmsInfo.getHttpPort(), sslContext, hostnameVerifier,
        username, password);
  }


  private ClusterManagementServiceInfo getClusterManagementServiceInfo(
      Set<InternalDistributedMember> locators) {
    ClusterManagementServiceInfo info = new ClusterManagementServiceInfo();
    MemberInformation memberInfo = null;
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
        memberInfo = memberInformations.get(0);
        break;
      } catch (FunctionException e) {
        logger.warn("Unable to execute GetMemberInformationFunction on " + locator.getId());
      }
    }

    if (memberInfo == null) {
      throw new IllegalStateException("Unable to determine ClusterManagementService endpoint");
    }

    info.setHostName(getHostName(memberInfo));
    info.setHttpPort(memberInfo.getHttpServicePort());
    info.setSSL(memberInfo.isWebSSL());
    info.setSecured(memberInfo.isSecured());
    return info;
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
