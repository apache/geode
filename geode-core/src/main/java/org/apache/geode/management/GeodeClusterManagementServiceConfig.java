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

package org.apache.geode.management;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.logging.log4j.Logger;
import org.springframework.web.client.RestTemplate;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.api.ClusterManagementServiceConfig;
import org.apache.geode.management.client.JavaClientClusterManagementServiceConfig;
import org.apache.geode.management.internal.SSLUtil;
import org.apache.geode.management.internal.cli.domain.MemberInformation;
import org.apache.geode.management.internal.cli.functions.GetMemberInformationFunction;
import org.apache.geode.management.internal.configuration.messages.ClusterManagementServiceInfo;
import org.apache.geode.management.internal.configuration.messages.ClusterManagementServiceInfoRequest;
import org.apache.geode.security.AuthInitialize;

/**
 * The class used to create an instance of {@link ClusterManagementService} using appropriate
 * settings derived from either a {@link Cache} or {@link ClientCache}.
 * <p/>
 * For example, if a Geode client has already been configured to use SSL and a {@link
 * SecurityManager} is configured on the cluster, the following might be sufficient:
 * <p/>
 *
 * <pre>
 *   ClusterManagementServiceConfig config = GeodeClusterManagementServiceConfig.builder()
 *       .setClientCache(cache)
 *       .build()
 *
 *   ClusterManagementService client = new ClientClusterManagementService(config);
 * </pre>
 */

public class GeodeClusterManagementServiceConfig implements ClusterManagementServiceConfig {

  @Immutable
  private static final GetMemberInformationFunction MEMBER_INFORMATION_FUNCTION =
      new GetMemberInformationFunction();

  private static final Logger logger = LogService.getLogger();

  public interface GenericBuilder {

    /**
     * If necessary, the username may explicitly be provided.
     */
    GenericBuilder setUsername(String username);

    /**
     * If necessary, the password may explicitly be provided.
     */
    GenericBuilder setPassword(String password);

    ClusterManagementServiceConfig build();
  }

  public interface ClientCacheBuilder extends GenericBuilder {

    /**
     * When used in the context of a Geode client a {@link ClientCache} can be used to configure the
     * connection. The following will be derived:
     * <ul>
     * <li>The locator address, taken from the default {@link Pool}, will be used as the
     * hostname.</li>
     * <li>The locator address will be used to query for the correct
     * {@code ClusterManagementService} port</li>
     * <li>The client's SSL properties will be used to configure the {@code SSLContext}</li>
     * <li>The client's configured {@code security-username} and {@code security-password} Geode
     * properties will be used if available</li>
     * </ul>
     */
    ClientCacheBuilder setClientCache(ClientCache clientCache);

    ClusterManagementServiceConfig build();
  }

  public interface CacheBuilder extends GenericBuilder {

    /**
     * When used in the context of a Geode server a {@link Cache} can be used to configure the
     * connection. The following will be derived:
     * <ul>
     * <li>The locator address will be used as the hostname.</li>
     * <li>The locator address will be used to query for the correct
     * {@code ClusterManagementService} port</li>
     * <li>The servers's SSL properties will be used to configure the {@code SSLContext}</li>
     * <li>The servers's configured {@code security-username} and {@code security-password} Geode
     * properties will be used if available</li>
     * </ul>
     */
    CacheBuilder setCache(Cache cache);

    ClusterManagementServiceConfig build();
  }

  public static class Builder implements ClientCacheBuilder, CacheBuilder {
    private String username;
    private String password;
    private Cache cache;
    private ClientCache clientCache;

    private Builder() {}

    @Override
    public GenericBuilder setUsername(String username) {
      this.username = username;
      return this;
    }

    @Override
    public GenericBuilder setPassword(String password) {
      this.password = password;
      return this;
    }

    @Override
    public CacheBuilder setCache(Cache cache) {
      this.cache = cache;
      return this;
    }

    @Override
    public ClientCacheBuilder setClientCache(ClientCache clientCache) {
      this.clientCache = clientCache;
      return this;
    }

    public ClusterManagementServiceConfig build() {
      if (cache != null && cache.isServer()) {
        return getClusterManagementServiceConfigOnServer();
      }

      if (clientCache != null) {
        return getClusterManagementServiceConfigOnClient();
      }

      throw new IllegalStateException("ClusterManagementService.create() " +
          "must be executed on one of locator, server or client cache VMs");
    }

    private ClusterManagementServiceConfig getClusterManagementServiceConfigOnServer() {
      Set<InternalDistributedMember> locatorsWithClusterConfig =
          ((GemFireCacheImpl) cache).getDistributionManager()
              .getAllHostedLocatorsWithSharedConfiguration()
              .keySet();

      ClusterManagementServiceInfo cmsInfo =
          getClusterManagementServiceInfo(locatorsWithClusterConfig);

      DistributionConfig config = ((GemFireCacheImpl) cache).getSystem().getConfig();
      return createClusterManagementServiceConfig(config, cmsInfo);
    }

    private ClusterManagementServiceConfig getClusterManagementServiceConfigOnClient() {
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
      return createClusterManagementServiceConfig(config, cmsInfo);

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

    private ClusterManagementServiceConfig createClusterManagementServiceConfig(
        DistributionConfig distributionConfig, ClusterManagementServiceInfo cmsInfo) {
      // if user didn't pass in a username and the locator requires credentials, use the credentials
      // user used to create the client cache
      if (cmsInfo.isSecured() && username == null) {
        Properties securityProps = distributionConfig.getSecurityProps();
        username = securityProps.getProperty(AuthInitialize.SECURITY_USERNAME);
        password = securityProps.getProperty(AuthInitialize.SECURITY_PASSWORD);
        if (StringUtils.isBlank(username)) {
          String message =
              String.format("You will need to either call getService with username and "
                  + "password or specify security-username and security-password in the properties when "
                  + "starting this geode server/client.");
          throw new IllegalStateException(message);
        }
      }

      SSLContext sslContext = null;
      HostnameVerifier hostnameVerifier = null;
      if (cmsInfo.isSSL()) {
        SSLConfig sslConfig = SSLConfigurationFactory.getSSLConfigForComponent(
            distributionConfig, SecurableCommunicationChannel.WEB);
        if (!sslConfig.useDefaultSSLContext() && sslConfig.getTruststore() == null) {
          throw new IllegalStateException(
              "This server/client needs to have ssl-truststore or ssl-use-default-context specified in order to use cluster management service.");
        }

        sslContext = SSLUtil.createAndConfigureSSLContext(sslConfig, false);
        hostnameVerifier = new NoopHostnameVerifier();
      }

      return JavaClientClusterManagementServiceConfig.builder()
          .setUsername(username)
          .setPassword(password)
          .setHost(cmsInfo.getHostName())
          .setPort(cmsInfo.getHttpPort())
          .setHostnameVerifier(hostnameVerifier)
          .setSslContext(sslContext)
          .build();
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public RestTemplate getRestTemplate() {
    return null;
  }
}
