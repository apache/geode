/*
 *
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
 *
 */

package org.apache.geode.tools.pulse.internal.data;

import static java.time.Instant.now;

import java.time.Instant;
import java.util.HashMap;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClient;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClientService;
import org.springframework.security.oauth2.client.authentication.OAuth2AuthenticationToken;
import org.springframework.security.oauth2.client.endpoint.DefaultRefreshTokenTokenResponseClient;
import org.springframework.security.oauth2.client.endpoint.OAuth2RefreshTokenGrantRequest;
import org.springframework.security.oauth2.core.AbstractOAuth2Token;
import org.springframework.security.oauth2.core.OAuth2AuthenticationException;
import org.springframework.security.oauth2.core.OAuth2AuthorizationException;
import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2RefreshToken;
import org.springframework.security.oauth2.core.endpoint.OAuth2AccessTokenResponse;
import org.springframework.security.oauth2.core.oidc.user.OidcUser;
import org.springframework.stereotype.Component;

/**
 * A Singleton instance of the memory cache for clusters.
 *
 * @since GemFire version 7.0.Beta 2012-09-23
 */
@Component
public class Repository {
  private static final Logger logger = LogManager.getLogger();
  private static final Locale LOCALE =
      new Locale(PulseConstants.APPLICATION_LANGUAGE, PulseConstants.APPLICATION_COUNTRY);

  private final OAuth2AuthorizedClientService authorizedClientService;
  private final ClusterFactory clusterFactory;
  private final HashMap<String, Cluster> clusterMap = new HashMap<>();
  private Boolean jmxUseLocator;
  private String host;
  private String port;
  private boolean useSSLLocator = false;
  private boolean useSSLManager = false;
  private Properties javaSslProperties;


  private final ResourceBundle resourceBundle =
      ResourceBundle.getBundle(PulseConstants.LOG_MESSAGES_FILE, LOCALE);

  private final PulseConfig pulseConfig = new PulseConfig();

  @Autowired(required = false)
  public Repository() {
    this(null);
  }

  @Autowired(required = false)
  public Repository(OAuth2AuthorizedClientService authorizedClientService) {
    this(authorizedClientService, Cluster::new);
  }

  public Repository(OAuth2AuthorizedClientService authorizedClientService,
      ClusterFactory clusterFactory) {
    this.authorizedClientService = authorizedClientService;
    this.clusterFactory = clusterFactory;
  }

  /**
   * this will return a cluster already connected to the geode jmx manager for the user in the
   * request
   * <p>
   * But for multi-user connections to gemfireJMX, i.e pulse that uses gemfire integrated security,
   * we will need to get the username from the context
   */
  public Cluster getCluster() {
    Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
    if (authentication == null) {
      return null;
    }

    if (authentication instanceof OAuth2AuthenticationToken) {
      return getClusterWithAuthenticationToken((OAuth2AuthenticationToken) authentication);
    }

    return getClusterWithUserNameAndPassword(authentication.getName(), null);
  }

  public Cluster getClusterWithUserNameAndPassword(String userName, String password) {
    String[] credentials = {userName, password};
    return getClusterWithCredentials(userName, credentials);
  }

  public Cluster getClusterWithCredentials(String userName, Object credentials) {
    synchronized (clusterMap) {
      Cluster cluster = clusterMap.get(userName);
      if (cluster == null) {
        logger.info(resourceBundle.getString("LOG_MSG_CREATE_NEW_THREAD") + " : " + userName);
        cluster = clusterFactory.create(host, port, userName, resourceBundle, this);
        // Assign name to thread created
        cluster.setName(PulseConstants.APP_NAME + "-" + host + ":" + port + ":" + userName);
        cluster.connectToGemFire(credentials);
        if (cluster.isConnectedFlag()) {
          clusterMap.put(userName, cluster);
        }
      }
      return cluster;
    }
  }

  /**
   * Returns the cluster for the user associated with the given authentication. If the user's
   * access token is expired, it is refreshed and the cluster is reconnected to JMX using the fresh
   * token. If the refresh fails, the user's cluster is disconnected from JMX and removed from the
   * repository.
   */
  private Cluster getClusterWithAuthenticationToken(OAuth2AuthenticationToken authentication) {
    OAuth2AuthorizedClient authorizedClient = getAuthorizedClient(authentication);
    if (isExpired(authorizedClient.getAccessToken())) {
      return reconnectedClusterForExpiredClient(authentication, authorizedClient);
    }

    // When the cluster connects to JMX, it will include this access token as the
    // jmx.remote.credentials attribute in the connection environment. The JMX server will then pass
    // the access token to the security manager for authentication.
    String credentials = authorizedClient.getAccessToken().getTokenValue();
    String subject = getSubject(authentication);
    return getClusterWithCredentials(subject, credentials);
  }

  private static String getSubject(Authentication authentication) {
    OAuth2AuthenticationToken oauth2Authentication = (OAuth2AuthenticationToken) authentication;
    OidcUser oidcUser = (OidcUser) oauth2Authentication.getPrincipal();
    return oidcUser.getIdToken().getSubject();
  }

  public void logoutUser(String userName) {
    Cluster cluster = clusterMap.remove(userName);
    if (cluster != null) {
      try {
        cluster.setStopUpdates(true);
        cluster.getJMXConnector().close();
      } catch (Exception e) {
        // We're logging out so this can be ignored
      }
    }
  }

  public void removeAllClusters() {
    Set<String> keySet = clusterMap.keySet();

    for (String key : keySet) {
      Cluster c = clusterMap.get(key);
      c.stopThread();
      clusterMap.remove(key);
      logger.info("{} : {}", resourceBundle.getString("LOG_MSG_REMOVE_THREAD"), key);
    }
  }

  public Boolean getJmxUseLocator() {
    return jmxUseLocator;
  }

  public void setJmxUseLocator(Boolean jmxUseLocator) {
    Objects.requireNonNull(jmxUseLocator, "jmxUseLocat == null");
    this.jmxUseLocator = jmxUseLocator;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String jmxHost) {
    host = jmxHost;
  }

  public String getPort() {
    return port;
  }

  public void setPort(String jmxPort) {
    port = jmxPort;
  }

  public boolean isUseSSLLocator() {
    return useSSLLocator;
  }

  public void setUseSSLLocator(boolean useSSLLocator) {
    this.useSSLLocator = useSSLLocator;
  }

  public boolean isUseSSLManager() {
    return useSSLManager;
  }

  public void setUseSSLManager(boolean useSSLManager) {
    this.useSSLManager = useSSLManager;
  }

  public PulseConfig getPulseConfig() {
    return pulseConfig;
  }

  public Properties getJavaSslProperties() {
    return javaSslProperties;
  }

  public void setJavaSslProperties(Properties javaSslProperties) {
    this.javaSslProperties = javaSslProperties;
  }

  public ResourceBundle getResourceBundle() {
    return resourceBundle;
  }

  private OAuth2AuthorizedClient getAuthorizedClient(
      OAuth2AuthenticationToken authenticationToken) {
    return authorizedClientService.loadAuthorizedClient(
        authenticationToken.getAuthorizedClientRegistrationId(), authenticationToken.getName());
  }

  private static boolean isExpired(AbstractOAuth2Token token) {
    Instant tokenExpiration = token.getExpiresAt();
    return tokenExpiration != null && tokenExpiration.isBefore(now());
  }

  private OAuth2AuthorizedClient refreshExpiredClient(Authentication authentication,
      OAuth2AuthorizedClient expiredClient) {
    OAuth2RefreshToken refreshToken = expiredClient.getRefreshToken();
    String subject = getSubject(authentication);
    if (refreshToken == null) {
      throw new OAuth2AuthenticationException(new OAuth2Error("401"),
          "User " + subject + " has no refresh token.");
    }
    if (isExpired(refreshToken)) {
      throw new OAuth2AuthenticationException(new OAuth2Error("401"),
          "The refresh token for " + subject + " has expired.");
    }

    OAuth2AccessTokenResponse freshToken = getFreshToken(expiredClient);

    OAuth2AuthorizedClient freshClient = new OAuth2AuthorizedClient(
        expiredClient.getClientRegistration(), expiredClient.getPrincipalName(),
        freshToken.getAccessToken(), freshToken.getRefreshToken());

    authorizedClientService.saveAuthorizedClient(freshClient, authentication);

    return freshClient;
  }

  /**
   * Refreshes the expired client's access token, reconnects the associated user's cluster using
   * the new token, and returns the reconnected cluster. If the access token cannot be refreshed,
   * the user's cluster is disconnected from JMX and removed from the repository.
   */
  private Cluster reconnectedClusterForExpiredClient(OAuth2AuthenticationToken authentication,
      OAuth2AuthorizedClient expiredClient) {
    String subject = getSubject(authentication);

    logger.info("Attempting to refresh the expired access token for {}.", subject);

    OAuth2AuthorizedClient freshClient;
    try {
      freshClient = refreshExpiredClient(authentication, expiredClient);
    } catch (OAuth2AuthenticationException | OAuth2AuthorizationException authException) {
      String message = "Failed to refresh the access token for " + subject +
          ". Disconnecting and removing the user's cluster.";
      logger.info(message);
      logoutUser(subject);
      throw authException;
    }

    logger.info("Refreshed the access token for {}. Reconnecting the user's cluster.", subject);
    synchronized (clusterMap) {
      Cluster cluster = clusterMap.get(subject);
      if (cluster != null) {
        // When the cluster reconnects to JMX, it will include this access token as the
        // jmx.remote.credentials attribute in the connection environment. The JMX server will then
        // pass the access token to the security manager for authentication.
        String credentials = freshClient.getAccessToken().getTokenValue();
        cluster.reconnectToGemFire(credentials);
      }
      return cluster;
    }
  }

  private static OAuth2AccessTokenResponse getFreshToken(OAuth2AuthorizedClient expiredClient) {
    OAuth2RefreshTokenGrantRequest refreshRequest = new OAuth2RefreshTokenGrantRequest(
        expiredClient.getClientRegistration(),
        expiredClient.getAccessToken(),
        expiredClient.getRefreshToken());

    return new DefaultRefreshTokenTokenResponseClient()
        .getTokenResponse(refreshRequest);
  }
}
