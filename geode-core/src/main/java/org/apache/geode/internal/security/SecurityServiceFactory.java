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
package org.apache.geode.internal.security;

import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTHENTICATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_PEER_AUTHENTICATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_POST_PROCESSOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_SHIRO_INIT;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.CacheConfig;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.security.shiro.ConfigInitializer;
import org.apache.geode.internal.security.shiro.RealmInitializer;
import org.apache.geode.security.PostProcessor;
import org.apache.geode.security.SecurityManager;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.UnavailableSecurityManagerException;

import java.util.Properties;

public class SecurityServiceFactory {

  private SecurityServiceFactory() {
    // do not instantiate
  }

  public static SecurityService create(CacheConfig cacheConfig,
      DistributionConfig distributionConfig) {
    Properties securityConfig = getSecurityConfig(distributionConfig);
    SecurityManager securityManager =
        getSecurityManager(getSecurityManagerFromConfig(cacheConfig), securityConfig);
    PostProcessor postProcessor =
        getPostProcessor(getPostProcessorFromConfig(cacheConfig), securityConfig);
    return create(distributionConfig, securityManager, postProcessor);
  }

  /**
   * Creates and initializes SecurityService. Initialization will invoke init on both
   * SecurityManager and PostProcessor if they are specified.
   */
  public static SecurityService create(DistributionConfig distributionConfig,
      SecurityManager securityManager, PostProcessor postProcessor) {
    Properties securityConfig = getSecurityConfig(distributionConfig);

    securityManager = getSecurityManager(securityManager, securityConfig);
    postProcessor = getPostProcessor(postProcessor, securityConfig);

    SecurityService securityService = create(securityConfig, securityManager, postProcessor);
    initialize(securityService, distributionConfig);
    return securityService;
  }

  public static SecurityService create() {
    return new DisabledSecurityService();
  }

  public static SecurityService create(Properties securityConfig, SecurityManager securityManager,
      PostProcessor postProcessor) {
    SecurityServiceType type = determineType(securityConfig, securityManager, postProcessor);
    switch (type) {
      case CUSTOM:
        String shiroConfig = getProperty(securityConfig, SECURITY_SHIRO_INIT);
        if (isNotBlank(shiroConfig)) {
          new ConfigInitializer().initialize(shiroConfig);
        }
        return new CustomSecurityService(postProcessor);
      case ENABLED:
        return new EnabledSecurityService(securityManager, postProcessor, new RealmInitializer());
      case LEGACY:
        String clientAuthenticator = getProperty(securityConfig, SECURITY_CLIENT_AUTHENTICATOR);
        String peerAuthenticator = getProperty(securityConfig, SECURITY_PEER_AUTHENTICATOR);
        return new LegacySecurityService(clientAuthenticator, peerAuthenticator);
      default:
        return new DisabledSecurityService();
    }
  }

  public static SecurityService findSecurityService() {
    InternalCache cache = GemFireCacheImpl.getInstance();
    if (cache != null) {
      return cache.getSecurityService();
    }
    return SecurityServiceFactory.create();
  }

  static SecurityServiceType determineType(Properties securityConfig,
      SecurityManager securityManager, PostProcessor postProcessor) {
    boolean hasShiroConfig = hasProperty(securityConfig, SECURITY_SHIRO_INIT);
    if (hasShiroConfig) {
      return SecurityServiceType.CUSTOM;
    }

    boolean hasSecurityManager =
        securityManager != null || hasProperty(securityConfig, SECURITY_MANAGER);
    if (hasSecurityManager) {
      return SecurityServiceType.ENABLED;
    }

    boolean hasClientAuthenticator = hasProperty(securityConfig, SECURITY_CLIENT_AUTHENTICATOR);
    boolean hasPeerAuthenticator = hasProperty(securityConfig, SECURITY_PEER_AUTHENTICATOR);
    if (hasClientAuthenticator || hasPeerAuthenticator) {
      return SecurityServiceType.LEGACY;
    }

    boolean isShiroInUse = isShiroInUse();
    if (isShiroInUse) {
      return SecurityServiceType.CUSTOM;
    }

    return SecurityServiceType.DISABLED;
  }

  static SecurityManager getSecurityManager(SecurityManager securityManager,
      Properties securityConfig) {
    if (securityManager != null) {
      return securityManager;
    }

    String securityManagerConfig = getProperty(securityConfig, SECURITY_MANAGER);
    if (isNotBlank(securityManagerConfig)) {
      securityManager = CallbackInstantiator.getObjectOfTypeFromClassName(securityManagerConfig,
          SecurityManager.class);
    }

    return securityManager;
  }

  static PostProcessor getPostProcessor(PostProcessor postProcessor, Properties securityConfig) {
    if (postProcessor != null) {
      return postProcessor;
    }

    String postProcessorConfig = getProperty(securityConfig, SECURITY_POST_PROCESSOR);
    if (isNotBlank(postProcessorConfig)) {
      postProcessor = CallbackInstantiator.getObjectOfTypeFromClassName(postProcessorConfig,
          PostProcessor.class);
    }

    return postProcessor;
  }

  private static boolean isShiroInUse() {
    try {
      return SecurityUtils.getSecurityManager() != null;
    } catch (UnavailableSecurityManagerException ignore) {
      return false;
    }
  }

  private static Properties getSecurityConfig(DistributionConfig distributionConfig) {
    if (distributionConfig == null) {
      return new Properties();
    }
    return distributionConfig.getSecurityProps();
  }

  private static SecurityManager getSecurityManagerFromConfig(CacheConfig cacheConfig) {
    if (cacheConfig == null) {
      return null;
    }
    return cacheConfig.getSecurityManager();
  }

  private static PostProcessor getPostProcessorFromConfig(CacheConfig cacheConfig) {
    if (cacheConfig == null) {
      return null;
    }
    return cacheConfig.getPostProcessor();
  }

  private static boolean hasProperty(Properties securityConfig, String key) {
    return securityConfig != null && getProperty(securityConfig, key) != null;
  }

  private static String getProperty(Properties securityConfig, String key) {
    if (securityConfig == null) {
      return null;
    }
    return securityConfig.getProperty(key);
  }

  private static void initialize(SecurityService securityService,
      DistributionConfig distributionConfig) {
    if (securityService != null && distributionConfig != null) {
      securityService.initSecurity(distributionConfig.getSecurityProps());
    }
  }
}
