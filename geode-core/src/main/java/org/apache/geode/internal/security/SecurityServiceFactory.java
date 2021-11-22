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

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTHENTICATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_PEER_AUTHENTICATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_SHIRO_INIT;

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.internal.cache.CacheConfig;
import org.apache.geode.internal.security.shiro.SecurityManagerProvider;
import org.apache.geode.security.PostProcessor;
import org.apache.geode.security.SecurityManager;

public class SecurityServiceFactory {
  public static final String SECURITY_SERVICE_SYSTEM_PROPERTY =
      "org.apache.geode.internal.security.SecurityService";

  private SecurityServiceFactory() {
    // do not instantiate
  }

  public static SecurityService create() {
    return new LegacySecurityService();
  }

  public static SecurityService create(Properties securityProps) {
    return create(securityProps, null, null);
  }

  public static SecurityService create(Properties securityProps, CacheConfig cacheConfig) {
    if (cacheConfig == null) {
      return create(securityProps, null, null);
    }

    return create(securityProps, cacheConfig.getSecurityManager(), cacheConfig.getPostProcessor());
  }

  public static SecurityService create(Properties securityProps,
      SecurityManager preferredSecurityManager, PostProcessor preferredPostProcessor) {
    String securityServiceClassName = System.getProperty(SECURITY_SERVICE_SYSTEM_PROPERTY);
    if (securityServiceClassName != null) {
      return CallbackInstantiator.getObjectOfType(securityServiceClassName, SecurityService.class);
    }

    if (securityProps == null) {
      // avoid NPE, and we can stil use preferredSecurityManager to create the service
      securityProps = new Properties();
    }

    String shiroConfig = securityProps.getProperty(SECURITY_SHIRO_INIT);
    SecurityManager securityManager = CallbackInstantiator.getSecurityManager(securityProps);
    PostProcessor postProcessor = CallbackInstantiator.getPostProcessor(securityProps);

    // cacheConfig's securityManager/postprocessor takes precedence over those defined in
    // securityProps
    if (preferredSecurityManager != null) {
      // cacheConfig's security manager will override property's shiro.ini settings
      shiroConfig = null;
      securityManager = preferredSecurityManager;
    }
    if (preferredPostProcessor != null) {
      postProcessor = preferredPostProcessor;
    }

    if (StringUtils.isNotBlank(shiroConfig)) {
      return new IntegratedSecurityService(new SecurityManagerProvider(shiroConfig), postProcessor);
    } else if (securityManager != null) {
      return new IntegratedSecurityService(new SecurityManagerProvider(securityManager),
          postProcessor);
    } else if (isShiroInUse()) {
      return new IntegratedSecurityService(new SecurityManagerProvider(), postProcessor);
    }

    // if not return legacy security service
    String clientAuthenticatorConfig = securityProps.getProperty(SECURITY_CLIENT_AUTHENTICATOR);
    String peerAuthenticatorConfig = securityProps.getProperty(SECURITY_PEER_AUTHENTICATOR);
    return new LegacySecurityService(clientAuthenticatorConfig, peerAuthenticatorConfig);
  }

  private static boolean isShiroInUse() {
    // Don't import Shiro otherwise clients must include on classpath
    try {
      return null != Class.forName("org.apache.shiro.SecurityUtils").getMethod("getSecurityManager")
          .invoke(null);
    } catch (Exception e) {
      return false;
    }
  }

}
