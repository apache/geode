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
 *
 */

package org.apache.geode.security;

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTHENTICATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_PEER_AUTHENTICATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_SHIRO_INIT;

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.internal.security.CallbackInstantiator;
import org.apache.geode.internal.security.DefaultSecurityServiceFactory;
import org.apache.geode.internal.security.LegacySecurityService;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.security.shiro.SecurityManagerProvider;

public class TestSecurityServiceFactory extends DefaultSecurityServiceFactory {

  @Override
  public SecurityService create(Properties securityProps, SecurityManager preferredSecurityManager,
      PostProcessor preferredPostProcessor) {
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
      return new TestIntegratedSecurityService(new SecurityManagerProvider(shiroConfig),
          postProcessor);
    } else if (securityManager != null) {
      return new TestIntegratedSecurityService(new SecurityManagerProvider(securityManager),
          postProcessor);
    } else if (isShiroInUse()) {
      return new TestIntegratedSecurityService(new SecurityManagerProvider(), postProcessor);
    }

    // if not return legacy security service
    String clientAuthenticatorConfig = securityProps.getProperty(SECURITY_CLIENT_AUTHENTICATOR);
    String peerAuthenticatorConfig = securityProps.getProperty(SECURITY_PEER_AUTHENTICATOR);
    return new LegacySecurityService(clientAuthenticatorConfig, peerAuthenticatorConfig);
  }
}
