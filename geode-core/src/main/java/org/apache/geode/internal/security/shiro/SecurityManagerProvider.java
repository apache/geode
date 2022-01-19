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
package org.apache.geode.internal.security.shiro;

import static org.apache.geode.logging.internal.spi.LoggingProvider.SECURITY_LOGGER_NAME;

import org.apache.logging.log4j.Logger;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.config.Ini;
import org.apache.shiro.config.IniSecurityManagerFactory;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.realm.Realm;
import org.apache.shiro.session.mgt.DefaultSessionManager;
import org.apache.shiro.session.mgt.SessionManager;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.security.SecurityManager;

public class SecurityManagerProvider {
  private static final Logger logger = LogService.getLogger(SECURITY_LOGGER_NAME);

  private final org.apache.shiro.mgt.SecurityManager shiroManager;
  private SecurityManager securityManager;

  public SecurityManagerProvider() {
    shiroManager = SecurityUtils.getSecurityManager();
  }

  public SecurityManagerProvider(String shiroConfig) {
    securityManager = null;

    IniSecurityManagerFactory factory = new IniSecurityManagerFactory("classpath:" + shiroConfig);
    // we will need to make sure that shiro uses a case sensitive permission resolver
    Ini.Section main = factory.getIni().addSection("main");
    main.put("geodePermissionResolver", GeodePermissionResolver.class.getName());
    if (!main.containsKey("iniRealm.permissionResolver")) {
      main.put("iniRealm.permissionResolver", "$geodePermissionResolver");
    }
    shiroManager = factory.getInstance();
  }


  public SecurityManagerProvider(SecurityManager securityManager) {
    this.securityManager = securityManager;

    Realm realm = new CustomAuthRealm(securityManager);
    shiroManager = new DefaultSecurityManager(realm);
    increaseShiroGlobalSessionTimeout((DefaultSecurityManager) shiroManager);
  }

  private void increaseShiroGlobalSessionTimeout(final DefaultSecurityManager shiroManager) {
    SessionManager sessionManager = shiroManager.getSessionManager();
    if (sessionManager instanceof DefaultSessionManager) {
      DefaultSessionManager defaultSessionManager = (DefaultSessionManager) sessionManager;
      defaultSessionManager.setGlobalSessionTimeout(Long.MAX_VALUE);
      long value = defaultSessionManager.getGlobalSessionTimeout();
      if (value != Long.MAX_VALUE) {
        logger.error("Unable to set Shiro Global Session Timeout. Current value is '{}'.", value);
      }
    } else {
      logger.error("Unable to set Shiro Global Session Timeout. Current SessionManager is '{}'.",
          sessionManager == null ? "null" : sessionManager.getClass());
    }
  }

  public org.apache.shiro.mgt.SecurityManager getShiroSecurityManager() {
    return shiroManager;
  }

  public SecurityManager getSecurityManager() {
    return securityManager;
  }
}
