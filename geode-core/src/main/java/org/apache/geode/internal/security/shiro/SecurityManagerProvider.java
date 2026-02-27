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
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.realm.Realm;
import org.apache.shiro.realm.text.IniRealm;
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

    // Shiro 2.1.0: IniSecurityManagerFactory is removed. Use Ini and DefaultSecurityManager
    // directly. Create an IniRealm from the Ini so realms are properly configured.
    Ini ini = new Ini();
    ini.loadFromPath("classpath:" + shiroConfig);
    Ini.Section main = ini.getSection("main");
    if (main == null) {
      main = ini.addSection("main");
    }
    main.put("geodePermissionResolver", GeodePermissionResolver.class.getName());
    if (!main.containsKey("iniRealm.permissionResolver")) {
      main.put("iniRealm.permissionResolver", "$geodePermissionResolver");
    }

    // Build an IniRealm from the loaded Ini and set GeodePermissionResolver explicitly.
    // Create the realm first, set the GeodePermissionResolver, then attach the Ini
    // so the realm parses roles/permissions using our resolver.
    IniRealm iniRealm = new IniRealm();
    iniRealm.setPermissionResolver(new GeodePermissionResolver());
    iniRealm.setIni(ini);
    // If the realm exposes an init method, ensure it is initialized (defensive).
    try {
      java.lang.reflect.Method init = iniRealm.getClass().getMethod("init");
      if (init != null) {
        init.invoke(iniRealm);
      }
    } catch (Throwable t) {
      // Not critical if method is absent or invocation fails, but log for diagnostics.
      logger.debug("IniRealm init invocation failed; continuing without init", t);
    }

    // Create a DefaultSecurityManager backed by the IniRealm so realms exist.
    shiroManager = new DefaultSecurityManager((Realm) iniRealm);

    // try to increase global session timeout similar to other provider constructors
    if (shiroManager instanceof DefaultSecurityManager) {
      increaseShiroGlobalSessionTimeout((DefaultSecurityManager) shiroManager);
    }
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
