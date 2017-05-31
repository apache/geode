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

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.security.SecurityManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.realm.Realm;
import org.apache.shiro.session.mgt.DefaultSessionManager;
import org.apache.shiro.session.mgt.SessionManager;

public class RealmInitializer {
  private static Logger logger = LogService.getLogger(LogService.SECURITY_LOGGER_NAME);

  public RealmInitializer() {
    // nothing
  }

  public void initialize(final SecurityManager securityManager) {
    Realm realm = new CustomAuthRealm(securityManager);
    DefaultSecurityManager shiroManager = new DefaultSecurityManager(realm);
    SecurityUtils.setSecurityManager(shiroManager);
    increaseShiroGlobalSessionTimeout(shiroManager);
  }

  private void increaseShiroGlobalSessionTimeout(final DefaultSecurityManager shiroManager) {
    SessionManager sessionManager = shiroManager.getSessionManager();
    if (DefaultSessionManager.class.isInstance(sessionManager)) {
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
}
