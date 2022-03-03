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

import static org.apache.geode.management.internal.security.ResourceConstants.MISSING_CREDENTIALS_MESSAGE;

import java.security.Principal;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.remote.JMXAuthenticator;
import javax.management.remote.JMXConnectionNotification;
import javax.management.remote.JMXPrincipal;
import javax.security.auth.Subject;

import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.internal.security.ResourceConstants;
import org.apache.geode.security.AuthenticationFailedException;

/**
 * this will make JMX authentication to use Shiro for Authentication
 */
public class JMXShiroAuthenticator implements JMXAuthenticator, NotificationListener {

  private final SecurityService securityService;
  private final Map<String, org.apache.shiro.subject.Subject> connectedUsers =
      new ConcurrentHashMap<>();

  public JMXShiroAuthenticator(SecurityService securityService) {
    this.securityService = securityService;
  }

  @Override
  public Subject authenticate(Object credentials) {
    String username;
    Properties credProps = new Properties();
    if (credentials instanceof String[]) {
      final String[] aCredentials = (String[]) credentials;
      username = aCredentials[0];
      credProps.setProperty(ResourceConstants.USER_NAME, aCredentials[0]);
      credProps.setProperty(ResourceConstants.PASSWORD, aCredentials[1]);
    } else if (credentials instanceof String) {
      username = "token-" + credentials.hashCode();
      credProps.setProperty(ResourceConstants.TOKEN, (String) credentials);
    } else {
      throw new AuthenticationFailedException(MISSING_CREDENTIALS_MESSAGE);
    }

    org.apache.shiro.subject.Subject shiroSubject = securityService.login(credProps);
    Principal principal;

    if (shiroSubject == null) {
      principal = new JMXPrincipal(username);
    } else {
      principal = new ShiroPrincipal(shiroSubject);
    }

    return new Subject(true, Collections.singleton(principal), Collections.emptySet(),
        Collections.emptySet());
  }

  @Override
  public void handleNotification(Notification notification, Object handback) {
    if (notification instanceof JMXConnectionNotification) {
      JMXConnectionNotification cxNotification = (JMXConnectionNotification) notification;
      String type = cxNotification.getType();
      String connectionId = cxNotification.getConnectionId();
      if (JMXConnectionNotification.OPENED.equals(type)) {
        connectedUsers.put(connectionId, securityService.getSubject());
      } else if (JMXConnectionNotification.CLOSED.equals(type)) {
        org.apache.shiro.subject.Subject subject = connectedUsers.remove(connectionId);
        securityService.bindSubject(subject);
        securityService.logout();
      }
    }
  }
}
