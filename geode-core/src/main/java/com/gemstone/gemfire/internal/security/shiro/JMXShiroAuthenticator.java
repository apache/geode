/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.security.shiro;

import static com.gemstone.gemfire.management.internal.security.ResourceConstants.*;

import java.util.Collections;
import java.util.Properties;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.remote.JMXAuthenticator;
import javax.management.remote.JMXConnectionNotification;
import javax.management.remote.JMXPrincipal;
import javax.security.auth.Subject;

import com.gemstone.gemfire.internal.security.GeodeSecurityUtil;
import com.gemstone.gemfire.management.internal.security.ResourceConstants;

/**
 * this will make JMX authentication to use Shiro for Authentication
 */

public class JMXShiroAuthenticator implements JMXAuthenticator, NotificationListener {

  @Override
  public Subject authenticate(Object credentials) {
    String username = null, password = null;
    if (credentials instanceof String[]) {
      final String[] aCredentials = (String[]) credentials;
      username = aCredentials[0];
      password = aCredentials[1];
    } else if (credentials instanceof Properties) {
      username = ((Properties) credentials).getProperty(ResourceConstants.USER_NAME);
      password = ((Properties) credentials).getProperty(ResourceConstants.PASSWORD);
    } else {
      throw new SecurityException(WRONGE_CREDENTIALS_MESSAGE);
    }

    GeodeSecurityUtil.login(username, password);

    return new Subject(true, Collections.singleton(new JMXPrincipal(username)), Collections.EMPTY_SET,
      Collections.EMPTY_SET);
  }

  @Override
  public void handleNotification(Notification notification, Object handback) {
    if (notification instanceof JMXConnectionNotification) {
      JMXConnectionNotification cxNotification = (JMXConnectionNotification) notification;
      String type = cxNotification.getType();
      if (JMXConnectionNotification.CLOSED.equals(type)) {
        GeodeSecurityUtil.logout();
      }
    }
  }
}
