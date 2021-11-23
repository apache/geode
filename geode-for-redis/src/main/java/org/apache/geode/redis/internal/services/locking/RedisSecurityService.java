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

package org.apache.geode.redis.internal.services.locking;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import io.netty.channel.ChannelId;
import org.apache.shiro.subject.Subject;

import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.ResourcePermission;

/**
 * This class is a thin wrapper around Geode's {@link SecurityService} and delegates
 * login (authenticate), logout and authorize calls. It exists as a central point to track whether
 * a given channel (connection) has previously been authenticated.
 */
public class RedisSecurityService {

  private final Map<String, Subject> subjects = new ConcurrentHashMap<>();

  private final SecurityService securityService;

  public RedisSecurityService(SecurityService securityService) {
    this.securityService = securityService;
  }

  public boolean isEnabled() {
    return securityService.isIntegratedSecurity();
  }

  public boolean isAuthenticated(ChannelId channelId) {
    return subjects.containsKey(channelId.asShortText());
  }

  public Subject login(ChannelId channelId, Properties properties) {
    Subject subject = securityService.login(properties);
    subjects.put(channelId.asShortText(), subject);
    return subject;
  }

  public void logout(ChannelId channelId) {
    Subject subject = subjects.remove(channelId.asShortText());
    if (subject != null) {
      subject.logout();
    }
  }

  public void authorize(ResourcePermission resourcePermission, Subject subject) {
    securityService.authorize(resourcePermission, subject);
  }

}
