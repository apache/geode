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
package org.apache.geode.redis.internal.executor.connection;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_AUTH_CALLED_WITHOUT_SECURITY_CONFIGURED;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_INVALID_USERNAME_OR_PASSWORD;
import static org.apache.geode.redis.internal.netty.Coder.bytesToString;

import java.util.List;
import java.util.Properties;

import org.apache.shiro.subject.Subject;

import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.redis.internal.executor.CommandExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;
import org.apache.geode.security.AuthenticationExpiredException;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.SecurityManager;

public class AuthExecutor implements CommandExecutor {

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    SecurityService securityService = context.getSecurityService();

    // We're deviating from Redis here in that any AUTH requests, without security explicitly
    // set up, will fail.
    if (!securityService.isIntegratedSecurity()) {
      return RedisResponse.error(ERROR_AUTH_CALLED_WITHOUT_SECURITY_CONFIGURED);
    }

    Properties props = getSecurityProperties(command, context);
    try {
      Subject subject = securityService.login(props);
      context.setSubject(subject);
    } catch (AuthenticationFailedException | AuthenticationExpiredException ex) {
      return RedisResponse.wrongpass(ERROR_INVALID_USERNAME_OR_PASSWORD);
    }

    return RedisResponse.ok();
  }

  Properties getSecurityProperties(Command command, ExecutionHandlerContext context) {
    Properties properties = new Properties();
    List<byte[]> commandElems = command.getProcessedCommand();
    if (commandElems.size() == 2) {
      properties.setProperty(SecurityManager.USER_NAME, context.getRedisUsername());
      properties.setProperty(SecurityManager.PASSWORD, bytesToString(commandElems.get(1)));
    } else {
      properties.setProperty(SecurityManager.USER_NAME, bytesToString(commandElems.get(1)));
      properties.setProperty(SecurityManager.PASSWORD, bytesToString(commandElems.get(2)));
    }
    return properties;
  }

}
