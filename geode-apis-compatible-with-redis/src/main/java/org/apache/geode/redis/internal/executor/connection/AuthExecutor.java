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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_AUTH_CALLED_WITHOUT_PASSWORD_CONFIGURED;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_INVALID_USERNAME_OR_PASSWORD;
import static org.apache.geode.redis.internal.netty.Coder.bytesToString;

import java.util.List;
import java.util.Properties;

import org.apache.geode.redis.internal.executor.Executor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.SecurityManager;

public class AuthExecutor implements Executor {

  @Override
  public RedisResponse executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    SecurityManager securityManager = context.getSecurityManager();
    Properties props = new Properties();
    if (commandElems.size() == 2) {
      if (securityManager == null) {
        return RedisResponse.error(ERROR_AUTH_CALLED_WITHOUT_PASSWORD_CONFIGURED);
      }
      props.setProperty("security-username", context.getRedisUsername());
      props.setProperty("security-password", bytesToString(commandElems.get(1)));
    } else {
      if (securityManager == null) {
        String receivedUsername = new String(commandElems.get(1));
        if (receivedUsername.equalsIgnoreCase(context.getRedisUsername())) {
          return RedisResponse.ok();
        } else {
          return RedisResponse.wrongpass(ERROR_INVALID_USERNAME_OR_PASSWORD);
        }
      }
      props.setProperty("security-username", bytesToString(commandElems.get(1)));
      props.setProperty("security-password", bytesToString(commandElems.get(2)));
    }

    try {
      Object principal = securityManager.authenticate(props);
      context.setPrincipal(principal);
    } catch (AuthenticationFailedException ex) {
      return RedisResponse.wrongpass(ERROR_INVALID_USERNAME_OR_PASSWORD);
    }

    return RedisResponse.ok();
  }

}
