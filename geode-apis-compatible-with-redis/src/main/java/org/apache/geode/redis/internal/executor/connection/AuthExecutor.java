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

import static org.apache.geode.redis.internal.netty.Coder.bytesToString;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.executor.Executor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;
import org.apache.geode.security.SecurityManager;

public class AuthExecutor implements Executor {

  @Override
  public RedisResponse executeCommand(Command command,
      ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    byte[] password = context.getAuthPassword();
    if (password == null) {
      return RedisResponse.error(RedisConstants.ERROR_NO_PASS);
    }

    boolean correct = Arrays.equals(commandElems.get(1), password);

    if (correct) {
      context.setAuthenticationVerified();
      return RedisResponse.ok();
    } else {
      return RedisResponse.error(RedisConstants.ERROR_INVALID_PWD);
    }
  }

  Properties getSecurityProperties(Command command, ExecutionHandlerContext context) {
    Properties properties = new Properties();
    List<byte[]> commandElems = command.getProcessedCommand();
    if (commandElems.size() == 2) {
      properties.setProperty(SecurityManager.USER_NAME, "user");
      properties.setProperty(SecurityManager.PASSWORD, bytesToString(commandElems.get(1)));
    } else {
      properties.setProperty(SecurityManager.USER_NAME, bytesToString(commandElems.get(1)));
      properties.setProperty(SecurityManager.PASSWORD, bytesToString(commandElems.get(2)));
    }
    return properties;
  }

}
