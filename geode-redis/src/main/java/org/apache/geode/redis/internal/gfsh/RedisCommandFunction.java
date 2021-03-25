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
package org.apache.geode.redis.internal.gfsh;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.redis.internal.GeodeRedisService;

public class RedisCommandFunction extends CliFunction<Boolean> {

  private static final long serialVersionUID = -6607122865046807926L;

  public static void register() {
    FunctionService.registerFunction(new RedisCommandFunction());
  }

  @Override
  public CliFunctionResult executeFunction(FunctionContext<Boolean> context) {
    boolean enableUnsupportedCommands = context.getArguments();

    InternalCache cache = (InternalCache) context.getCache();

    if (!cache.getInternalDistributedSystem().getConfig().getRedisEnabled()) {
      return new CliFunctionResult(context.getMemberName(), false,
          "Error: Geode APIs compatible with Redis are not enabled");
    }

    GeodeRedisService geodeRedisService = cache.getService(GeodeRedisService.class);

    if (geodeRedisService == null) {
      return new CliFunctionResult(context.getMemberName(), false,
          "Error: GeodeRedisService is not running.");
    }

    geodeRedisService.setEnableUnsupported(enableUnsupportedCommands);

    String unsupportedCommandsString = enableUnsupportedCommands ? "Unsupported commands enabled."
        : "Unsupported commands disabled.";
    return new CliFunctionResult(context.getMemberName(), true, unsupportedCommandsString);
  }
}
