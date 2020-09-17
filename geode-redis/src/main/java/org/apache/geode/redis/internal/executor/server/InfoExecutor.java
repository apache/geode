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

package org.apache.geode.redis.internal.executor.server;

import java.util.List;

import org.apache.geode.redis.internal.ParameterRequirements.RedisParametersMismatchException;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class InfoExecutor extends AbstractExecutor {

  @Override
  public RedisResponse executeCommand(Command command,
                                      ExecutionHandlerContext context) {
    final int TCP_PORT = context.getServerPort();
    final String CURRENT_REDIS_VERSION = "5.0.6";
    final String SERVER_STRING =
        "# Server\r\n" +
            "redis_version:" + CURRENT_REDIS_VERSION + "\r\n" +
            "redis_mode:standalone\r\n" +
            "tcp_port:" + TCP_PORT + "\r\n";
    final String PERSISTENCE_STRING =
        "# Persistence\r\n" +
            "loading:0\r\n";
    final String CLUSTER_STRING =
        "# Cluster\r\n" +
            "cluster_enabled:0\r\n";
    final String SECTION_SEPARATOR = "\r\n";

    final String INFO_STRING =
        SERVER_STRING + SECTION_SEPARATOR +
            PERSISTENCE_STRING + SECTION_SEPARATOR +
            CLUSTER_STRING;

    List<ByteArrayWrapper> commands =
        command.getProcessedCommandWrappers();

    String resultsString;

    if (commands.size() > 2) {
      throw new RedisParametersMismatchException(
          RedisConstants.ERROR_SYNTAX);
    }
    if (commands.size() == 2) {
      String parameter = commands.get(1).toString();
      if (parameter.equalsIgnoreCase("server")) {
        resultsString = SERVER_STRING;
      } else if (parameter.equalsIgnoreCase("cluster")) {
        resultsString = CLUSTER_STRING;
      } else if (parameter.equalsIgnoreCase("persistence")) {
        resultsString = PERSISTENCE_STRING;
      } else if(parameter.equalsIgnoreCase("default")){
        resultsString = INFO_STRING;
      } else if(parameter.equalsIgnoreCase("all")){
        resultsString = INFO_STRING;
      } else {
        resultsString = "";
      }
    } else {
      resultsString = INFO_STRING;
    }

    return RedisResponse.bulkString(resultsString);
  }
}
