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

import java.util.Arrays;
import java.util.List;

import org.apache.geode.redis.internal.ParameterRequirements.RedisParametersMismatchException;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class InfoExecutor extends AbstractExecutor {

  private final String CURRENT_REDIS_VERSION = "5.0.6";
  private ExecutionHandlerContext context = null;

  @Override
  public RedisResponse executeCommand(Command command,
                                      ExecutionHandlerContext context) {
    this.context = context;
    String resultsString;
    List<ByteArrayWrapper> commands =
        command.getProcessedCommandWrappers();

    if (containsTooManyParameters(commands)) {
      throw new RedisParametersMismatchException(RedisConstants.ERROR_SYNTAX);
    }

    if (containsOptionalParameter(commands)) {
      String parameter = commands.get(1).toString();
      resultsString = getReturnValueFor(parameter);
    } else {
      resultsString = getDefaultSectionsString();
    }

    return RedisResponse.bulkString(resultsString);
  }

  private String getReturnValueFor(String parameter) {
    parameter = parameter.toUpperCase();

    String resultsString;

    if (isAKnownSection(parameter)) {
      resultsString = getSectionInfo(parameter);
    } else if (isAnotherKnownOption(parameter)) {
      resultsString = getInfoForOption(parameter);
    } else {
      resultsString = "";
    }

    return resultsString;
  }

  private boolean isAKnownSection(String parameter) {
    List knownSections =
        Arrays.asList("SERVER", "CLUSTER", "PERSISTENCE");

    return knownSections.contains(parameter);
  }

  private boolean isAnotherKnownOption(String parameter) {
    List knownOptions =
        Arrays.asList("DEFAULT", "ALL");

    return knownOptions.contains(parameter);
  }

  private String getInfoForOption(String parameter) {
    String infoString = null;
    if (parameter.equals("DEFAULT")) {
      infoString = getDefaultSectionsString();
    } else if (parameter.equals("ALL")) {
      infoString = getDefaultSectionsString();
    }
    return infoString;
  }

  private String getSectionInfo(String section) {
    String sectionInfo = null;
    if (section.equalsIgnoreCase("server")) {
      sectionInfo = getServerSection();
    } else if (
        section.equalsIgnoreCase("cluster")) {
      sectionInfo = getClusterSection();
    } else if (section
        .equalsIgnoreCase("persistence")) {
      sectionInfo = getPersistenceSection();
    }
    return sectionInfo;
  }

  private boolean containsTooManyParameters(List<ByteArrayWrapper> commands) {
    return commands.size() > 2;
  }

  private boolean containsOptionalParameter(List<ByteArrayWrapper> commands) {
    return commands.size() == 2;
  }

  private String getDefaultSectionsString() {
    return getServerSection() + getSectionSeparator() +
        getPersistenceSection() + getSectionSeparator() +
        getClusterSection();
  }

  private String getSectionSeparator() {
    return "\r\n";
  }

  private String getClusterSection() {
    return "# Cluster\r\n" +
        "cluster_enabled:0\r\n";
  }

  private String getServerSection() {

    int TCP_PORT = this.context.getServerPort();

    return "# Server\r\n" +
        "redis_version:" + this.CURRENT_REDIS_VERSION + "\r\n" +
        "redis_mode:standalone\r\n" +
        "tcp_port:" + TCP_PORT + "\r\n";
  }

  private String getPersistenceSection() {
    return "# Persistence\r\n" +
        "loading:0\r\n";
  }
}
