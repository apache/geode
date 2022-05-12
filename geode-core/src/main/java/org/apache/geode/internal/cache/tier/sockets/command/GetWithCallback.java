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

package org.apache.geode.internal.cache.tier.sockets.command;

import java.io.IOException;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.SecurityService;

public class GetWithCallback extends AbstractGet {

  @Immutable
  private static final GetWithCallback singleton = new GetWithCallback();

  public static Command getCommand() {
    return singleton;
  }

  @Override
  protected void processRequest(final @NotNull Message request,
      final @NotNull ServerConnection serverConnection,
      final @NotNull SecurityService securityService, final @NotNull CacheServerStats stats,
      final long startTime, final long stepStartTime) throws IOException, ResponseException {

    final String regionName = request.getPart(0).getCachedString();
    final Object key = getObjectOrThrowResponseException(request.getPart(1));
    final Object callbackArg = getObjectOrThrowResponseException(request.getPart(2));

    processGetRequest(request, serverConnection, securityService, stats, startTime,
        stepStartTime, regionName, key, callbackArg);
  }

}
