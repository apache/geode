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
package org.apache.geode.modules.session;

import java.util.Map;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.InternalClientMembership;

public class GetQueueSize implements Function {
  public static final String ID = "get-queue-size";

  @Override
  @SuppressWarnings("unchecked")
  public void execute(FunctionContext context) {
    InternalCache cache = (InternalCache) context.getCache();
    Map clientProxyMembershipIDMap = InternalClientMembership.getClientQueueSizes(cache);
    context.getResultSender().lastResult(clientProxyMembershipIDMap);
  }

  @Override
  public String getId() {
    return ID;
  }
}
