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
package org.apache.geode.management.internal.cli.functions;

import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.management.internal.configuration.domain.SharedConfigurationStatus;

public class FetchSharedConfigurationStatusFunction extends FunctionAdapter
    implements InternalEntity {

  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    InternalLocator locator = InternalLocator.getLocator();
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    DistributedMember member = cache.getDistributedSystem().getDistributedMember();
    SharedConfigurationStatus status = locator.getSharedConfigurationStatus().getStatus();

    String memberId = member.getName();
    if (StringUtils.isBlank(memberId)) {
      memberId = member.getId();
    }

    CliFunctionResult result = new CliFunctionResult(memberId, new String[] {status.name()});
    context.getResultSender().lastResult(result);
  }

  @Override
  public String getId() {
    return FetchSharedConfigurationStatusFunction.class.getName();
  }

}
