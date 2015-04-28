/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.functions;

import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.management.internal.configuration.domain.SharedConfigurationStatus;

public class FetchSharedConfigurationStatusFunction extends FunctionAdapter implements
    InternalEntity {

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
    
    CliFunctionResult result = new CliFunctionResult(memberId, new String[]{status.name()});
    context.getResultSender().lastResult(result);
  }

  @Override
  public String getId() {
    return FetchSharedConfigurationStatusFunction.class.getName();
  }

}
