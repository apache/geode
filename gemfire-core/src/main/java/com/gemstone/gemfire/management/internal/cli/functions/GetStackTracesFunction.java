/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.functions;


import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.OSProcess;
import com.gemstone.gemfire.management.internal.cli.domain.StackTracesPerMember;

public class GetStackTracesFunction extends FunctionAdapter implements
    InternalEntity {

  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    try {
      Cache cache = CacheFactory.getAnyInstance();
      String memberNameOrId = cache.getDistributedSystem().getDistributedMember().getName();
      
      if (memberNameOrId == null) {
        memberNameOrId = cache.getDistributedSystem().getDistributedMember().getId();
      }
      StackTracesPerMember stackTracePerMember = new StackTracesPerMember(memberNameOrId, OSProcess.zipStacks());
      context.getResultSender().lastResult(stackTracePerMember);
    } catch (Exception e) {
      context.getResultSender().sendException(e);
    } 
  }

  @Override
  public String getId() {
    // TODO Auto-generated method stub
    return GetStackTracesFunction.class.getName();
  }
}
