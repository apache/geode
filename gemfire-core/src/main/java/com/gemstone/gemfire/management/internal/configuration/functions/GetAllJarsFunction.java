/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.configuration.functions;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.SharedConfiguration;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.management.internal.configuration.domain.Configuration;

public class GetAllJarsFunction extends FunctionAdapter implements
    InternalEntity {
  
  private static final long serialVersionUID = 1L;

  public GetAllJarsFunction() {
  }

  @Override
  public void execute(FunctionContext context)  {
    InternalLocator locator = (InternalLocator) Locator.getLocator();
    
    if (locator != null) {
      SharedConfiguration sharedConfig = locator.getSharedConfiguration();
      if (sharedConfig != null) {
        try {
          Map<String, Configuration> entireConfig  = sharedConfig.getEntireConfiguration();
          Set<String> configNames = entireConfig.keySet();
          context.getResultSender().lastResult(sharedConfig.getAllJars(configNames));
        } catch (IOException e) {
          context.getResultSender().sendException(e);
        } catch (Exception e) {
          context.getResultSender().sendException(e);
        }
      }
    }
    
    context.getResultSender().lastResult(null);
  }

  @Override
  public String getId() {
    return GetAllJarsFunction.class.getName();
  }

}
