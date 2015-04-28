/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli;

import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.shell.GfshExecutionStrategy;

/**
 * Interceptor interface which {@link GfshExecutionStrategy} can use to
 * intercept before & after actual command execution.
 * 
 * @author Abhishek Chaudhari
 * 
 * @since 7.0
 */
public interface CliAroundInterceptor {
  
  public Result preExecution(GfshParseResult parseResult);
  
  public Result postExecution(GfshParseResult parseResult, Result commandResult);

}
