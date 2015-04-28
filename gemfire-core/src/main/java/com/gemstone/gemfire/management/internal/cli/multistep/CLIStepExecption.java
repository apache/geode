/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.multistep;

import com.gemstone.gemfire.management.cli.Result;

public class CLIStepExecption extends RuntimeException {
  
  
  private Result result;
  
  
  public CLIStepExecption(Result result){
    this.result = result;
  }


  public Result getResult() {
    return result;
  }
  
  

}
