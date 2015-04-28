package com.gemstone.gemfire.management.internal.cli.multistep;

import com.gemstone.gemfire.management.cli.Result;


/**
 * 
 * 
 * @author tushark
 *
 */
public interface CLIStep {
  
  public Result exec();
  
  public String getName();

}
