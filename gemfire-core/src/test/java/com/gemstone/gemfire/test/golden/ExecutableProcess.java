package com.gemstone.gemfire.test.golden;

/**
 * Defines the work that is executed within a remote process. 
 */
public interface ExecutableProcess {
  public void executeInProcess() throws Exception;
}
