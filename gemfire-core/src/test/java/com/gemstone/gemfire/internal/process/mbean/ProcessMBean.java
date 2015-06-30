package com.gemstone.gemfire.internal.process.mbean;

/**
 * Extracted from LocalProcessControllerDUnitTest.
 * 
 * @author Kirk Lund
 */
public interface ProcessMBean {
  public int getPid();
  public boolean isProcess();
  public void stop();
}
