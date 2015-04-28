package com.gemstone.gemfire.internal.process;

/**
 * A listener for events that should be reported to the users
 * console during the startup of a gemfire member.
 */
public interface StartupStatusListener {

  /**
   * Report the current status of system startup. The status
   * message reported to this method should already be internationalized. 
   */
  public void setStatus(String status);
}
