package com.gemstone.org.jgroups.debug;

public interface JChannelTestHook {
  /**
   * test hook invoked before JChannel is closing. 
   */
  public void beforeChannelClosing(String string, Throwable cause);

  /**
   * Must be called after purpose of this test hook is achieved.
   */
  public void reset();

}
