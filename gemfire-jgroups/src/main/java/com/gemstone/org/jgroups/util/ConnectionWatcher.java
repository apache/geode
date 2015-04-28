package com.gemstone.org.jgroups.util;

import java.net.Socket;

/**
 * ConnectionWatcher is used to observe tcp/ip connection formation in SockCreator
 * implementations.
 * 
 * @author bschuchardt
 *
 */
public interface ConnectionWatcher {
  /**
   * this is invoked with the connecting socket just prior to issuing
   * a connect() call.  It can be used to start another thread or task
   * to monitor the connection attempt.  
   */
  public void beforeConnect(Socket socket);
  
  /**
   * this is invoked after the connection attempt has finished.  It can
   * be used to cancel the task started by beforeConnect
   */
  public void afterConnect(Socket socket);
}
