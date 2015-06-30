/*========================================================================= 
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.connection;

import java.io.Serializable;

/**
 * @author hgadre
 *
 */
public class EndPoint implements Serializable {
  
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private String hostName;
  private int port;
  
  
  public EndPoint(String hostNm, int p) {
    super();
    hostName = hostNm;
    port = p;
  }
  
  public EndPoint() {
  }

  public String getHostName() {
    return hostName;
  }

  public void setHostName(String hostNm) {
    hostName = hostNm;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int p) {
    port = p;
  }
  
  @Override
  public String toString() {
    return "["+getHostName()+":"+getPort()+"]";
  }

}
