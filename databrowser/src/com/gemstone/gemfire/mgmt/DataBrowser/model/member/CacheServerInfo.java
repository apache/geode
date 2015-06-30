/*========================================================================= 
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.model.member;

/**
 * This class provides the information related to a GemFire Cache server.
 * 
 * @author Hrishi
 **/
public class CacheServerInfo {

  private String bindAddress;
  private int    port;
  private boolean notifyBySubscription;

  public CacheServerInfo() {
    this.bindAddress = null;
    this.port = -1;
    this.notifyBySubscription = false;
  }

  public CacheServerInfo(String bindAddr, int p, boolean ntfyBySubscr) {
    super();
    bindAddress = bindAddr;
    notifyBySubscription = ntfyBySubscr;
    port = p;
  }

  /**
   * Getter of the property <tt>bindAddress</tt>
   * 
   * @return Returns the bindAddress.
   * 
   */
  public String getBindAddress() {
    return bindAddress;
  }

  /**
   * Setter of the property <tt>bindAddress</tt>
   * 
   * @param bindAddr
   *          The bindAddress to set.
   **/
  public void setBindAddress(String bindAddr) {
    this.bindAddress = bindAddr;
  }

  /**
   * Getter of the property <tt>port</tt>
   * 
   * @return Returns the port.
   **/
  public int getPort() {
    return port;
  }

  /**
   * Setter of the property <tt>port</tt>
   * 
   * @param p
   *          The port to set.
   **/
  public void setPort(int p) {
    port = p;
  }
  
  public boolean isNotifyBySubscription() {
    return notifyBySubscription;
  }
  
  public void setNotifyBySubscription(boolean ntfyBySubscrp) {
    notifyBySubscription = ntfyBySubscrp;
  }
  
  @Override
  public String toString() {
    StringBuffer buff = new StringBuffer();
    buff.append("CacheServerInfo ["); 
    buff.append("Host: " + this.bindAddress); 
    buff.append(", Port: " + this.port);
    buff.append("]");
    return buff.toString();
  }
}