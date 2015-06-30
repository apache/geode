/*========================================================================= 
 * (c)Copyright 2002-2011, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.connection;

/**
 * Encapsulation the necessary parameter to establish connection with DS
 * 
 * @author mjha
 * 
 */
public class DSConfiguration {
  /**
   * Jmx Agent Host
   */
  private String host;
  /**
   * Jmx agent port
   */
  private int port;
  
  private String version;
  
  private SecurityAttributes      secAttributes;
  
  private String userName;
  
  private String password;

  public DSConfiguration() {

  }

  public String getHost() {
    return host;
  }

  public void setHost(String h) {
    host = h;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int p) {
    port = p;
  }

  /**
   * @return the version
   */
  public String getVersion() {
    return version;
  }

  /**
   * @param version the version to set
   */
  public void setVersion(String version) {
    this.version = version;
  }

  public SecurityAttributes getSecAttributes() {
    return secAttributes;
  }

  public void setSecAttributes(SecurityAttributes secAttr) {
    secAttributes = secAttr;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }
  
  
}
