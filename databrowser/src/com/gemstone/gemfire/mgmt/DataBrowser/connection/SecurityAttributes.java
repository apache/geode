/*========================================================================= 
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.connection;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author mjha
 * 
 */
public class SecurityAttributes implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  /**
   * path of security plugin
   */
  private String              securityPluginPath;
  /**
   * Additional security properties
   */
  private Map<String, String> securityProperties;
  
  private String              authImplString;

  public SecurityAttributes() {
    securityProperties = new HashMap<String, String>();
  }

  public String getSecurityPluginPath() {
    return securityPluginPath;
  }

  public void setSecurityPluginPath(String secPluginPath) {
    securityPluginPath = secPluginPath;
  }

  public Map<String, String> getSecurityProperties() {
    return securityProperties;
  }

  public void setSecurityProperties(Map<String, String> secProps) {
    if (secProps != null) {
      // TODO MGH - For now just to be safe. Not sure if this is thread safe if 
      // concurrent queries are allowed. 
      // TODO MGH - Should the existing security properties be flushed and the only 
      // thos specified in secProps be stored?
      synchronized( securityProperties ) {
        securityProperties.putAll(secProps);
      }
    }
  }

  public String getAuthImplString() {
    return authImplString;
  }

  public void setAuthImplString(String authImpl) {
    authImplString = authImpl;
  }

}
