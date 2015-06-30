/*========================================================================= 
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.connection;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author hgadre
 *
 */
public class ClientConfiguration implements Serializable {
  
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private List<EndPoint> locators;
  private List<EndPoint> cacheServers;
  /**
   * path of security plugin
   */
  private String   securityPluginPath;
  /**
   * Additional security properties
   */
  private Map<String, String> securityProperties;
  
  public ClientConfiguration() {
   this.locators = new ArrayList<EndPoint>();
   this.cacheServers = new ArrayList<EndPoint>();  
   this.securityProperties = new HashMap<String, String>();
  }
  
  public List<EndPoint> getCacheServers() {
    return this.cacheServers;
  }
  
  public List<EndPoint> getLocators() {
    return locators;
  }
  
  public void setCacheServers(List<EndPoint> cacheSvrs) {
    cacheServers.clear();
    cacheServers.addAll(cacheSvrs);
  }
  
  public void setLocators(List<EndPoint> lctrs) {
    locators.clear();
    locators.addAll(lctrs);
  }
  
  public void addCacheServer(EndPoint pt) {
    cacheServers.add(pt);
  }
  
  public void addLocator(EndPoint pt) {
    locators.add(pt);
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
    if(secProps != null) {
      securityProperties.putAll(secProps);    
    }
  }

}
