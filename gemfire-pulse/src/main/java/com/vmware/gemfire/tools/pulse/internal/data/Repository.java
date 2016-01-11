/*
 * =========================================================================
 *  Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.vmware.gemfire.tools.pulse.internal.data;

import java.net.ConnectException;
import java.util.HashMap;
import java.util.Locale;
import java.util.ResourceBundle;

import com.vmware.gemfire.tools.pulse.internal.log.PulseLogWriter;
import java.util.Iterator;
import java.util.Map;

/**
 * A Singleton instance of the memory cache for clusters.
 * 
 * @author Anand Hariharan
 * @since version 7.0.Beta 2012-09-23
 */
public class Repository {
  private PulseLogWriter LOGGER;

  private static Repository instance = new Repository();
  private HashMap<String, Cluster> clusterMap = new HashMap<String, Cluster>();
  private Boolean jmxUseLocator;
  private String jmxHost;
  private String jmxPort;
  private String jmxUserName;
  private String jmxUserPassword;
  private Boolean isEmbeddedMode;
  private boolean useSSLLocator = false;
  private boolean useSSLManager = false;
  

  private String pulseWebAppUrl;

  Locale locale = new Locale(PulseConstants.APPLICATION_LANGUAGE,
      PulseConstants.APPLICATION_COUNTRY);

  private ResourceBundle resourceBundle = ResourceBundle.getBundle(
      PulseConstants.LOG_MESSAGES_FILE, locale);

  private PulseConfig pulseConfig = new PulseConfig();

  private Repository() {

  }

  public static Repository get() {
    return instance;
  }

  public Boolean getJmxUseLocator() {
    return this.jmxUseLocator;
  }

  public void setJmxUseLocator(Boolean jmxUseLocator) {
    this.jmxUseLocator = jmxUseLocator;
  }

  public String getJmxHost() {
    return this.jmxHost;
  }

  public void setJmxHost(String jmxHost) {
    this.jmxHost = jmxHost;
  }

  public String getJmxPort() {
    return this.jmxPort;
  }

  public void setJmxPort(String jmxPort) {
    this.jmxPort = jmxPort;
  }

  public String getJmxUserName() {
    return this.jmxUserName;
  }

  public void setJmxUserName(String jmxUserName) {
    this.jmxUserName = jmxUserName;
  }

  public String getJmxUserPassword() {
    return this.jmxUserPassword;
  }

  public void setJmxUserPassword(String jmxUserPassword) {
    this.jmxUserPassword = jmxUserPassword;
  }

  public Boolean getIsEmbeddedMode() {
    return this.isEmbeddedMode;
  }

  public void setIsEmbeddedMode(Boolean isEmbeddedMode) {
    this.isEmbeddedMode = isEmbeddedMode;
  }

  public boolean isUseSSLLocator() {
    return useSSLLocator;
  }

  public void setUseSSLLocator(boolean useSSLLocator) {
    this.useSSLLocator = useSSLLocator;
  }

  public boolean isUseSSLManager() {
    return useSSLManager;
  }

  public void setUseSSLManager(boolean useSSLManager) {
    this.useSSLManager = useSSLManager;
  }

  public String getPulseWebAppUrl() {
    return this.pulseWebAppUrl;
  }

  public void setPulseWebAppUrl(String pulseWebAppUrl) {
    this.pulseWebAppUrl = pulseWebAppUrl;
  }

  public PulseConfig getPulseConfig() {
    return this.pulseConfig;
  }

  public void setPulseConfig(PulseConfig pulseConfig) {
    this.pulseConfig = pulseConfig;
  }

  /**
   * Convenience method for now, seeing that we're maintaining a 1:1 mapping
   * between webapp and cluster
   */
  public Cluster getCluster() {
    return this.getCluster(getJmxHost(), getJmxPort());
  }

  public Cluster getCluster(String host, String port) {
    synchronized (this.clusterMap) {
      String key = this.getClusterKey(host, port);
      Cluster data = this.clusterMap.get(key);

      LOGGER = PulseLogWriter.getLogger();

      if (data == null) {
        try {
          if (LOGGER.infoEnabled()) {
            LOGGER.info(resourceBundle.getString("LOG_MSG_CREATE_NEW_THREAD")
                + " : " + key);
          }
          data = new Cluster(host, port, this.getJmxUserName(), this.getJmxUserPassword());
          // Assign name to thread created
          data.setName(PulseConstants.APP_NAME + "-" + host + ":" + port);
          // Start Thread
          data.start();
          this.clusterMap.put(key, data);
        } catch (ConnectException e) {
          data = null;
          if (LOGGER.fineEnabled()) {
            LOGGER.fine(e.getMessage());
          }
        }
      }
      return data;
    }
  }

  private String getClusterKey(String host, String port) {
    return host + ":" + port;
  }

  // This method is used to remove all cluster threads
  public void removeAllClusters() {

    Iterator<Map.Entry<String, Cluster>> iter = clusterMap.entrySet()
        .iterator();

    while (iter.hasNext()) {
      Map.Entry<String, Cluster> entry = iter.next();
      Cluster c = entry.getValue();
      String clusterKey = entry.getKey();
      c.stopThread();
      iter.remove();
      if (LOGGER.infoEnabled()) {
        LOGGER.info(resourceBundle.getString("LOG_MSG_REMOVE_THREAD") + " : "
            + clusterKey.toString());
      }
    }
  }

  public ResourceBundle getResourceBundle() {
    return this.resourceBundle;
  }

}
