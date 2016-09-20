/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.geode.tools.pulse.internal.data;

import org.apache.geode.tools.pulse.internal.log.PulseLogWriter;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import java.net.ConnectException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;

/**
 * A Singleton instance of the memory cache for clusters.
 * 
 * @since GemFire version 7.0.Beta 2012-09-23
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
  private boolean useGemFireCredentials = false;
  

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
   * we're maintaining a 1:1 mapping between webapp and cluster, there is no need for a map of clusters based on the host and port
   * We are using this clusterMap to maintain cluster for different users now.
   * For a single-user connection to gemfire JMX, we will use the default username/password in the pulse.properties
   * (# JMX User Properties )
   * pulse.jmxUserName=admin
   * pulse.jmxUserPassword=admin
   *
   * But for multi-user connections to gemfireJMX, i.e pulse that uses gemfire integrated security, we will need to get the username form the context
   */
  public Cluster getCluster() {
    String username = null;
    String password = null;
    if(useGemFireCredentials) {
      Authentication auth = SecurityContextHolder.getContext().getAuthentication();
      if(auth!=null) {
        username = auth.getName();
        password = (String) auth.getCredentials();
      }
    }
    else{
      username = this.jmxUserName;
      password = this.jmxUserPassword;
    }
    return this.getCluster(username, password);
  }

  public Cluster getCluster(String username, String password) {
    synchronized (this.clusterMap) {
      String key = username;
      Cluster data = this.clusterMap.get(key);

      LOGGER = PulseLogWriter.getLogger();

      if (data == null) {
        try {
          if (LOGGER.infoEnabled()) {
            LOGGER.info(resourceBundle.getString("LOG_MSG_CREATE_NEW_THREAD")
                + " : " + key);
          }
          data = new Cluster(this.jmxHost, this.jmxPort, username, password);
          // Assign name to thread created
          data.setName(PulseConstants.APP_NAME + "-" + this.jmxHost + ":" + this.jmxPort + ":" + username);
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

  public boolean isUseGemFireCredentials() {
    return useGemFireCredentials;
  }

  public void setUseGemFireCredentials(boolean useGemFireCredentials) {
    this.useGemFireCredentials = useGemFireCredentials;
  }
  
  

}
