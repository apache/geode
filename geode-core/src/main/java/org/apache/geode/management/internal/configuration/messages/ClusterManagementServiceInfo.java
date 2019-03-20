package org.apache.geode.management.internal.configuration.messages;

import java.io.Serializable;

public class ClusterManagementServiceInfo implements Serializable {
  private String hostName;
  // -1 indicates the service is not running
  private int httpPort = -1;
  // TODO: add isSSL and isSecured information later

  public int getHttpPort() {
    return httpPort;
  }

  public void setHttpPort(int httpPort) {
    this.httpPort = httpPort;
  }

  public String getHostName() {
    return hostName;
  }

  public void setHostName(String hostName) {
    this.hostName = hostName;
  }

  public boolean isRunning() {
    return httpPort > 0;
  }
}
