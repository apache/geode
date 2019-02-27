package org.apache.geode.management.api;

public class GeodeManagementException extends RuntimeException {
  private String serverException;

  public GeodeManagementException(String serverException, String message) {
    super(message);
    this.serverException = serverException;
  }

  public String getServerException() {
    return serverException;
  }
}
