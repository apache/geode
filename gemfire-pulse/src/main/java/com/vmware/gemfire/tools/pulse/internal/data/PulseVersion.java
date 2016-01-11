/*
 * =========================================================================
 *  Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.vmware.gemfire.tools.pulse.internal.data;

import java.util.ResourceBundle;

/**
 * Class PulseVersion
 * 
 * This class is for holding Pulse Applications Version's details (like version
 * details, build details, source details, etc) from properties file
 * 
 * @author Sachin K
 * @since version Helios
 */

public class PulseVersion {

  private String pulseVersion;

  public String getPulseVersion() {
    return pulseVersion;
  }

  public void setPulseVersion(String pulseVersion) {
    this.pulseVersion = pulseVersion;
  }

  private String pulseBuildId;

  public String getPulseBuildId() {
    return pulseBuildId;
  }

  public void setPulseBuildId(String pulseBuildId) {
    this.pulseBuildId = pulseBuildId;
  }

  private String pulseBuildDate;

  public String getPulseBuildDate() {
    return pulseBuildDate;
  }

  public void setPulseBuildDate(String pulseBuildDate) {
    this.pulseBuildDate = pulseBuildDate;
  }

  private String pulseSourceDate;

  public String getPulseSourceDate() {
    return pulseSourceDate;
  }

  public void setPulseSourceDate(String pulseSourceDate) {
    this.pulseSourceDate = pulseSourceDate;
  }

  private String pulseSourceRevision;

  public String getPulseSourceRevision() {
    return pulseSourceRevision;
  }

  public void setPulseSourceRevision(String pulseSourceRevision) {
    this.pulseSourceRevision = pulseSourceRevision;
  }

  private String pulseSourceRepository;

  public String getPulseSourceRepository() {
    return pulseSourceRepository;
  }

  public void setPulseSourceRepository(String pulseSourceRepository) {
    this.pulseSourceRepository = pulseSourceRepository;
  }

  public String getPulseVersionLogMessage() {
    ResourceBundle resourceBundle = Repository.get().getResourceBundle();
    String logMessage = resourceBundle.getString("LOG_MSG_PULSE_VERSION") + " "
        + this.getPulseVersion() + " " + this.getPulseBuildId() + " "
        + this.getPulseBuildDate();
    return logMessage;
  }

}
