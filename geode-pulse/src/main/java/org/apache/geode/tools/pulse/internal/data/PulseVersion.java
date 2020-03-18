/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.geode.tools.pulse.internal.data;

import java.util.ResourceBundle;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Class PulseVersion
 *
 * This class is for holding Pulse Applications Version's details (like version details, build
 * details, source details, etc) from properties file
 *
 * @since GemFire version Helios
 */
@Component
public class PulseVersion {

  private final Repository repository;
  private String pulseVersion;

  @Autowired
  public PulseVersion(Repository repository) {
    this.repository = repository;
  }

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
    ResourceBundle resourceBundle = repository.getResourceBundle();
    return resourceBundle.getString("LOG_MSG_PULSE_VERSION") + " "
        + this.getPulseVersion() + " " + this.getPulseBuildId() + " " + this.getPulseBuildDate();
  }

}
