/*
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
 */

package org.apache.geode.management.configuration;

import static io.swagger.annotations.ApiModelProperty.AccessMode.READ_ONLY;

import java.io.File;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.swagger.annotations.ApiModelProperty;

import org.apache.geode.management.runtime.DeploymentInfo;

public class Deployment extends GroupableConfiguration<DeploymentInfo> implements HasFile {
  public static final String DEPLOYMENT_ENDPOINT = "/deployments";
  private String fileName;
  @ApiModelProperty(accessMode = READ_ONLY)
  private String deployedTime;
  @ApiModelProperty(accessMode = READ_ONLY)
  private String deployedBy;

  // the file is not serialized over the wire
  private transient File file;

  public Deployment() {}

  public Deployment(String fileName, String deployedBy, String deployedTime) {
    this.fileName = fileName;
    this.deployedBy = deployedBy;
    this.deployedTime = deployedTime;
  }

  @JsonIgnore
  public File getFile() {
    return file;
  }

  public void setFile(File file) {
    this.file = file;
    setFileName(file.getName());
  }

  @Override
  @JsonIgnore
  public String getId() {
    return getFileName();
  }

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String jarFileName) {
    this.fileName = jarFileName;
  }

  public String getDeployedTime() {
    return deployedTime;
  }

  /**
   * For internal use only
   */
  public void setDeployedTime(String deployedTime) {
    this.deployedTime = deployedTime;
  }

  public String getDeployedBy() {
    return deployedBy;
  }

  /**
   * For internal use only
   */
  public void setDeployedBy(String deployedBy) {
    this.deployedBy = deployedBy;
  }

  @Override
  public Links getLinks() {
    return new Links(getId(), DEPLOYMENT_ENDPOINT);
  }

  @Override
  public String toString() {
    return "Deployment{" +
        "jarFileName='" + fileName + '\'' +
        ", deployedTime='" + deployedTime + '\'' +
        ", deployedBy='" + deployedBy + '\'' +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Deployment that = (Deployment) o;
    return Objects.equals(fileName, that.fileName) &&
        Objects.equals(deployedTime, that.deployedTime) &&
        Objects.equals(deployedBy, that.deployedBy);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fileName, deployedTime, deployedBy);
  }
}
