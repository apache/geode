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

import static io.swagger.v3.oas.annotations.media.Schema.AccessMode.READ_ONLY;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.swagger.v3.oas.annotations.media.Schema;

import org.apache.geode.management.api.CommandType;
import org.apache.geode.management.runtime.DeploymentInfo;

public class Deployment extends GroupableConfiguration<DeploymentInfo> implements HasFile {
  private static final long serialVersionUID = 6992732279452865384L;
  public static final String DEPLOYMENT_ENDPOINT = "/deployments";
  private String jarFileName;
  @Schema(accessMode = READ_ONLY)
  private String deployedTime;
  @Schema(accessMode = READ_ONLY)
  private String deployedBy;
  private List<String> dependencies;
  private String applicationName;

  // the file is not serialized over the wire
  private transient File file;

  public Deployment() {
    dependencies = Collections.emptyList();
  }

  public Deployment(String jarFileName, String deployedBy, String deployedTime,
      List<String> dependencies, String applicationName) {
    this.jarFileName = jarFileName;
    this.deployedBy = deployedBy;
    this.deployedTime = deployedTime;
    this.dependencies = new LinkedList<>(dependencies);
    this.applicationName = applicationName;
  }

  public Deployment(String jarFileName, String deployedBy, String deployedTime,
      String applicationName) {
    this(jarFileName, deployedBy, deployedTime, Collections.emptyList(), applicationName);
  }

  public Deployment(Deployment deployment, File jarFile) {
    this(deployment.getFileName(), deployment.getDeployedBy(), deployment.deployedTime,
        deployment.getApplicationName());
    file = jarFile;
  }

  public Deployment(String name, String deployedBy, String deployedTime) {
    this(name, deployedBy, deployedTime, null);
  }

  public String getApplicationName() {
    return applicationName;
  }

  @JsonIgnore
  public File getFile() {
    return file;
  }

  @JsonIgnore
  public String getFilePath() {
    if (file == null) {
      return "No file set";
    }
    try {
      return file.getCanonicalPath();
    } catch (IOException e) {
      return "Unknown path";
    }
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
    return jarFileName;
  }

  public void setFileName(String jarFileName) {
    this.jarFileName = jarFileName;
  }

  public String getDeployedTime() {
    return deployedTime;
  }

  /**
   * For internal use only
   *
   * @param deployedTime the deployed time value
   */
  public void setDeployedTime(String deployedTime) {
    this.deployedTime = deployedTime;
  }

  public String getDeployedBy() {
    return deployedBy;
  }

  /**
   * For internal use only
   *
   * @param deployedBy the deployed by value
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
        "jarFileName='" + jarFileName + '\'' +
        ", deployedTime='" + deployedTime + '\'' +
        ", deployedBy='" + deployedBy + '\'' +
        '}';
  }

  @Override
  public CommandType getCreationCommandType() {
    return CommandType.CREATE_OR_UPDATE;
  }

  public List<String> getDependencies() {
    return dependencies;
  }

  public void setDependencies(List<String> dependencies) {
    this.dependencies = dependencies;
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
    return Objects.equals(jarFileName, that.jarFileName) &&
        Objects.equals(deployedTime, that.deployedTime) &&
        Objects.equals(deployedBy, that.deployedBy);
  }

  @Override
  public int hashCode() {
    return Objects.hash(jarFileName, deployedTime, deployedBy);
  }
}
