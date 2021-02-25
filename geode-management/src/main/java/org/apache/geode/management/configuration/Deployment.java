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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.swagger.annotations.ApiModelProperty;

import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.api.CommandType;
import org.apache.geode.management.internal.utils.JarFileUtils;
import org.apache.geode.management.runtime.DeploymentInfo;

/**
 * This represents a jar that is to be deployed or is currently deployed. This class is used by the
 * JarDeploymentService to deploy jars from both the {@link ClusterManagementService} and gfsh.
 */
public class Deployment extends GroupableConfiguration<DeploymentInfo> implements HasFile {
  private static final long serialVersionUID = 6992732279452865384L;
  public static final String DEPLOYMENT_ENDPOINT = "/deployments";

  // Name given to this deployment by the --name option of the
  private String deploymentName;
  private String jarFileName;
  @ApiModelProperty(accessMode = READ_ONLY)
  private String deployedTime;
  @ApiModelProperty(accessMode = READ_ONLY)
  private String deployedBy;

  private final List<String> deploymentsToDependOn = new ArrayList<>();

  // the file is not serialized over the wire
  private transient File file;

  public Deployment() {}

  public Deployment(Deployment deployment) {
    this(deployment, deployment.file);
  }

  public Deployment(Deployment deployment, File file) {
    this(deployment.deploymentName, deployment.jarFileName, deployment.deployedBy,
        deployment.deployedTime, deployment.deploymentsToDependOn);
    this.file = file;
  }

  public Deployment(String jarFileName, String deployedBy, String deployedTime) {
    this(JarFileUtils.getArtifactId(jarFileName), jarFileName, deployedBy, deployedTime);
  }

  public Deployment(String deploymentName, String jarFileName, String deployedBy,
      String deployedTime, List<String> deploymentsToDependOn) {
    this(deploymentName, jarFileName, deployedBy, deployedTime);
    this.deploymentsToDependOn.addAll(deploymentsToDependOn);
  }

  public Deployment(String deploymentName, String jarFileName, String deployedBy,
      String deployedTime) {
    this.deploymentName = deploymentName;
    this.jarFileName = jarFileName;
    this.deployedBy = deployedBy;
    this.deployedTime = deployedTime;
  }

  public List<String> getModuleDependencyNames() {
    return deploymentsToDependOn;
  }

  public void setDeploymentName(String deploymentName) {
    this.deploymentName = deploymentName;
  }

  public void setDeploymentsToDependOn(List<String> deploymentsToDependOn) {
    this.deploymentsToDependOn.clear();
    this.deploymentsToDependOn.addAll(deploymentsToDependOn);
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
    return getDeploymentName();
  }

  public String getDeploymentName() {
    if (deploymentName != null) {
      return deploymentName;
    } else if (getFileName() != null) {
      return JarFileUtils.getArtifactId(getFileName());
    }
    return null;
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
        "deploymentName='" + deploymentName + '\'' +
        ", jarFileName='" + jarFileName + '\'' +
        ", filePath='" + getFilePath() + '\'' +
        ", deployedTime='" + deployedTime + '\'' +
        ", deployedBy='" + deployedBy + '\'' +
        ", deploymentsToDependOn='" + deploymentsToDependOn + '\'' +
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
    return jarFileName.equals(that.jarFileName) &&
        Objects.equals(deploymentName, that.deploymentName) &&
        Objects.equals(deployedTime, that.deployedTime) &&
        Objects.equals(deployedBy, that.deployedBy);
  }

  @Override
  public int hashCode() {
    return Objects.hash(deploymentName, jarFileName, deployedTime, deployedBy);
  }

  @Override
  public CommandType getCreationCommandType() {
    return CommandType.CREATE_OR_UPDATE;
  }
}
