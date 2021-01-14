/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.deployment;

import java.io.File;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

public class NewDeployment {

  private String deploymentName;
  private List<String> jarFileNames;
  private List<String> deploymentsToDependOn;
  private String deployedTime;
  private transient List<File> files;

  public NewDeployment(String deploymentName, List<String> jarFileNames,
      String... deploymentsToDependOn) {
    this.deploymentName = deploymentName;
    this.jarFileNames = jarFileNames;
    this.deploymentsToDependOn = Arrays.asList(deploymentsToDependOn);
    this.deployedTime = Instant.now().toString();
  }

  public String getDeploymentName() {
    return deploymentName;
  }

  public void setDeploymentName(String deploymentName) {
    this.deploymentName = deploymentName;
  }

  public List<String> getJarFileNames() {
    return jarFileNames;
  }

  public String getDeployedTime() {
    return deployedTime;
  }

  public void setJarFileNames(List<String> jarFileNames) {
    this.jarFileNames = jarFileNames;
  }

  public List<String> getDeploymentsToDependOn() {
    return deploymentsToDependOn;
  }

  public void setDeploymentsToDependOn(List<String> deploymentsToDependOn) {
    this.deploymentsToDependOn = deploymentsToDependOn;
  }

  public List<File> getFiles() {
    return files;
  }

  public void setFiles(List<File> files) {
    this.files = files;
  }
}
