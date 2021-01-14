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
package org.apache.geode.deployment.impl.modular;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.geode.deployment.JarDeploymentService;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.services.result.ServiceResult;

public class ModularJarDeploymentService implements JarDeploymentService {

  @Override
  public ServiceResult<Map<String, String>> deploy(Deployment deployment) {
    return null;
  }

  @Override
  public ServiceResult<Map<String, String>> deploy(File file) {
    return null;
  }

  @Override
  public ServiceResult<String> undeployByDeploymentName(String deploymentName) {
    return null;
  }

  @Override
  public ServiceResult<String> undeployByFileName(String fileName) {
    return null;
  }

  @Override
  public List<Deployment> listDeployed() {
    return null;
  }

  @Override
  public ServiceResult<Deployment> getDeployed(String deploymentName) {
    return null;
  }

  @Override
  public void setWorkingDirectory(File workingDirectory) {

  }

  @Override
  public Map<Path, File> backupJars(Path backupDirectory) {
    return null;
  }

  @Override
  public void loadPreviouslyDeployedJarsFromDisk() {

  }
}
