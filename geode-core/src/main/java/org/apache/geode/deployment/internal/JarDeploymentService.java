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
package org.apache.geode.deployment.internal;

import java.io.File;
import java.util.List;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.services.result.ServiceResult;

/**
 * Implementations of {@link JarDeploymentService} will be responsible for deploying jars into Geode
 * from both gfsh and the {@link ClusterManagementService}.
 *
 * @since Geode 1.15
 */
@Experimental
public interface JarDeploymentService {

  /**
   * Deploys the jar(s) represented by the given {@link Deployment}, making their content accessible
   * to the system.
   *
   * @param deployment a {@link Deployment} containing a jar or jars to be deployed as well as any
   *        other necessary information such as dependencies and a name.
   * @return a {@link ServiceResult} containing an updated {@link Deployment} representing the jars
   *         that were deployed when successful and an error message if deployment fails.
   */
  ServiceResult<Deployment> deploy(Deployment deployment);

  /**
   * Deploys a single {@link File}, without a {@link Deployment}, into the system.
   *
   * @param file a {@link File} which will be the jar to deploy.
   * @return a {@link ServiceResult} containing a {@link Deployment} representing the jar that was
   *         deployed when successful and an error message is deployment fails.
   */
  ServiceResult<Deployment> deploy(File file);

  /**
   * Removes jars from the system by their file name.
   *
   * @param fileName the name of a jar that has previously been deployed.
   * @return a {@link ServiceResult} containing a {@link Deployment} representing the removed jar
   *         when successful and an error message if the file could not be found or undeployed.
   */
  ServiceResult<Deployment> undeployByFileName(String fileName);

  /**
   * Lists all jars currently deployed in Geode.
   *
   * @return a {@link List} of {@link Deployment}s representing the jars that have been deployed
   *         into Geode.
   */
  List<Deployment> listDeployed();

  /**
   * Retrieves a {@link Deployment} by name.
   *
   * @param fileName the name of an existing {@link Deployment} to be returned.
   * @return a {@link ServiceResult} containing a {@link Deployment} when a {@link Deployment} is
   *         found fileName and an error message if one cannot be found.
   */
  ServiceResult<Deployment> getDeployed(String fileName);

  /**
   * Reconfigures the {@link JarDeploymentService} with a new working directory where jars will be
   * deployed to. This should only be called BEFORE deploying any jars into Geode.
   *
   * @param workingDirectory a {@link File} representing the directory that will contain jars that
   *        have been deployed.
   */
  void reinitializeWithWorkingDirectory(File workingDirectory);

  /**
   * Deploys all jars currently sitting in the working directory that are not officially deployed.
   * This is called when the system starts to load in previously deployed jars.
   */
  void loadJarsFromWorkingDirectory();

  /**
   * Undeploys all jars and shuts down the {@link JarDeploymentService}.
   */
  void close();
}
