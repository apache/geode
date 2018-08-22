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
package org.apache.geode.management.internal.cli.commands;

import java.util.List;
import java.util.Objects;

import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;

/**
 * Encapsulates common functionality for implementing command classes for the Geode shell (gfsh).
 * this provides wrapper around the static methods in CliUtils for easy mock of the commands
 *
 * this class should not have much implementation of its own other then those tested in
 * GfshCommandJUnitTest
 */
@SuppressWarnings("unused")
public abstract class InternalGfshCommand extends GfshCommand {

  public void persistClusterConfiguration(Result result, Runnable runnable) {
    if (result == null) {
      throw new IllegalArgumentException("Result should not be null");
    }
    if (getConfigurationPersistenceService() == null) {
      result.setCommandPersisted(false);
    } else {
      runnable.run();
      result.setCommandPersisted(true);
    }
  }

  public XmlEntity findXmlEntity(List<CliFunctionResult> functionResults) {
    return functionResults.stream().filter(CliFunctionResult::isSuccessful)
        .map(CliFunctionResult::getXmlEntity).filter(Objects::nonNull).findFirst().orElse(null);
  }

  public boolean isDebugging() {
    return getGfsh() != null && getGfsh().getDebug();
  }

  public boolean isLogging() {
    return getGfsh() != null;
  }

  public Gfsh getGfsh() {
    return Gfsh.getCurrentInstance();
  }

  public ManagementService getManagementService() {
    return ManagementService.getExistingManagementService(getCache());
  }

  @Override
  public InternalConfigurationPersistenceService getConfigurationPersistenceService() {
    return (InternalConfigurationPersistenceService) super.getConfigurationPersistenceService();
  }

}
