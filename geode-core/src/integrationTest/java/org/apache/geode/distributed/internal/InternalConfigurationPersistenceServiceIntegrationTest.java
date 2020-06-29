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

package org.apache.geode.distributed.internal;

import static org.apache.geode.distributed.ConfigurationProperties.LOAD_CLUSTER_CONFIGURATION_FROM_DIR;
import static org.apache.geode.distributed.internal.InternalConfigurationPersistenceService.CLUSTER_CONFIG_ARTIFACTS_DIR_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.UncheckedIOException;
import java.nio.file.Path;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.junit.rules.LocatorStarterRule;

public class InternalConfigurationPersistenceServiceIntegrationTest {
  @Rule
  public LocatorStarterRule locator = new LocatorStarterRule();

  @Test
  public void loadConfigFromDirWhenDirDoesNotExist() {
    // first make sure config dir does not exist on disk
    Path configDir = locator.getWorkingDir().toPath().resolve(CLUSTER_CONFIG_ARTIFACTS_DIR_NAME);
    assertThat(configDir).doesNotExist();

    // when the dir does not exist but load_cluster_configuration_from_dir is set to true, the
    // locator should not be able to start
    assertThatThrownBy(
        () -> locator.withProperty(LOAD_CLUSTER_CONFIGURATION_FROM_DIR, "true").startLocator())
            .isInstanceOf(UncheckedIOException.class)
            .hasMessageContaining("ConfigDir does not exist:");
  }
}
