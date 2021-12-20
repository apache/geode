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
package org.apache.geode.internal.cache.persistence;

import static org.apache.geode.internal.cache.persistence.DefaultDiskDirs.getDefaultDiskDirs;
import static org.apache.geode.internal.lang.SystemPropertyHelper.DEFAULT_DISK_DIRS_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import org.apache.geode.internal.lang.SystemProperty;


public class DefaultDiskDirsTest {

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Test
  public void getDefaultDiskDirsReturnsTheDefault() throws Exception {
    assertThat(getDefaultDiskDirs()).isEqualTo(new File[] {new File(".")});
  }

  @Test
  public void getDefaultDiskDirsReturnsOverriddenValue() {
    System.setProperty(SystemProperty.DEFAULT_PREFIX + DEFAULT_DISK_DIRS_PROPERTY,
        "/FullyQualifiedPath");
    assertThat(getDefaultDiskDirs()).isEqualTo(new File[] {new File("/FullyQualifiedPath")});
  }
}
