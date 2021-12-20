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

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.internal.lang.SystemProperty;
import org.apache.geode.test.junit.categories.PersistenceTest;

@Category({PersistenceTest.class})
public class DefaultDiskDirsIntegrationTest {

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void getDefaultDiskDirsReturnsOverriddenValue() {
    System.setProperty(SystemProperty.DEFAULT_PREFIX + DEFAULT_DISK_DIRS_PROPERTY,
        temporaryFolder.getRoot().getAbsolutePath());
    assertThat(getDefaultDiskDirs()[0]).exists().isEqualTo(temporaryFolder.getRoot());
  }
}
