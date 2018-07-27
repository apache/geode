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
package org.apache.geode.internal.cache;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;


public class DiskStoreMonitorTest {

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setup() {
    System.setProperty(DiskStoreMonitor.DISK_USAGE_DISABLE_MONITORING, "true");
  }

  @Test
  public void usesCurrentDirWhenLogFileIsNull() {
    DiskStoreMonitor diskStoreMonitor = new DiskStoreMonitor(null);
    assertThat(diskStoreMonitor.getLogDisk().dir()).isEqualTo(new File("."));
  }

  @Test
  public void usesLogFileParentDir() {
    DiskStoreMonitor diskStoreMonitor = new DiskStoreMonitor(new File("parent", "child"));
    assertThat(diskStoreMonitor.getLogDisk().dir()).isEqualTo(new File("parent"));
  }

  @Test
  public void usesCurrentDirWhenLogFileParentIsNull() {
    DiskStoreMonitor diskStoreMonitor = new DiskStoreMonitor(new File("child"));
    assertThat(diskStoreMonitor.getLogDisk().dir()).isEqualTo(new File("."));
  }
}
