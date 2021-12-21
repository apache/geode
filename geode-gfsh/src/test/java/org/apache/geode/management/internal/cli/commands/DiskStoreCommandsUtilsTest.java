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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

import java.util.HashMap;
import java.util.Map;

import javax.management.ObjectName;

import org.junit.Test;
import org.mockito.Mockito;

import org.apache.geode.management.DistributedSystemMXBean;

public class DiskStoreCommandsUtilsTest {
  private final String memberName = "memberONe";
  private final String diskStoreName = "diskStoreOne";

  @Test
  public void diskStoreBeanAndMemberBeanDiskStoreExists() throws Exception {
    Map<String, String[]> memberDiskStore = new HashMap<>();
    memberDiskStore.put(memberName, new String[] {diskStoreName});
    ObjectName objectName = new ObjectName("");

    DistributedSystemMXBean distributedSystemMXBean = Mockito.mock(DistributedSystemMXBean.class);
    doReturn(memberDiskStore).when(distributedSystemMXBean).listMemberDiskstore();
    doReturn(objectName).when(distributedSystemMXBean).fetchDiskStoreObjectName(any(), any());

    assertThat(
        DiskStoreCommandsUtils.diskStoreBeanAndMemberBeanDiskStoreExists(distributedSystemMXBean,
            memberName, diskStoreName)).isTrue();
  }

  @Test
  public void diskStoreBeanExistsAndMemberDiskStoreNotFound() throws Exception {
    Map<String, String[]> memberDiskStore = new HashMap<>();
    memberDiskStore.put(memberName, new String[] {});
    ObjectName objectName = new ObjectName("");

    DistributedSystemMXBean distributedSystemMXBean = Mockito.mock(DistributedSystemMXBean.class);
    doReturn(memberDiskStore).when(distributedSystemMXBean).listMemberDiskstore();
    doReturn(objectName).when(distributedSystemMXBean).fetchDiskStoreObjectName(any(), any());

    assertThat(
        DiskStoreCommandsUtils.diskStoreBeanAndMemberBeanDiskStoreExists(distributedSystemMXBean,
            memberName, diskStoreName)).isFalse();
  }

  @Test
  public void diskStoreBeanNotFoundAndMemberDiskStoreExists() throws Exception {
    Map<String, String[]> memberDiskStore = new HashMap<>();
    memberDiskStore.put(memberName, new String[] {diskStoreName});

    DistributedSystemMXBean distributedSystemMXBean = Mockito.mock(DistributedSystemMXBean.class);
    doReturn(memberDiskStore).when(distributedSystemMXBean).listMemberDiskstore();
    doThrow(new Exception("not found")).when(distributedSystemMXBean)
        .fetchDiskStoreObjectName(any(), any());

    assertThat(
        DiskStoreCommandsUtils.diskStoreBeanAndMemberBeanDiskStoreExists(distributedSystemMXBean,
            memberName, diskStoreName)).isFalse();
  }

  @Test
  public void diskStoreBeanExistsMemberDiskStoreIsNull() throws Exception {
    ObjectName objectName = new ObjectName("");

    DistributedSystemMXBean distributedSystemMXBean = Mockito.mock(DistributedSystemMXBean.class);
    doReturn(null).when(distributedSystemMXBean).listMemberDiskstore();
    doReturn(objectName).when(distributedSystemMXBean).fetchDiskStoreObjectName(any(), any());

    assertThat(
        DiskStoreCommandsUtils.diskStoreBeanAndMemberBeanDiskStoreExists(distributedSystemMXBean,
            memberName, diskStoreName)).isFalse();
  }

  @Test
  public void diskStoreBeanExistsMemberDiskStoreContainsNullArray() throws Exception {
    Map<String, String[]> memberDiskStore = new HashMap<>();
    memberDiskStore.put(memberName, null);
    ObjectName objectName = new ObjectName("");

    DistributedSystemMXBean distributedSystemMXBean = Mockito.mock(DistributedSystemMXBean.class);
    doReturn(memberDiskStore).when(distributedSystemMXBean).listMemberDiskstore();
    doReturn(objectName).when(distributedSystemMXBean).fetchDiskStoreObjectName(any(), any());

    assertThat(
        DiskStoreCommandsUtils.diskStoreBeanAndMemberBeanDiskStoreExists(distributedSystemMXBean,
            memberName, diskStoreName)).isFalse();
  }

  @Test
  public void diskStoreBeanExistsMemberDiskStoreNamesHasNullValues() throws Exception {
    Map<String, String[]> memberDiskStore = new HashMap<>();
    memberDiskStore.put(memberName, new String[] {null, null});
    ObjectName objectName = new ObjectName("");

    DistributedSystemMXBean distributedSystemMXBean = Mockito.mock(DistributedSystemMXBean.class);
    doReturn(memberDiskStore).when(distributedSystemMXBean).listMemberDiskstore();
    doReturn(objectName).when(distributedSystemMXBean).fetchDiskStoreObjectName(any(), any());

    assertThat(
        DiskStoreCommandsUtils.diskStoreBeanAndMemberBeanDiskStoreExists(distributedSystemMXBean,
            memberName, diskStoreName)).isFalse();
  }
}
