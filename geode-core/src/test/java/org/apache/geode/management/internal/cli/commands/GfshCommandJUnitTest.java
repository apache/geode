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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.management.ObjectName;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.exceptions.EntityNotFoundException;

public class GfshCommandJUnitTest {

  private String memberName = "memberONe";
  private String diskStoreName = "diskStoreOne";
  private GfshCommand command;

  @Before
  public void before() throws Exception {
    command = spy(GfshCommand.class);
  }

  @Test
  public void getMember() throws Exception {
    doReturn(null).when(command).findMember("test");
    assertThatThrownBy(() -> command.getMember("test")).isInstanceOf(EntityNotFoundException.class);
  }

  @Test
  public void getMembers() throws Exception {
    String[] members = {"member"};
    doReturn(Collections.emptySet()).when(command).findMembers(members, null);
    assertThatThrownBy(() -> command.getMembers(members, null))
        .isInstanceOf(EntityNotFoundException.class);
  }

  @Test
  public void getMembersIncludingLocators() throws Exception {
    String[] members = {"member"};
    doReturn(Collections.emptySet()).when(command).findMembersIncludingLocators(members, null);
    assertThatThrownBy(() -> command.getMembersIncludingLocators(members, null))
        .isInstanceOf(EntityNotFoundException.class);
  }

  @Test
  public void diskStoreBeanAndMemberBeanDiskStoreExists() throws Exception {
    Map<String, String[]> memberDiskStore = new HashMap<>();
    memberDiskStore.put(memberName, new String[] {diskStoreName});
    ObjectName objectName = new ObjectName("");

    DistributedSystemMXBean distributedSystemMXBean = Mockito.mock(DistributedSystemMXBean.class);
    doReturn(memberDiskStore).when(distributedSystemMXBean).listMemberDiskstore();
    doReturn(objectName).when(distributedSystemMXBean).fetchDiskStoreObjectName(any(), any());

    assertThat(command.diskStoreBeanAndMemberBeanDiskStoreExists(distributedSystemMXBean,
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

    assertThat(command.diskStoreBeanAndMemberBeanDiskStoreExists(distributedSystemMXBean,
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

    assertThat(command.diskStoreBeanAndMemberBeanDiskStoreExists(distributedSystemMXBean,
        memberName, diskStoreName)).isFalse();
  }

  @Test
  public void diskStoreBeanExistsMemberDiskStoreIsNull() throws Exception {
    ObjectName objectName = new ObjectName("");

    DistributedSystemMXBean distributedSystemMXBean = Mockito.mock(DistributedSystemMXBean.class);
    doReturn(null).when(distributedSystemMXBean).listMemberDiskstore();
    doReturn(objectName).when(distributedSystemMXBean).fetchDiskStoreObjectName(any(), any());

    assertThat(command.diskStoreBeanAndMemberBeanDiskStoreExists(distributedSystemMXBean,
        memberName, diskStoreName)).isFalse();
  }
}
