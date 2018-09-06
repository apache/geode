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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.util.CollectionUtils;
import org.apache.geode.management.internal.cli.domain.DiskStoreDetails;
import org.apache.geode.management.internal.cli.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.cli.functions.DescribeDiskStoreFunction;
import org.apache.geode.management.internal.cli.functions.ListDiskStoresFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;

/**
 * The DiskStoreCommandsJUnitTest class is a test suite of test cases testing the contract and
 * functionality of the command classes relating to disk stores that implement commands in the
 * GemFire shell (gfsh) that access and modify disk stores in GemFire.
 *
 * @see org.apache.geode.management.internal.cli.commands.DescribeDiskStoreCommand
 * @see org.apache.geode.management.internal.cli.commands.ListDiskStoresCommand
 * @see org.apache.geode.management.internal.cli.domain.DiskStoreDetails
 * @see org.apache.geode.management.internal.cli.functions.DescribeDiskStoreFunction
 * @see org.apache.geode.management.internal.cli.functions.ListDiskStoresFunction
 * @see org.junit.Test
 *
 * @since GemFire 7.0
 */
public class DiskStoreCommandsJUnitTest {
  private AbstractExecution mockFunctionExecutor;
  private ResultCollector mockResultCollector;
  private DistributedMember mockDistributedMember;
  private ListDiskStoresCommand listDiskStoresCommand;
  private DescribeDiskStoreCommand describeDiskStoreCommand;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    listDiskStoresCommand = spy(ListDiskStoresCommand.class);
    describeDiskStoreCommand = spy(DescribeDiskStoreCommand.class);
    InternalCache mockCache = mock(InternalCache.class, "InternalCache");
    mockResultCollector = mock(ResultCollector.class, "ResultCollector");
    mockFunctionExecutor = mock(AbstractExecution.class, "Function Executor");
    mockDistributedMember = mock(DistributedMember.class, "DistributedMember");

    when(mockFunctionExecutor.setArguments(any())).thenReturn(mockFunctionExecutor);

    when(listDiskStoresCommand.getCache()).thenReturn(mockCache);
    doReturn(mockDistributedMember).when(listDiskStoresCommand).getMember(anyString());
    doReturn(Collections.singleton(mockDistributedMember)).when(listDiskStoresCommand)
        .getAllMembers();
    doReturn(mockFunctionExecutor).when(listDiskStoresCommand).getMembersFunctionExecutor(any());

    when(describeDiskStoreCommand.getCache()).thenReturn(mockCache);
    doReturn(mockDistributedMember).when(describeDiskStoreCommand).getMember(anyString());
    doReturn(Collections.singleton(mockDistributedMember)).when(describeDiskStoreCommand)
        .getAllMembers();
    doReturn(mockFunctionExecutor).when(describeDiskStoreCommand).getMembersFunctionExecutor(any());
  }

  private DiskStoreDetails createDiskStoreDetails(final String memberId,
      final String diskStoreName) {
    return new DiskStoreDetails(diskStoreName, memberId);
  }

  @Test
  public void testGetDiskStoreDescription() {
    final String memberId = "mockMember";
    final String diskStoreName = "mockDiskStore";
    final DiskStoreDetails expectedDiskStoredDetails =
        createDiskStoreDetails(memberId, diskStoreName);
    when(mockFunctionExecutor.execute(any(DescribeDiskStoreFunction.class)))
        .thenReturn(mockResultCollector);
    when(mockResultCollector.getResult())
        .thenReturn(Collections.singletonList(expectedDiskStoredDetails));

    final DiskStoreDetails actualDiskStoreDetails =
        describeDiskStoreCommand.getDiskStoreDescription(memberId, diskStoreName);
    AssertionsForClassTypes.assertThat(actualDiskStoreDetails).isNotNull();
    AssertionsForClassTypes.assertThat(actualDiskStoreDetails).isEqualTo(expectedDiskStoredDetails);
  }

  @Test
  public void testGetDiskStoreDescriptionThrowsEntityNotFoundException() {
    final String memberId = "mockMember";
    final String diskStoreName = "mockDiskStore";
    when(mockFunctionExecutor.execute(any(DescribeDiskStoreFunction.class)))
        .thenThrow(new EntityNotFoundException("Mock Exception"));

    assertThatThrownBy(
        () -> describeDiskStoreCommand.getDiskStoreDescription(memberId, diskStoreName))
            .isInstanceOf(EntityNotFoundException.class).hasMessage("Mock Exception");
  }

  @Test
  public void testGetDiskStoreDescriptionThrowsRuntimeException() {
    final String memberId = "mockMember";
    final String diskStoreName = "mockDiskStore";
    when(mockFunctionExecutor.execute(any(DescribeDiskStoreFunction.class)))
        .thenThrow(new RuntimeException("Mock Exception"));

    assertThatThrownBy(
        () -> describeDiskStoreCommand.getDiskStoreDescription(memberId, diskStoreName))
            .isInstanceOf(RuntimeException.class).hasMessage("Mock Exception");
  }

  @Test
  public void testGetDiskStoreDescriptionWithInvalidFunctionResultReturnType() {
    final String memberId = "mockMember";
    final String diskStoreName = "mockDiskStore";
    when(mockFunctionExecutor.execute(any(DescribeDiskStoreFunction.class)))
        .thenReturn(mockResultCollector);
    when(mockResultCollector.getResult()).thenReturn(Collections.singletonList(new Object()));

    assertThatThrownBy(
        () -> describeDiskStoreCommand.getDiskStoreDescription(memberId, diskStoreName))
            .isInstanceOf(RuntimeException.class).hasNoCause().hasMessage(
                CliStrings.format(CliStrings.UNEXPECTED_RETURN_TYPE_EXECUTING_COMMAND_ERROR_MESSAGE,
                    Object.class.getName(), CliStrings.DESCRIBE_DISK_STORE));
  }

  @Test
  public void testGetDiskStoreList() {
    final DiskStoreDetails diskStoreDetails1 =
        createDiskStoreDetails("memberOne", "cacheServer1DiskStore1");
    final DiskStoreDetails diskStoreDetails2 =
        createDiskStoreDetails("memberOne", "cacheServer1DiskStore2");
    final DiskStoreDetails diskStoreDetails3 =
        createDiskStoreDetails("memberTwo", "cacheServer2DiskStore1");
    final DiskStoreDetails diskStoreDetails4 =
        createDiskStoreDetails("memberTwo", "cacheServer2DiskStore2");
    final List<DiskStoreDetails> expectedDiskStores =
        Arrays.asList(diskStoreDetails1, diskStoreDetails2, diskStoreDetails3, diskStoreDetails4);
    final List<Set<DiskStoreDetails>> results = new ArrayList<>();
    results.add(CollectionUtils.asSet(diskStoreDetails1, diskStoreDetails2));
    results.add(CollectionUtils.asSet(diskStoreDetails4, diskStoreDetails3));
    when(mockFunctionExecutor.execute(any(ListDiskStoresFunction.class)))
        .thenReturn(mockResultCollector);
    when(mockResultCollector.getResult()).thenReturn(results);

    final List<DiskStoreDetails> actualDiskStores =
        listDiskStoresCommand.getDiskStoreListing(Collections.singleton(mockDistributedMember));
    assertThat(actualDiskStores).isNotNull();
    assertThat(actualDiskStores).isEqualTo(expectedDiskStores);
    verify(mockFunctionExecutor, times(1)).setIgnoreDepartedMembers(true);
  }

  @Test
  public void testGetDiskStoreListThrowsRuntimeException() {
    when(mockFunctionExecutor.execute(any(ListDiskStoresFunction.class)))
        .thenThrow(new RuntimeException("Expected Exception"));

    assertThatThrownBy(() -> listDiskStoresCommand
        .getDiskStoreListing(Collections.singleton(mockDistributedMember)))
            .isInstanceOf(RuntimeException.class).hasMessage("Expected Exception");
  }

  @Test
  public void testGetDiskStoreListReturnsFunctionInvocationTargetExceptionInResults() {
    final DiskStoreDetails diskStoreDetails =
        createDiskStoreDetails("memberOne", "cacheServerDiskStore");
    final List<DiskStoreDetails> expectedDiskStores = Collections.singletonList(diskStoreDetails);
    final List<Object> results = new ArrayList<>();
    results.add(CollectionUtils.asSet(diskStoreDetails));
    results.add(new FunctionInvocationTargetException("expected"));
    when(mockFunctionExecutor.execute(any(ListDiskStoresFunction.class)))
        .thenReturn(mockResultCollector);
    when(mockResultCollector.getResult()).thenReturn(results);

    final List<DiskStoreDetails> actualDiskStores =
        listDiskStoresCommand.getDiskStoreListing(Collections.singleton(mockDistributedMember));
    assertThat(actualDiskStores).isNotNull();
    assertThat(actualDiskStores).isEqualTo(expectedDiskStores);
    verify(mockFunctionExecutor, times(1)).setIgnoreDepartedMembers(true);
  }
}
