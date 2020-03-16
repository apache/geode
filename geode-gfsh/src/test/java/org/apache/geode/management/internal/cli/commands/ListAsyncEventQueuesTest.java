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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.internal.cli.domain.AsyncEventQueueDetails;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.junit.categories.AEQTest;
import org.apache.geode.test.junit.rules.GfshParserRule;

@Category({AEQTest.class})
public class ListAsyncEventQueuesTest {
  private static final String COMMAND = "list async-event-queues ";

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();
  private static final String FUNCTION_EXCEPTION_MESSAGE =
      "A mysterious test exception occurred during function execution.";

  private ListAsyncEventQueuesCommand command;
  private List<CliFunctionResult> memberCliResults;
  private DistributedMember mockedMember;
  private DistributedMember anotherMockedMember;


  @Before
  public void before() {
    mockedMember = mock(DistributedMember.class);
    anotherMockedMember = mock(DistributedMember.class);
    command = spy(ListAsyncEventQueuesCommand.class);
  }

  @Test
  public void noMembersFound() {
    // Mock zero members
    doReturn(Collections.emptySet()).when(command).getAllNormalMembers();

    // Command should succeed with one row of data
    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess()
        .containsOutput(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void oneServerWithOneQueue() {
    // Mock one member
    doReturn(new HashSet<>(Collections.singletonList(mockedMember))).when(command)
        .getAllNormalMembers();

    // Mock member's queue details
    FakeDetails details = new FakeDetails("server1", "s1-queue-id", 5, true, "diskStoreName", 10,
        "my.listener.class", new Properties(), false, false);
    CliFunctionResult memberResult = new CliFunctionResult(details.getMemberName(),
        Collections.singletonList(details.asAsyncEventQueueDetails()));
    memberCliResults = Collections.singletonList(memberResult);
    doReturn(memberCliResults).when(command).executeAndGetFunctionResult(any(), any(), any());

    // Command should succeed with one row of data
    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess()
        .tableHasRowWithValues(details.expectedRowHeaderAndValue());
  }

  @Test
  @SuppressWarnings("deprecation")
  public void oneServerWithOneParameterizedQueue() {
    // Mock one member
    doReturn(new HashSet<>(Collections.singletonList(mockedMember))).when(command)
        .getAllNormalMembers();

    // Mock member's queue details
    Properties listenerProperties = new Properties();
    listenerProperties.setProperty("special-property", "special-value");
    listenerProperties.setProperty("another-property", "mundane-value");
    FakeDetails details = new FakeDetails("server1", "s1-queue-id", 5, true, "diskStoreName", 10,
        "my.listener.class", listenerProperties, false, false);
    CliFunctionResult memberResult = new CliFunctionResult(details.getMemberName(),
        Collections.singletonList(details.asAsyncEventQueueDetails()));
    memberCliResults = Collections.singletonList(memberResult);
    doReturn(memberCliResults).when(command).executeAndGetFunctionResult(any(), any(), any());

    // Command should succeed with one row of data
    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess()
        .tableHasRowWithValues(details.expectedRowHeaderAndValue());
  }

  @Test
  public void oneMemberButNoQueues() {
    // Mock one member
    doReturn(new HashSet<>(Collections.singletonList(mockedMember))).when(command)
        .getAllNormalMembers();

    // Mock member's lack of queue details
    CliFunctionResult memberResult = new CliFunctionResult("server1", Collections.emptyList());
    memberCliResults = Collections.singletonList(memberResult);
    doReturn(memberCliResults).when(command).executeAndGetFunctionResult(any(), any(), any());

    // Command should succeed, but indicate there were no queues
    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess()
        .containsOutput(CliStrings.LIST_ASYNC_EVENT_QUEUES__NO_QUEUES_FOUND_MESSAGE);
  }

  @Test
  public void oneMemberWhoErrorsOut() {
    // Mock one member
    doReturn(new HashSet<>(Collections.singletonList(mockedMember))).when(command)
        .getAllNormalMembers();

    // Mock member's error result
    CliFunctionResult memberResult =
        new CliFunctionResult("server1", new Exception(FUNCTION_EXCEPTION_MESSAGE));
    memberCliResults = Collections.singletonList(memberResult);
    doReturn(memberCliResults).when(command).executeAndGetFunctionResult(any(), any(), any());

    // Command should succeed, but indicate there were no queues
    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess()
        .containsOutput(CliStrings.LIST_ASYNC_EVENT_QUEUES__NO_QUEUES_FOUND_MESSAGE);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void twoServersWithManyQueues() {
    // Mock two members, even though they're not directly consumed
    doReturn(new HashSet<>(Arrays.asList(mockedMember, anotherMockedMember))).when(command)
        .getAllNormalMembers();

    // Mock member's queue details
    FakeDetails details1 = new FakeDetails("server1", "s1-queue-id1", 5, false, "diskStoreName", 1,
        "my.listener.class", new Properties(), false, false);
    FakeDetails details2 = new FakeDetails("server1", "s1-queue-id2", 15, true,
        "otherDiskStoreName", 10, "my.listener.class", new Properties(), false, false);
    FakeDetails details3 = new FakeDetails("server1", "s1-queue-id3", 25, true, "diskStoreName",
        100, "my.listener.class", new Properties(), true, true);
    CliFunctionResult member1Result =
        new CliFunctionResult("server1", Arrays.asList(details1.asAsyncEventQueueDetails(),
            details2.asAsyncEventQueueDetails(), details3.asAsyncEventQueueDetails()));

    FakeDetails details4 = new FakeDetails("server2", "s2-queue-id1", 5, false, "diskStoreName", 1,
        "my.listener.class", new Properties(), false, false);
    FakeDetails details5 = new FakeDetails("server2", "s2-queue-id2", 15, true,
        "otherDiskStoreName", 10, "my.listener.class", new Properties(), false, false);
    FakeDetails details6 = new FakeDetails("server2", "s2-queue-id3", 25, true, "diskStoreName",
        100, "my.listener.class", new Properties(), false, false);
    CliFunctionResult member2Result =
        new CliFunctionResult("server2", Arrays.asList(details4.asAsyncEventQueueDetails(),
            details5.asAsyncEventQueueDetails(), details6.asAsyncEventQueueDetails()));

    memberCliResults = Arrays.asList(member1Result, member2Result);
    doReturn(memberCliResults).when(command).executeAndGetFunctionResult(any(), any(), any());

    // Command should succeed with one row of data
    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess()
        .tableHasRowWithValues(details1.expectedRowHeaderAndValue())
        .tableHasRowWithValues(details2.expectedRowHeaderAndValue())
        .tableHasRowWithValues(details3.expectedRowHeaderAndValue())
        .tableHasRowWithValues(details4.expectedRowHeaderAndValue())
        .tableHasRowWithValues(details5.expectedRowHeaderAndValue())
        .tableHasRowWithValues(details6.expectedRowHeaderAndValue());
  }

  @Test
  @SuppressWarnings("deprecation")
  public void oneMemberSucceedsAndOneFails() {
    // Mock two members, even though they're not directly consumed
    doReturn(new HashSet<>(Arrays.asList(mockedMember, anotherMockedMember))).when(command)
        .getAllNormalMembers();

    // Mock member's queue details
    FakeDetails details1 = new FakeDetails("server1", "s1-queue-id1", 5, false, "diskStoreName", 1,
        "my.listener.class", new Properties(), false, false);
    FakeDetails details2 = new FakeDetails("server1", "s1-queue-id2", 15, true,
        "otherDiskStoreName", 10, "my.listener.class", new Properties(), false, false);
    FakeDetails details3 = new FakeDetails("server1", "s1-queue-id3", 25, true, "diskStoreName",
        100, "my.listener.class", new Properties(), false, false);
    CliFunctionResult member1Result =
        new CliFunctionResult("server1", Arrays.asList(details1.asAsyncEventQueueDetails(),
            details2.asAsyncEventQueueDetails(), details3.asAsyncEventQueueDetails()));

    // Mock the other's failure
    CliFunctionResult member2Result =
        new CliFunctionResult("server2", new Exception(FUNCTION_EXCEPTION_MESSAGE));

    memberCliResults = Arrays.asList(member1Result, member2Result);
    doReturn(memberCliResults).when(command).executeAndGetFunctionResult(any(), any(), any());

    // Command should succeed with one row of data
    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess()
        .tableHasRowWithValues(details1.expectedRowHeaderAndValue())
        .tableHasRowWithValues(details2.expectedRowHeaderAndValue())
        .tableHasRowWithValues(details3.expectedRowHeaderAndValue())
        .containsOutput(FUNCTION_EXCEPTION_MESSAGE);
  }

  /**
   * Wrapper for mocked AsyncEventQueueData, with convenience method for expected table output.
   */
  static class FakeDetails {
    private String memberName;
    private String queueId;
    private int batchSize;
    private boolean persistent;
    private String diskStoreName;
    private int maxQueueMemory;
    private String listener;
    private Properties listenerProperties;
    private boolean createWithPausedEventProcessing;
    private boolean pausedEventProcessing;

    private FakeDetails(String memberName, String queueId, int batchSize, boolean persistent,
        String diskStoreName, int maxQueueMemory, String listener, Properties listenerProperties,
        boolean createWithPausedEventProcessing, boolean pausedEventProcessing) {
      this.memberName = memberName;
      this.queueId = queueId;
      this.batchSize = batchSize;
      this.persistent = persistent;
      this.diskStoreName = diskStoreName;
      this.maxQueueMemory = maxQueueMemory;
      this.listener = listener;
      this.listenerProperties = listenerProperties;
      this.createWithPausedEventProcessing = createWithPausedEventProcessing;
      this.pausedEventProcessing = pausedEventProcessing;
    }

    public String getMemberName() {
      return memberName;
    }

    private AsyncEventQueueDetails asAsyncEventQueueDetails() {
      return new AsyncEventQueueDetails(queueId, batchSize, persistent, diskStoreName,
          maxQueueMemory, listener, listenerProperties, createWithPausedEventProcessing,
          pausedEventProcessing);
    }

    private String[] expectedRowHeaderAndValue() {
      return new String[] {"Member", "ID", "Batch Size", "Persistent", "Disk Store", "Max Memory",
          "Listener", "Created with paused event processing", "Currently Paused", memberName,
          queueId, String.valueOf(batchSize),
          String.valueOf(persistent),
          diskStoreName, String.valueOf(maxQueueMemory), expectedListenerOutput(),
          String.valueOf(createWithPausedEventProcessing), String.valueOf(pausedEventProcessing)};
    }

    private String expectedListenerOutput() {
      return listener + ListAsyncEventQueuesCommand.propertiesToString(listenerProperties);
    }
  }
}
