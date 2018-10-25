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
package org.apache.geode.cache.lucene.internal.cli;

import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.INDEX_NAME;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.REGION_NAME;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.stubbing.Answer;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.categories.LuceneTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category(LuceneTest.class)
@RunWith(JUnitParamsRunner.class)
public class DestroyLuceneIndexCommandsDUnitTest implements Serializable {

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  private MemberVM locator;

  private MemberVM server1;

  private MemberVM server2;

  private static CountDownLatch indexCreationInProgress;

  private static CountDownLatch indexDestroyComplete;

  @Before
  public void before() throws Exception {
    Properties props = new Properties();
    props.setProperty(SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.cache.lucene.internal.cli.DestroyLuceneIndexCommandsDUnitTest");
    locator = cluster.startLocatorVM(0, props);
    server1 = cluster.startServerVM(1, props, locator.getPort());
    server2 = cluster.startServerVM(2, props, locator.getPort());
    gfsh.connectAndVerify(locator);
  }

  @Test
  @Parameters({"true", "false"})
  public void testDestroyIndex(boolean createRegion) throws Exception {
    // Create index and region if necessary
    server1.invoke(() -> createIndex(1));
    server2.invoke(() -> createIndex(1));
    if (createRegion) {
      server1.invoke(() -> createRegion());
      server2.invoke(() -> createRegion());
    }

    // Execute command to destroy index
    CommandResultAssert commandResultAssert =
        gfsh.executeAndAssertThat("destroy lucene index --name=index0 --region=region");

    // Assert command was successful and contains the correct rows and output
    String expectedStatus = CliStrings.format(
        LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__SUCCESSFULLY_DESTROYED_INDEX_0_FROM_REGION_1,
        new Object[] {"index0", "/region"});
    validateCommandResult(commandResultAssert, expectedStatus);

    // Verify defined and created indexes are empty in both members
    server1.invoke(() -> verifyDefinedAndCreatedIndexSizes(0, 0));
    server2.invoke(() -> verifyDefinedAndCreatedIndexSizes(0, 0));
  }

  @Test
  public void testDestroyIndexWithRegionInOneMember() throws Exception {
    // Create index in both members
    server1.invoke(() -> createIndex(1));
    server2.invoke(() -> createIndex(1));

    // Create region in one member
    server1.invoke(() -> createRegion());

    // Execute command to destroy index
    CommandResultAssert commandResultAssert =
        gfsh.executeAndAssertThat("destroy lucene index --name=index0 --region=region");

    // Assert command was successful and contains the correct rows and output
    String expectedStatus = CliStrings.format(
        LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__SUCCESSFULLY_DESTROYED_INDEX_0_FROM_REGION_1,
        new Object[] {"index0", "/region"});
    validateCommandResult(commandResultAssert, expectedStatus);

    // Verify defined and created indexes are empty in both members
    server1.invoke(() -> verifyDefinedAndCreatedIndexSizes(0, 0));
    server2.invoke(() -> verifyDefinedAndCreatedIndexSizes(0, 0));
  }

  @Test
  public void testDestroyNonExistentIndex() throws Exception {
    // Execute command to destroy index
    CommandResultAssert commandResultAssert =
        gfsh.executeAndAssertThat("destroy lucene index --name=index0 --region=region");

    // Assert command was successful and contains the correct rows and output
    String expectedStatus = String.format("Lucene index %s was not found in region %s",
        "index0", "/region");
    validateCommandResult(commandResultAssert, expectedStatus);

    // Verify defined and created indexes are empty in both members
    server1.invoke(() -> verifyDefinedAndCreatedIndexSizes(0, 0));
    server2.invoke(() -> verifyDefinedAndCreatedIndexSizes(0, 0));
  }

  @Test
  public void testDestroyIndexWithRegionCreationInProgress() throws Exception {
    server1.invoke(() -> initializeCountDownLatches());
    server2.invoke(() -> initializeCountDownLatches());

    server1.invoke(() -> createIndexesOnSpy(1));
    server2.invoke(() -> createIndexesOnSpy(1));

    // Asynchronously create region. This will cause the invokeBeforeAfterDataRegionCreated Answer
    // to be invoked which will wait for the index to be destroyed before invoking the real
    // afterDataRegionCreated method and completing region creation. The registerIndex method will
    // realize the defined index has been destroyed and destroy the real one.
    AsyncInvocation server1RegionCreationInvocation = server1.invokeAsync(() -> createRegion());
    AsyncInvocation server2RegionCreationInvocation = server2.invokeAsync(() -> createRegion());

    // Wait for index creation to be in progress
    server1.invoke(() -> waitForIndexCreationInProgress());
    server2.invoke(() -> waitForIndexCreationInProgress());

    // Execute command to destroy index
    CommandResultAssert commandResultAssert =
        gfsh.executeAndAssertThat("destroy lucene index --name=index0 --region=region");

    // Assert command was successful and contains the correct rows and output
    String expectedStatus = CliStrings.format(
        LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__SUCCESSFULLY_DESTROYED_INDEX_0_FROM_REGION_1,
        new Object[] {"index0", "/region"});
    validateCommandResult(commandResultAssert, expectedStatus);

    // Notify region creation to continue creating the region
    server1.invoke(() -> notifyIndexDestroyComplete());
    server2.invoke(() -> notifyIndexDestroyComplete());

    server1RegionCreationInvocation.await(30, TimeUnit.SECONDS);
    server2RegionCreationInvocation.await(30, TimeUnit.SECONDS);

    // Verify defined and created indexes are empty in both members
    server1.invoke(() -> verifyDefinedAndCreatedIndexSizes(0, 0));
    server2.invoke(() -> verifyDefinedAndCreatedIndexSizes(0, 0));
  }

  @Test
  @Parameters({"true", "false"})
  public void testDestroyIndexesWithOneIndex(boolean createRegion) throws Exception {
    // Create index and region if necessary
    server1.invoke(() -> createIndex(1));
    server2.invoke(() -> createIndex(1));
    if (createRegion) {
      server1.invoke(() -> createRegion());
      server2.invoke(() -> createRegion());
    }

    // Execute command to destroy indexes
    CommandResultAssert commandResultAssert =
        gfsh.executeAndAssertThat("destroy lucene index --region=region");

    // Assert command was successful and contains the correct rows and output
    String expectedStatus = CliStrings.format(
        LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__SUCCESSFULLY_DESTROYED_INDEXES_FROM_REGION_0,
        new Object[] {"/region"});
    validateCommandResult(commandResultAssert, expectedStatus);

    // Verify defined and created indexes are empty in both members
    server1.invoke(() -> verifyDefinedAndCreatedIndexSizes(0, 0));
    server2.invoke(() -> verifyDefinedAndCreatedIndexSizes(0, 0));
  }

  @Test
  public void testDestroyIndexesWithOneIndexAndRegionInOneMember() throws Exception {
    // Create index in both members
    server1.invoke(() -> createIndex(1));
    server2.invoke(() -> createIndex(1));

    // Create region in one member
    server1.invoke(() -> createRegion());

    // Execute command to destroy indexes
    CommandResultAssert commandResultAssert =
        gfsh.executeAndAssertThat("destroy lucene index --region=region");

    // Assert command was successful and contains the correct rows and output
    String expectedStatus = CliStrings.format(
        LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__SUCCESSFULLY_DESTROYED_INDEXES_FROM_REGION_0,
        new Object[] {"/region"});
    validateCommandResult(commandResultAssert, expectedStatus);

    // Verify defined and created indexes are empty in both members
    server1.invoke(() -> verifyDefinedAndCreatedIndexSizes(0, 0));
    server2.invoke(() -> verifyDefinedAndCreatedIndexSizes(0, 0));
  }

  @Test
  @Parameters({"true", "false"})
  public void testDestroyIndexesWithTwoIndexes(boolean createRegion) throws Exception {
    // Create index and region if necessary
    server1.invoke(() -> createIndex(2));
    server2.invoke(() -> createIndex(2));
    if (createRegion) {
      server1.invoke(() -> createRegion());
      server2.invoke(() -> createRegion());
    }

    // Execute command to destroy indexes
    CommandResultAssert commandResultAssert =
        gfsh.executeAndAssertThat("destroy lucene index --region=region");

    // Assert command was successful and contains the correct rows and output
    String expectedStatus = CliStrings.format(
        LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__SUCCESSFULLY_DESTROYED_INDEXES_FROM_REGION_0,
        new Object[] {"/region"});
    validateCommandResult(commandResultAssert, expectedStatus);

    // Verify defined and created indexes are empty in both members
    server1.invoke(() -> verifyDefinedAndCreatedIndexSizes(0, 0));
    server2.invoke(() -> verifyDefinedAndCreatedIndexSizes(0, 0));
  }

  @Test
  public void testDestroyIndexesWithTwoIndexesAndRegionInOneMember() throws Exception {
    // Create index in both members
    server1.invoke(() -> createIndex(2));
    server2.invoke(() -> createIndex(2));

    // Create region in one member
    server1.invoke(() -> createRegion());

    // Execute command to destroy indexes
    CommandResultAssert commandResultAssert =
        gfsh.executeAndAssertThat("destroy lucene index --region=region");

    // Assert command was successful and contains the correct rows and output
    String expectedStatus = CliStrings.format(
        LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__SUCCESSFULLY_DESTROYED_INDEXES_FROM_REGION_0,
        new Object[] {"/region"});
    validateCommandResult(commandResultAssert, expectedStatus);

    // Verify defined and created indexes are empty in both members
    server1.invoke(() -> verifyDefinedAndCreatedIndexSizes(0, 0));
    server2.invoke(() -> verifyDefinedAndCreatedIndexSizes(0, 0));
  }

  @Test
  public void testDestroyNonExistentIndexes() throws Exception {
    // Execute command to destroy indexes
    CommandResultAssert commandResultAssert =
        gfsh.executeAndAssertThat("destroy lucene index --region=region");

    // Assert command was successful and contains the correct rows and output
    String expectedStatus = String.format("No Lucene indexes were found in region %s",
        "/region");
    validateCommandResult(commandResultAssert, expectedStatus);

    // Verify defined and created indexes are empty in both members
    server1.invoke(() -> verifyDefinedAndCreatedIndexSizes(0, 0));
    server2.invoke(() -> verifyDefinedAndCreatedIndexSizes(0, 0));
  }

  @Test
  public void testDestroyIndexesWithRegionCreationInProgress() throws Exception {
    server1.invoke(() -> initializeCountDownLatches());
    server2.invoke(() -> initializeCountDownLatches());

    server1.invoke(() -> createIndexesOnSpy(2));
    server2.invoke(() -> createIndexesOnSpy(2));

    // Asynchronously create region. This will cause the invokeBeforeAfterDataRegionCreated Answer
    // to be invoked which will wait for the indexes to be destroyed before invoking the real
    // afterDataRegionCreated method and completing region creation. The registerIndex method will
    // realize the defined index has been destroyed and destroy the real one.
    AsyncInvocation server1RegionCreationInvocation = server1.invokeAsync(() -> createRegion());
    AsyncInvocation server2RegionCreationInvocation = server2.invokeAsync(() -> createRegion());

    // Wait for index creation to be in progress
    server1.invoke(() -> waitForIndexCreationInProgress());
    server2.invoke(() -> waitForIndexCreationInProgress());

    // Execute command to destroy index
    CommandResultAssert commandResultAssert =
        gfsh.executeAndAssertThat("destroy lucene index --region=region");

    // Assert command was successful and contains the correct rows and output
    String expectedStatus = CliStrings.format(
        LuceneCliStrings.LUCENE_DESTROY_INDEX__MSG__SUCCESSFULLY_DESTROYED_INDEXES_FROM_REGION_0,
        new Object[] {"/region"});
    validateCommandResult(commandResultAssert, expectedStatus);

    // Notify region creation to continue creating the region
    server1.invoke(() -> notifyIndexDestroyComplete());
    server2.invoke(() -> notifyIndexDestroyComplete());

    server1RegionCreationInvocation.await(30, TimeUnit.SECONDS);
    server2RegionCreationInvocation.await(30, TimeUnit.SECONDS);

    // Verify defined and created indexes are empty in both members
    server1.invoke(() -> verifyDefinedAndCreatedIndexSizes(0, 0));
    server2.invoke(() -> verifyDefinedAndCreatedIndexSizes(0, 0));
  }

  private void validateCommandResult(CommandResultAssert commandResultAssert,
      String expectedStatus) {
    commandResultAssert.statusIsSuccess();
    commandResultAssert.tableHasRowCount("Member", 2);
    commandResultAssert.tableHasColumnOnlyWithValues("Status", expectedStatus);
  }

  private void verifyDefinedAndCreatedIndexSizes(int definedIndexesSize, int createdIndexesSize) {
    LuceneServiceImpl luceneService =
        (LuceneServiceImpl) LuceneServiceProvider.get(ClusterStartupRule.getCache());
    assertThat(luceneService.getIndexes(REGION_NAME)).isEmpty();
    assertThat(luceneService.getDefinedIndexes(REGION_NAME)).isEmpty();
  }

  private void createIndex(int numIndexes) {
    LuceneService luceneService = LuceneServiceProvider.get(ClusterStartupRule.getCache());
    for (int i = 0; i < numIndexes; i++) {
      luceneService.createIndexFactory().setFields("text" + i).create(INDEX_NAME + i, REGION_NAME);
    }
  }

  private void createRegion() {
    ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.PARTITION).create(REGION_NAME);
  }

  private void initializeCountDownLatches() {
    indexCreationInProgress = new CountDownLatch(1);
    indexDestroyComplete = new CountDownLatch(1);
  }

  private void createIndexesOnSpy(int numIndexes) {
    LuceneServiceImpl luceneServiceSpy =
        (LuceneServiceImpl) spy(LuceneServiceProvider.get(ClusterStartupRule.getCache()));
    for (int i = 0; i < numIndexes; i++) {
      luceneServiceSpy.createIndexFactory().setFields("text" + i).create(INDEX_NAME + i,
          REGION_NAME);
    }

    Answer invokeBeforeAfterDataRegionCreated = invocation -> {
      // Confirm index creation is in progress
      indexCreationInProgress.countDown();

      // Wait for destroy index invocation to complete
      indexDestroyComplete.await();

      return invocation.callRealMethod();
    };

    doAnswer(invokeBeforeAfterDataRegionCreated).when(luceneServiceSpy)
        .afterDataRegionCreated(any());
  }

  private void waitForIndexCreationInProgress() throws Exception {
    indexCreationInProgress.await();
  }

  private void notifyIndexDestroyComplete() throws Exception {
    indexDestroyComplete.countDown();
  }
}
