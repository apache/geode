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
package org.apache.geode.cache.query.cq.dunit;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqStatusListener;

public class CqQueryTestListener implements CqStatusListener {
  protected final LogWriter logger;
  protected volatile int eventCreateCount = 0;
  protected volatile int eventUpdateCount = 0;
  protected volatile int eventDeleteCount = 0;
  protected volatile int eventInvalidateCount = 0;
  protected volatile int eventErrorCount = 0;

  protected volatile int totalEventCount = 0;
  protected volatile int eventQueryInsertCount = 0;
  protected volatile int eventQueryUpdateCount = 0;
  protected volatile int eventQueryDeleteCount = 0;
  protected volatile int eventQueryInvalidateCount = 0;

  protected volatile int cqsConnectedCount = 0;
  protected volatile int cqsDisconnectedCount = 0;

  protected volatile boolean eventClose = false;
  protected volatile boolean eventRegionClear = false;
  protected volatile boolean eventRegionInvalidate = false;

  public final Set destroys = ConcurrentHashMap.newKeySet();
  public final Set creates = ConcurrentHashMap.newKeySet();
  public final Set invalidates = ConcurrentHashMap.newKeySet();
  public final Set updates = ConcurrentHashMap.newKeySet();
  public final Set errors = ConcurrentHashMap.newKeySet();

  private static final String WAIT_PROPERTY = "CqQueryTestListener.maxWaitTime";

  private static final int WAIT_DEFAULT = (20 * 1000);

  public static final long MAX_TIME = Integer.getInteger(WAIT_PROPERTY, WAIT_DEFAULT).intValue();;

  public String cqName;
  public String userName;

  // This is to avoid reference to PerUserRequestSecurityTest which will fail to
  // initialize in a non Hydra environment.
  public static boolean usedForUnitTests = true;

  public ConcurrentLinkedQueue events = new ConcurrentLinkedQueue();

  public ConcurrentLinkedQueue cqEvents = new ConcurrentLinkedQueue();

  public CqQueryTestListener(LogWriter logger) {
    this.logger = logger;
  }

  @Override
  public void onEvent(CqEvent cqEvent) {
    this.totalEventCount++;

    Operation baseOperation = cqEvent.getBaseOperation();
    Operation queryOperation = cqEvent.getQueryOperation();
    Object key = cqEvent.getKey();

    // logger.info("CqEvent for the CQ: " + this.cqName +
    // "; Key=" + key +
    // "; baseOp=" + baseOperation +
    // "; queryOp=" + queryOperation +
    // "; totalEventCount=" + this.totalEventCount
    // );

    if (key != null) {
      events.add(key);
      cqEvents.add(cqEvent);
    }

    if (baseOperation.isUpdate()) {
      this.eventUpdateCount++;
      this.updates.add(key);
    } else if (baseOperation.isCreate()) {
      this.eventCreateCount++;
      this.creates.add(key);
    } else if (baseOperation.isDestroy()) {
      this.eventDeleteCount++;
      this.destroys.add(key);
    } else if (baseOperation.isInvalidate()) {
      this.eventDeleteCount++;
      this.invalidates.add(key);
    }

    if (queryOperation.isUpdate()) {
      this.eventQueryUpdateCount++;
    } else if (queryOperation.isCreate()) {
      this.eventQueryInsertCount++;
    } else if (queryOperation.isDestroy()) {
      this.eventQueryDeleteCount++;
    } else if (queryOperation.isInvalidate()) {
      this.eventQueryInvalidateCount++;
    } else if (queryOperation.isClear()) {
      this.eventRegionClear = true;
    } else if (queryOperation.isRegionInvalidate()) {
      this.eventRegionInvalidate = true;
    }
  }

  @Override
  public void onError(CqEvent cqEvent) {
    this.eventErrorCount++;
    this.errors.add(cqEvent.getThrowable().getMessage());
  }

  @Override
  public void onCqDisconnected() {
    this.cqsDisconnectedCount++;
  }

  @Override
  public void onCqConnected() {
    this.cqsConnectedCount++;
  }

  public int getErrorEventCount() {
    return this.eventErrorCount;
  }

  public int getTotalEventCount() {
    return this.totalEventCount;
  }

  public int getCreateEventCount() {
    return this.eventCreateCount;
  }

  public int getUpdateEventCount() {
    return this.eventUpdateCount;
  }

  public int getDeleteEventCount() {
    return this.eventDeleteCount;
  }

  public int getInvalidateEventCount() {
    return this.eventInvalidateCount;
  }

  public int getQueryInsertEventCount() {
    return this.eventQueryInsertCount;
  }

  public int getQueryUpdateEventCount() {
    return this.eventQueryUpdateCount;
  }

  public int getQueryDeleteEventCount() {
    return this.eventQueryDeleteCount;
  }

  public int getQueryInvalidateEventCount() {
    return this.eventQueryInvalidateCount;
  }

  public Object[] getEvents() {
    return this.cqEvents.toArray();
  }

  @Override
  public void close() {
    this.eventClose = true;
  }

  public void printInfo(final boolean printKeys) {
    logger.info("####" + this.cqName + ": " + " Events Total :" + this.getTotalEventCount()
        + " Events Created :" + this.eventCreateCount + " Events Updated :" + this.eventUpdateCount
        + " Events Deleted :" + this.eventDeleteCount + " Events Invalidated :"
        + this.eventInvalidateCount + " Query Inserts :" + this.eventQueryInsertCount
        + " Query Updates :" + this.eventQueryUpdateCount + " Query Deletes :"
        + this.eventQueryDeleteCount + " Query Invalidates :" + this.eventQueryInvalidateCount
        + " Total Events :" + this.totalEventCount);
    if (printKeys) {
      // for debugging on failuers ...
      logger.info("Number of Insert for key : " + this.creates.size() + " and updates : "
          + this.updates.size() + " and number of destroys : " + this.destroys.size()
          + " and number of invalidates : " + this.invalidates.size());

      logger.info("Keys in created sets : " + this.creates.toString());
      logger.info("Key in updates sets : " + this.updates.toString());
      logger.info("Key in destorys sets : " + this.destroys.toString());
      logger.info("Key in invalidates sets : " + this.invalidates.toString());
    }

  }

  public void waitForCreated(final Object key) {
    await().untilAsserted(() -> assertThat(CqQueryTestListener.this.creates).contains(key));
  }


  public void waitForTotalEvents(final int total) {
    await()
        .untilAsserted(() -> assertThat(CqQueryTestListener.this.totalEventCount).isEqualTo(total));
  }

  public void waitForDestroyed(final Object key) {
    await().untilAsserted(() -> assertThat(CqQueryTestListener.this.destroys).contains(key));
  }

  public void waitForInvalidated(final Object key) {
    await().untilAsserted(() -> assertThat(CqQueryTestListener.this.invalidates).contains(key));
  }

  public void waitForUpdated(final Object key) {
    await().untilAsserted(() -> assertThat(CqQueryTestListener.this.updates).contains(key));
  }

  public void waitForClose() {
    await().untilAsserted(() -> assertThat(CqQueryTestListener.this.eventClose).isTrue());
  }

  public void waitForRegionClear() {
    await().untilAsserted(() -> assertThat(CqQueryTestListener.this.eventRegionClear).isTrue());
  }

  public void waitForRegionInvalidate() {
    await()
        .untilAsserted(() -> assertThat(CqQueryTestListener.this.eventRegionInvalidate).isTrue());
  }

  public void waitForError(final String expectedMessage) {
    await()
        .untilAsserted(() -> assertThat(CqQueryTestListener.this.errors).contains(expectedMessage));
  }

  public void waitForCqsDisconnectedEvents(final int total) {
    await().untilAsserted(
        () -> assertThat(CqQueryTestListener.this.cqsDisconnectedCount).isEqualTo(total));
  }

  public void waitForCqsConnectedEvents(final int total) {
    await().untilAsserted(
        () -> assertThat(CqQueryTestListener.this.cqsConnectedCount).isEqualTo(total));
  }

  public void waitForEvents(final int creates, final int updates, final int deletes,
      final int queryInserts, final int queryUpdates, final int queryDeletes,
      final int totalEvents) {
    // Wait for expected events to arrive
    try {
      await().until(() -> {
        if ((creates > 0 && creates != this.getCreateEventCount())
            || (updates > 0 && updates != this.getUpdateEventCount())
            || (deletes > 0 && deletes != this.getDeleteEventCount())
            || (queryInserts > 0 && queryInserts != this.getQueryInsertEventCount())
            || (queryUpdates > 0 && queryUpdates != this.getQueryUpdateEventCount())
            || (queryDeletes > 0 && queryDeletes != this.getQueryDeleteEventCount())
            || (totalEvents > 0 && totalEvents != this.getTotalEventCount())) {
          return false;
        }
        return true;
      });
    } catch (Exception ex) {
      // We just wait for expected events to arrive.
      // Caller will do validation and throw exception.
    }
  }


  public void getEventHistory() {
    destroys.clear();
    creates.clear();
    invalidates.clear();
    updates.clear();
    this.eventClose = false;
  }

}
