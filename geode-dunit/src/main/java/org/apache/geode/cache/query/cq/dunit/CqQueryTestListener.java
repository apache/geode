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

  public static final long MAX_TIME = Integer.getInteger(WAIT_PROPERTY, WAIT_DEFAULT).intValue();

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
    totalEventCount++;

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
      eventUpdateCount++;
      updates.add(key);
    } else if (baseOperation.isCreate()) {
      eventCreateCount++;
      creates.add(key);
    } else if (baseOperation.isDestroy()) {
      eventDeleteCount++;
      destroys.add(key);
    } else if (baseOperation.isInvalidate()) {
      eventDeleteCount++;
      invalidates.add(key);
    }

    if (queryOperation.isUpdate()) {
      eventQueryUpdateCount++;
    } else if (queryOperation.isCreate()) {
      eventQueryInsertCount++;
    } else if (queryOperation.isDestroy()) {
      eventQueryDeleteCount++;
    } else if (queryOperation.isInvalidate()) {
      eventQueryInvalidateCount++;
    } else if (queryOperation.isClear()) {
      eventRegionClear = true;
    } else if (queryOperation.isRegionInvalidate()) {
      eventRegionInvalidate = true;
    }
  }

  @Override
  public void onError(CqEvent cqEvent) {
    eventErrorCount++;
    errors.add(cqEvent.getThrowable().getMessage());
  }

  @Override
  public void onCqDisconnected() {
    cqsDisconnectedCount++;
  }

  @Override
  public void onCqConnected() {
    cqsConnectedCount++;
  }

  public int getErrorEventCount() {
    return eventErrorCount;
  }

  public int getTotalEventCount() {
    return totalEventCount;
  }

  public int getCreateEventCount() {
    return eventCreateCount;
  }

  public int getUpdateEventCount() {
    return eventUpdateCount;
  }

  public int getDeleteEventCount() {
    return eventDeleteCount;
  }

  public int getInvalidateEventCount() {
    return eventInvalidateCount;
  }

  public int getQueryInsertEventCount() {
    return eventQueryInsertCount;
  }

  public int getQueryUpdateEventCount() {
    return eventQueryUpdateCount;
  }

  public int getQueryDeleteEventCount() {
    return eventQueryDeleteCount;
  }

  public int getQueryInvalidateEventCount() {
    return eventQueryInvalidateCount;
  }

  public Object[] getEvents() {
    return cqEvents.toArray();
  }

  @Override
  public void close() {
    eventClose = true;
  }

  public void printInfo(final boolean printKeys) {
    logger.info("####" + cqName + ": " + " Events Total :" + getTotalEventCount()
        + " Events Created :" + eventCreateCount + " Events Updated :" + eventUpdateCount
        + " Events Deleted :" + eventDeleteCount + " Events Invalidated :"
        + eventInvalidateCount + " Query Inserts :" + eventQueryInsertCount
        + " Query Updates :" + eventQueryUpdateCount + " Query Deletes :"
        + eventQueryDeleteCount + " Query Invalidates :" + eventQueryInvalidateCount
        + " Total Events :" + totalEventCount);
    if (printKeys) {
      // for debugging on failuers ...
      logger.info("Number of Insert for key : " + creates.size() + " and updates : "
          + updates.size() + " and number of destroys : " + destroys.size()
          + " and number of invalidates : " + invalidates.size());

      logger.info("Keys in created sets : " + creates);
      logger.info("Key in updates sets : " + updates);
      logger.info("Key in destorys sets : " + destroys);
      logger.info("Key in invalidates sets : " + invalidates);
    }

  }

  public void waitForCreated(final Object key) {
    await().untilAsserted(() -> assertThat(creates).contains(key));
  }


  public void waitForTotalEvents(final int total) {
    await()
        .untilAsserted(() -> assertThat(totalEventCount).isEqualTo(total));
  }

  public void waitForDestroyed(final Object key) {
    await().untilAsserted(() -> assertThat(destroys).contains(key));
  }

  public void waitForInvalidated(final Object key) {
    await().untilAsserted(() -> assertThat(invalidates).contains(key));
  }

  public void waitForUpdated(final Object key) {
    await().untilAsserted(() -> assertThat(updates).contains(key));
  }

  public void waitForClose() {
    await().untilAsserted(() -> assertThat(eventClose).isTrue());
  }

  public void waitForRegionClear() {
    await().untilAsserted(() -> assertThat(eventRegionClear).isTrue());
  }

  public void waitForRegionInvalidate() {
    await()
        .untilAsserted(() -> assertThat(eventRegionInvalidate).isTrue());
  }

  public void waitForError(final String expectedMessage) {
    await()
        .untilAsserted(() -> assertThat(errors).contains(expectedMessage));
  }

  public void waitForCqsDisconnectedEvents(final int total) {
    await().untilAsserted(
        () -> assertThat(cqsDisconnectedCount).isEqualTo(total));
  }

  public void waitForCqsConnectedEvents(final int total) {
    await().untilAsserted(
        () -> assertThat(cqsConnectedCount).isEqualTo(total));
  }

  public void waitForEvents(final int creates, final int updates, final int deletes,
      final int queryInserts, final int queryUpdates, final int queryDeletes,
      final int totalEvents) {
    // Wait for expected events to arrive
    try {
      await().until(() -> {
        return (creates <= 0 || creates == getCreateEventCount())
            && (updates <= 0 || updates == getUpdateEventCount())
            && (deletes <= 0 || deletes == getDeleteEventCount())
            && (queryInserts <= 0 || queryInserts == getQueryInsertEventCount())
            && (queryUpdates <= 0 || queryUpdates == getQueryUpdateEventCount())
            && (queryDeletes <= 0 || queryDeletes == getQueryDeleteEventCount())
            && (totalEvents <= 0 || totalEvents == getTotalEventCount());
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
    eventClose = false;
  }

}
