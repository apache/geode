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

package org.apache.geode.cache.wan.internal.txgrouping;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.geode.internal.lang.SystemProperty;
import org.apache.geode.util.internal.GeodeGlossary;

public class TxGroupingGatewaySenderProperties implements TxGroupingGatewaySender {

  /**
   * Milliseconds to wait before retrying to get events for a transaction from the
   * gateway sender queue when group-transaction-events is true.
   */
  public static final String GET_TRANSACTION_EVENTS_FROM_QUEUE_WAIT_TIME_MS_PROPERTY =
      "get-transaction-events-from-queue-wait-time-ms";

  /**
   * Number of times to retry to get events for a transaction from the gateway sender queue when
   * group-transaction-events is set to true.
   * When group-transaction-events is set to true and a batch ready to be sent does not contain
   * all the events for all the transactions to which the events belong, the gateway sender will try
   * to get the missing events of the transactions from the queue to add them to the batch
   * before sending it.
   * If the missing events are not in the queue when the gateway sender tries to get them
   * it will retry for a maximum of times equal to the value set in this parameter before
   * delivering the batch without the missing events and logging an error.
   * Setting this parameter to a very low value could cause that under heavy load and
   * group-transaction-events set to true, batches are sent with incomplete transactions. Setting it
   * to a high value could cause that under heavy load and group-transaction-events set to true,
   * batches are held for some time before being sent.
   */
  static final int GET_TRANSACTION_EVENTS_FROM_QUEUE_RETRIES =
      Integer.getInteger(GeodeGlossary.GEMFIRE_PREFIX + "get-transaction-events-from-queue-retries",
          10);
  /**
   * Milliseconds to wait before retrying to get events for a transaction from the
   * gateway sender queue when group-transaction-events is true.
   */
  public static final int GET_TRANSACTION_EVENTS_FROM_QUEUE_WAIT_TIME_MS =
      SystemProperty.getProductIntegerProperty(
          GET_TRANSACTION_EVENTS_FROM_QUEUE_WAIT_TIME_MS_PROPERTY).orElse(1);

  private AtomicInteger retriesToGetTransactionEventsFromQueue =
      new AtomicInteger(GET_TRANSACTION_EVENTS_FROM_QUEUE_RETRIES);

  private AtomicInteger transactionEventsFromQueueWaitMs =
      new AtomicInteger(GET_TRANSACTION_EVENTS_FROM_QUEUE_WAIT_TIME_MS);

  @Override
  public void setRetriesToGetTransactionEventsFromQueue(int retries) {
    retriesToGetTransactionEventsFromQueue.set(retries);
  }

  @Override
  public int getRetriesToGetTransactionEventsFromQueue() {
    return retriesToGetTransactionEventsFromQueue.get();
  }

  @Override
  public void setTransactionEventsFromQueueWaitMs(int millisecs) {
    transactionEventsFromQueueWaitMs.set(millisecs);

  }

  @Override
  public int getTransactionEventsFromQueueWaitMs() {
    return retriesToGetTransactionEventsFromQueue.get();
  }

}
