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
package org.apache.geode.internal.cache.wan;

import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.wan.GatewayQueueEvent;

/**
 * Represents <code>Cache</code> events going through <code>GatewaySender</code>s.
 *
 *
 * @since Geode 1.15
 */
public interface InternalGatewayQueueEvent extends GatewayQueueEvent {
  /**
   * @return the transactionId to which the event belongs or null if the event does not belong
   *         to a transaction.
   */
  TransactionId getTransactionId();

  /**
   * @return true if the event is the last one in the transaction it belongs to or if the
   *         event does not belong to a transaction. false, otherwise.
   */
  boolean isLastEventInTransaction();
}
