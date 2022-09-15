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

import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.client.internal.LocatorDiscoveryCallback;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.cache.wan.GatewaySenderStartupAction;

public interface InternalGatewaySenderFactory extends GatewaySenderFactory {

  GatewaySenderFactory setForInternalUse(boolean b);

  GatewaySenderFactory addAsyncEventListener(AsyncEventListener listener);

  GatewaySenderFactory setBucketSorted(boolean bucketSorted);

  GatewaySender create(String senderIdFromAsyncEventQueueId);

  void configureGatewaySender(GatewaySender senderCreation);

  GatewaySenderFactory setLocatorDiscoveryCallback(LocatorDiscoveryCallback myLocatorCallback);

  /**
   * Sets the maximum number of retries to get events from the queue
   * to complete a transaction when groupTransactionEvents is true.
   *
   * @param retries the maximum number of retries.
   */
  GatewaySenderFactory setRetriesToGetTransactionEventsFromQueue(int retries);

  /**
   * Sets startup-action of the <code>GatewaySender</code>. This action parameter is set after
   * start, stop, pause and resume gateway-sender gfsh commands.
   *
   * @param gatewaySenderStartupAction Gateway-sender gatewaySenderStartupAction
   *
   * @see GatewaySenderStartupAction
   */
  GatewaySenderFactory setStartupAction(
      GatewaySenderStartupAction gatewaySenderStartupAction);
}
