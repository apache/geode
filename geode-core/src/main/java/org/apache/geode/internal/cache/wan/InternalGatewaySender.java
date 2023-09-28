/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.cache.wan;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySenderStartupAction;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.statistics.StatisticsClock;

public interface InternalGatewaySender extends GatewaySender {

  Set<RegionQueue> getQueues();

  AbstractGatewaySenderEventProcessor getEventProcessor();

  boolean isPrimary();

  GatewaySenderStats getStatistics();

  StatisticsClock getStatisticsClock();

  boolean waitUntilFlushed(long timeout, TimeUnit unit) throws InterruptedException;

  boolean isForwardExpirationDestroy();

  boolean getIsMetaQueue();

  InternalCache getCache();

  void destroy(boolean initiator);

  void setStartEventProcessor(boolean isPaused);

  /**
   * Recovers partition region used by parallel gateway-sender queue. Parallel gateway sender
   * queue region is colocated with partition region on which is collecting events. It is necessary
   * to recover colocated gateway sender queue region, so it doesn't block the colocated data
   * region to reach the online status.
   */
  void recoverInStoppedState();

  /**
   * Returns the startup-action of the <code>GatewaySender</code>. This action parameter is set
   * after start, stop, pause and resume gateway-sender gfsh commands.
   *
   * @return startup action parameter value
   *
   * @see GatewaySenderStartupAction
   */
  GatewaySenderStartupAction getStartupAction();

  /**
   * This method returns startup action of gateway-sender. The startup action is calculated
   * based on the startup-action (please check <code>{@link GatewaySenderStartupAction}</code>) and
   * manual-start parameters. If set, then startup-action parameter has advantage over
   * the manual-start parameter.
   *
   * @return startup action
   *
   * @see GatewaySenderStartupAction
   */
  GatewaySenderStartupAction calculateStartupActionForGatewaySender();

  int getEventQueueSize();


  void prepareForStop();

}
