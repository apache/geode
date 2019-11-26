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
package org.apache.geode.internal.cache.xmlcache;

import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;

import java.util.List;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.internal.DistributionAdvisee;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderAttributes;

public class SerialAsyncEventQueueCreation extends AbstractGatewaySender implements GatewaySender {

  public SerialAsyncEventQueueCreation(InternalCache cache, GatewaySenderAttributes attrs) {
    super(cache, disabledClock(), attrs);
  }

  @Override
  public void distribute(EnumListenerEvent operation, EntryEventImpl event,
      List<Integer> remoteDSIds) {}

  @Override
  public void start(boolean cleanQueues) {}

  @Override
  public void stop() {}

  @Override
  public void rebalance() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fillInProfile(Profile profile) {}

  @Override
  public CancelCriterion getCancelCriterion() {
    return null;
  }

  @Override
  public DistributionAdvisor getDistributionAdvisor() {
    return null;
  }

  @Override
  public DistributionManager getDistributionManager() {
    return null;
  }

  @Override
  public String getFullPath() {
    return null;
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public DistributionAdvisee getParentAdvisee() {
    return null;
  }

  @Override
  public Profile getProfile() {
    return null;
  }

  @Override
  public int getSerialNumber() {
    return 0;
  }

  @Override
  public InternalDistributedSystem getSystem() {
    return null;
  }

  @Override
  public void setModifiedEventId(EntryEventImpl clonedEvent) {}
}
