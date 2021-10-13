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

package org.apache.geode.cache.wan.internal.serial;

import static java.lang.String.format;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.internal.GatewaySenderTypeFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.wan.GatewaySenderAttributes;
import org.apache.geode.internal.cache.wan.GatewaySenderException;
import org.apache.geode.internal.cache.wan.MutableGatewaySenderAttributes;
import org.apache.geode.internal.cache.xmlcache.SerialGatewaySenderCreation;
import org.apache.geode.internal.statistics.StatisticsClock;

public class SerialGatewaySenderTypeFactory implements GatewaySenderTypeFactory {
  @Override
  public void validate(final @NotNull MutableGatewaySenderAttributes attributes)
      throws GatewaySenderException {

    if (!attributes.getAsyncEventListeners().isEmpty()) {
      throw new GatewaySenderException(
          format(
              "SerialGatewaySender %s cannot define a remote site because at least AsyncEventListener is already added. Both listeners and remote site cannot be defined for the same gateway sender.",
              attributes.getId()));
    }

    if (attributes.mustGroupTransactionEvents() && attributes.getDispatcherThreads() > 1) {
      throw new GatewaySenderException(
          format(
              "SerialGatewaySender %s cannot be created with group transaction events set to true when dispatcher threads is greater than 1",
              attributes.getId()));
    }

    if (attributes.getOrderPolicy() == null && attributes.getDispatcherThreads() > 1) {
      attributes.setOrderPolicy(GatewaySender.DEFAULT_ORDER_POLICY);
    }

  }

  @Override
  public GatewaySender create(final @NotNull InternalCache cache,
      final @NotNull StatisticsClock clock,
      final @NotNull GatewaySenderAttributes attributes) {
    return new SerialGatewaySenderImpl(cache, clock, attributes);
  }

  @Override
  public GatewaySender createCreation(final @NotNull InternalCache cache,
      final @NotNull GatewaySenderAttributes attributes) {
    return new SerialGatewaySenderCreation(cache, attributes);
  }
}
