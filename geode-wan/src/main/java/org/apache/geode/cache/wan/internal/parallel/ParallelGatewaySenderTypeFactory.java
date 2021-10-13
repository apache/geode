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

package org.apache.geode.cache.wan.internal.parallel;

import static java.lang.String.format;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.internal.GatewaySenderTypeFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.wan.GatewaySenderAttributes;
import org.apache.geode.internal.cache.wan.GatewaySenderException;
import org.apache.geode.internal.cache.wan.MutableGatewaySenderAttributes;
import org.apache.geode.internal.cache.xmlcache.ParallelGatewaySenderCreation;
import org.apache.geode.internal.statistics.StatisticsClock;

public class ParallelGatewaySenderTypeFactory implements GatewaySenderTypeFactory {

  @Override
  public @NotNull String getType() {
    return "ParallelGatewaySender";
  }

  @Override
  public void validate(final @NotNull MutableGatewaySenderAttributes attributes)
      throws GatewaySenderException {
    if ((attributes.getOrderPolicy() != null)
        && attributes.getOrderPolicy().equals(GatewaySender.OrderPolicy.THREAD)) {
      throw new GatewaySenderException(
          format("%s %s can not be created with OrderPolicy %s",
              getType(), attributes.getId(), attributes.getOrderPolicy()));
    }
  }

  @Override
  public GatewaySender create(final @NotNull InternalCache cache,
      final @NotNull StatisticsClock clock,
      final @NotNull GatewaySenderAttributes attributes) {
    return new ParallelGatewaySenderImpl(cache, clock, attributes);
  }

  @Override
  public GatewaySender createCreation(final @NotNull InternalCache cache,
      final @NotNull GatewaySenderAttributes attributes) {
    return new ParallelGatewaySenderCreation(cache, attributes);
  }

}
