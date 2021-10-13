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

package org.apache.geode.cache.wan.internal;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.wan.GatewaySenderAttributes;
import org.apache.geode.internal.cache.wan.GatewaySenderException;
import org.apache.geode.internal.cache.wan.MutableGatewaySenderAttributes;
import org.apache.geode.internal.statistics.StatisticsClock;

public interface GatewaySenderTypeFactory {

  void validate(@NotNull MutableGatewaySenderAttributes attributes) throws GatewaySenderException;

  GatewaySender create(@NotNull InternalCache cache, @NotNull StatisticsClock clock,
      @NotNull GatewaySenderAttributes attributes);

  GatewaySender createCreation(@NotNull InternalCache cache,
      @NotNull GatewaySenderAttributes attributes);

}
