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
package org.apache.geode.management.internal;

import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import org.apache.geode.StatisticsFactory;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.statistics.StatisticsClock;

@FunctionalInterface
interface FederatingManagerFactory {

  FederatingManager create(ManagementResourceRepo repo, InternalDistributedSystem system,
      SystemManagementService service, InternalCache cache, StatisticsFactory statisticsFactory,
      StatisticsClock statisticsClock, MBeanProxyFactory proxyFactory, MemberMessenger messenger,
      Supplier<ExecutorService> executorServiceSupplier);
}
