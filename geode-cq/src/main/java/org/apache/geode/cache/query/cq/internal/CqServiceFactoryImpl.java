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

package org.apache.geode.cache.query.cq.internal;

import static java.util.Collections.singletonMap;

import java.io.DataInput;
import java.io.IOException;

import org.apache.geode.cache.query.cq.internal.command.CloseCQ;
import org.apache.geode.cache.query.cq.internal.command.ExecuteCQ61;
import org.apache.geode.cache.query.cq.internal.command.GetCQStats;
import org.apache.geode.cache.query.cq.internal.command.GetDurableCQs;
import org.apache.geode.cache.query.cq.internal.command.MonitorCQ;
import org.apache.geode.cache.query.cq.internal.command.StopCQ;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache.query.internal.cq.ServerCQ;
import org.apache.geode.cache.query.internal.cq.spi.CqServiceFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.CommandRegistry;
import org.apache.geode.internal.serialization.KnownVersion;

public class CqServiceFactoryImpl implements CqServiceFactory {

  @Override
  public void initialize() {}

  @Override
  public CqService create(InternalCache cache, CommandRegistry commandRegistry) {
    commandRegistry.register(MessageType.EXECUTECQ_MSG_TYPE,
        singletonMap(KnownVersion.OLDEST, ExecuteCQ61.getCommand()));
    commandRegistry.register(MessageType.EXECUTECQ_WITH_IR_MSG_TYPE,
        singletonMap(KnownVersion.OLDEST, ExecuteCQ61.getCommand()));
    commandRegistry.register(MessageType.GETCQSTATS_MSG_TYPE,
        singletonMap(KnownVersion.OLDEST, GetCQStats.getCommand()));
    commandRegistry.register(MessageType.MONITORCQ_MSG_TYPE,
        singletonMap(KnownVersion.OLDEST, MonitorCQ.getCommand()));
    commandRegistry.register(MessageType.STOPCQ_MSG_TYPE,
        singletonMap(KnownVersion.OLDEST, StopCQ.getCommand()));
    commandRegistry.register(MessageType.CLOSECQ_MSG_TYPE,
        singletonMap(KnownVersion.OLDEST, CloseCQ.getCommand()));
    commandRegistry.register(MessageType.GETDURABLECQS_MSG_TYPE,
        singletonMap(KnownVersion.OLDEST, GetDurableCQs.getCommand()));

    return new CqServiceImpl(cache);
  }

  @Override
  public ServerCQ readCqQuery(DataInput in) throws ClassNotFoundException, IOException {
    ServerCQImpl cq = new ServerCQImpl();
    cq.fromData(in);
    return cq;
  }

}
