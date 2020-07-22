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

import java.io.DataInput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.geode.cache.query.cq.internal.command.CloseCQ;
import org.apache.geode.cache.query.cq.internal.command.ExecuteCQ;
import org.apache.geode.cache.query.cq.internal.command.ExecuteCQ61;
import org.apache.geode.cache.query.cq.internal.command.GetCQStats;
import org.apache.geode.cache.query.cq.internal.command.GetDurableCQs;
import org.apache.geode.cache.query.cq.internal.command.MonitorCQ;
import org.apache.geode.cache.query.cq.internal.command.StopCQ;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache.query.internal.cq.ServerCQ;
import org.apache.geode.cache.query.internal.cq.spi.CqServiceFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.CommandInitializer;
import org.apache.geode.internal.serialization.Version;

public class CqServiceFactoryImpl implements CqServiceFactory {

  @Override
  public void initialize() {
    Map<Version, Command> versions = new HashMap<>();
    versions.put(Version.GFE_57, ExecuteCQ.getCommand());
    versions.put(Version.GFE_61, ExecuteCQ61.getCommand());
    CommandInitializer.registerCommand(MessageType.EXECUTECQ_MSG_TYPE, versions);
    CommandInitializer.registerCommand(MessageType.EXECUTECQ_WITH_IR_MSG_TYPE, versions);

    CommandInitializer.registerCommand(MessageType.GETCQSTATS_MSG_TYPE,
        Collections.singletonMap(Version.GFE_57, GetCQStats.getCommand()));
    CommandInitializer.registerCommand(MessageType.MONITORCQ_MSG_TYPE,
        Collections.singletonMap(Version.GFE_57, MonitorCQ.getCommand()));
    CommandInitializer.registerCommand(MessageType.STOPCQ_MSG_TYPE,
        Collections.singletonMap(Version.GFE_57, StopCQ.getCommand()));
    CommandInitializer.registerCommand(MessageType.CLOSECQ_MSG_TYPE,
        Collections.singletonMap(Version.GFE_57, CloseCQ.getCommand()));
    CommandInitializer.registerCommand(MessageType.GETDURABLECQS_MSG_TYPE,
        Collections.singletonMap(Version.GFE_70, GetDurableCQs.getCommand()));
  }

  @Override
  public CqService create(InternalCache cache) {
    return new CqServiceImpl(cache);
  }

  @Override
  public ServerCQ readCqQuery(DataInput in) throws ClassNotFoundException, IOException {
    ServerCQImpl cq = new ServerCQImpl();
    cq.fromData(in);
    return cq;
  }

}
