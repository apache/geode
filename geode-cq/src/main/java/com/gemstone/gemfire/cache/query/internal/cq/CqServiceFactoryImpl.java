/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.query.internal.cq;

import java.io.DataInput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.cache.query.internal.cq.spi.CqServiceFactory;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.CommandInitializer;
import com.gemstone.gemfire.internal.cache.tier.sockets.command.CloseCQ;
import com.gemstone.gemfire.internal.cache.tier.sockets.command.ExecuteCQ;
import com.gemstone.gemfire.internal.cache.tier.sockets.command.ExecuteCQ61;
import com.gemstone.gemfire.internal.cache.tier.sockets.command.GetCQStats;
import com.gemstone.gemfire.internal.cache.tier.sockets.command.GetDurableCQs;
import com.gemstone.gemfire.internal.cache.tier.sockets.command.MonitorCQ;
import com.gemstone.gemfire.internal.cache.tier.sockets.command.StopCQ;

public class CqServiceFactoryImpl implements CqServiceFactory {
  
  public void initialize() {
    {
      Map<Version, Command> versions = new HashMap<Version, Command>();
      versions.put(Version.GFE_57, ExecuteCQ.getCommand());
      versions.put(Version.GFE_61, ExecuteCQ61.getCommand());
      CommandInitializer.registerCommand(MessageType.EXECUTECQ_MSG_TYPE, versions);
      CommandInitializer.registerCommand(MessageType.EXECUTECQ_WITH_IR_MSG_TYPE, versions);
    }
    
    CommandInitializer.registerCommand(MessageType.GETCQSTATS_MSG_TYPE, Collections.singletonMap(Version.GFE_57, GetCQStats.getCommand()));
    CommandInitializer.registerCommand(MessageType.MONITORCQ_MSG_TYPE, Collections.singletonMap(Version.GFE_57, MonitorCQ.getCommand()));
    CommandInitializer.registerCommand(MessageType.STOPCQ_MSG_TYPE, Collections.singletonMap(Version.GFE_57, StopCQ.getCommand()));
    CommandInitializer.registerCommand(MessageType.CLOSECQ_MSG_TYPE, Collections.singletonMap(Version.GFE_57, CloseCQ.getCommand()));
    CommandInitializer.registerCommand(MessageType.GETDURABLECQS_MSG_TYPE, Collections.singletonMap(Version.GFE_70, GetDurableCQs.getCommand()));
  }

  @Override
  public CqService create(GemFireCacheImpl cache) {
    return new CqServiceImpl(cache);
  }

  @Override
  public ServerCQ readCqQuery(DataInput in) throws ClassNotFoundException, IOException {
    ServerCQImpl cq = new ServerCQImpl();
    cq.fromData(in);
    return cq;
  }

}
