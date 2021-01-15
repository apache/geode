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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.CommandInitializer;
import org.apache.geode.internal.serialization.KnownVersion;

public class CqServiceFactoryImplTest {
  @Test
  public void initialize() {
    final Integer[] messageTypes = {
        MessageType.EXECUTECQ_MSG_TYPE, MessageType.EXECUTECQ_WITH_IR_MSG_TYPE,
        MessageType.GETCQSTATS_MSG_TYPE, MessageType.MONITORCQ_MSG_TYPE,
        MessageType.STOPCQ_MSG_TYPE, MessageType.CLOSECQ_MSG_TYPE,
        MessageType.GETDURABLECQS_MSG_TYPE};

    final int previousSize = CommandInitializer.getCommands(KnownVersion.OLDEST).size();
    assertThat(CommandInitializer.getCommands(KnownVersion.OLDEST))
        .doesNotContainKeys(messageTypes);

    new CqServiceFactoryImpl().initialize();

    assertThat(CommandInitializer.getCommands(KnownVersion.OLDEST)).containsKeys(messageTypes)
        .hasSize(previousSize + messageTypes.length);
  }
}
