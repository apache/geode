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
package org.apache.geode.internal.cache.wan.serial;

import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;

public class TestSerialGatewaySenderEventProcessor extends SerialGatewaySenderEventProcessor {

  public TestSerialGatewaySenderEventProcessor(AbstractGatewaySender sender, String id,
      ThreadsMonitoring tMonitoring, boolean cleanQueues) {
    super(sender, id, tMonitoring, cleanQueues);
  }

  @Override
  protected void initializeMessageQueue(String id, boolean cleanQueues) {
    // Overridden to not create the RegionQueue in the constructor.
  }

  protected int getUnprocessedTokensSize() {
    return unprocessedTokens.size();
  }
}
