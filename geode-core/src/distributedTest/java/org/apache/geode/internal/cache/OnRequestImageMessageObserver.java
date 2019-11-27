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
package org.apache.geode.internal.cache;

import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.ClusterMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;

public abstract class OnRequestImageMessageObserver extends DistributionMessageObserver {
  private final String regionName;
  private final Runnable onMessageReceived;
  private boolean hasExecutedOnMessageReceived = false;

  OnRequestImageMessageObserver(String regionName, Runnable onMessageReceived) {
    this.regionName = regionName;
    this.onMessageReceived = onMessageReceived;
  }

  @Override
  public void beforeProcessMessage(ClusterDistributionManager dm, ClusterMessage message) {
    if (message instanceof InitialImageOperation.RequestImageMessage) {
      InitialImageOperation.RequestImageMessage rim =
          (InitialImageOperation.RequestImageMessage) message;
      synchronized (this) {
        if (!hasExecutedOnMessageReceived && rim.regionPath.contains(regionName)) {
          onMessageReceived.run();
          hasExecutedOnMessageReceived = true;
        }
      }
    }
  }
}
