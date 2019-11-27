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
package org.apache.geode.internal.cache.backup;

import java.util.Set;

import org.apache.geode.distributed.internal.ClusterMessage;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.remote.AdminMultipleReplyProcessor;

class BackupReplyProcessor extends AdminMultipleReplyProcessor {

  private final BackupResultCollector resultCollector;

  BackupReplyProcessor(BackupResultCollector resultCollector, DistributionManager dm,
      Set<InternalDistributedMember> recipients) {
    super(dm, recipients);
    this.resultCollector = resultCollector;
  }

  @Override
  protected boolean stopBecauseOfExceptions() {
    return false;
  }

  @Override
  protected int getAckWaitThreshold() {
    // Disable the 15 second warning if the backup is taking a long time
    return 0;
  }

  @Override
  public long getAckSevereAlertThresholdMS() {
    // Don't log severe alerts for backups either
    return Long.MAX_VALUE;
  }

  @Override
  protected void process(ClusterMessage message, boolean warn) {
    if (message instanceof BackupResponse) {
      BackupResponse response = (BackupResponse) message;
      resultCollector.addToResults(response.getSender(), response.getPersistentIds());
    }
    super.process(message, warn);
  }
}
