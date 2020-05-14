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

import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.Set;

import org.junit.Test;

import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.inet.LocalHostUtil;

public class LatestLastAccessTimeMessageTest {

  @Test
  public void processMessageShouldLookForNullCache() throws Exception {
    final DistributionManager distributionManager = mock(DistributionManager.class);
    final LatestLastAccessTimeReplyProcessor replyProcessor =
        mock(LatestLastAccessTimeReplyProcessor.class);
    final InternalDistributedRegion region = mock(InternalDistributedRegion.class);
    Set<InternalDistributedMember> recipients = Collections.singleton(new InternalDistributedMember(
        LocalHostUtil.getLocalHost(), 1234));
    final LatestLastAccessTimeMessage<String> lastAccessTimeMessage =
        new LatestLastAccessTimeMessage<>(replyProcessor, recipients, region, "foo");
    lastAccessTimeMessage.process(mock(ClusterDistributionManager.class));
  }
}
