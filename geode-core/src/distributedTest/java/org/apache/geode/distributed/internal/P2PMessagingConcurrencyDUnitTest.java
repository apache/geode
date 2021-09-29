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

package org.apache.geode.distributed.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.MembershipTest;

/**
 * Tests one-way P2P messaging between two peers. A shared,
 * ordered connection is used and many concurrent tasks
 * compete on the sending side. Tests with and without SSL/TLS
 * enabled. TLS enabled exercises ByteBufferSharing and friends.
 */
@Category({MembershipTest.class})
// @RunWith(ConcurrentTestRunner.class)
public class P2PMessagingConcurrencyDUnitTest {

  @Rule
  public final DistributedRule distributedRule = new DistributedRule();

  private VM sender;
  private VM receiver;

  @Before
  public void before() {
    sender = VM.getVM(0);
    receiver = VM.getVM(1);
  }

  @Test
  public void foo(/* final ParallelExecutor executor */) {

    final String locators = distributedRule.getLocators();

    final InternalDistributedMember receiverMember =
        receiver.invoke(() -> {
          final ClusterDistributionManager cdm = getCDM(locators);
          final InternalDistributedMember localMember = cdm.getDistribution().getLocalMember();
          return localMember;
        });

    sender.invoke(() -> {
      final ClusterDistributionManager cdm = getCDM(locators);
      final InternalDistributedMember localMember = cdm.getDistribution().getLocalMember();
      final TestMessage msg = new TestMessage(localMember, receiverMember);
      cdm.putOutgoing(msg);
    });
  }

  private static ClusterDistributionManager getCDM(final String locators) {
    final Properties props = new Properties();
    props.put("locators", locators);
    final CacheFactory cacheFactory = new CacheFactory(props);
    final Cache cache = cacheFactory.create();

    // now peel back the covers and get at ClusterDistributionManager
    return (ClusterDistributionManager) ((InternalCache) cache).getDistributionManager();
  }

  private static class TestMessage extends DistributionMessage {

    TestMessage(final InternalDistributedMember sender,
        final InternalDistributedMember receiver) {
      setSender(sender);
      setRecipient(receiver);
    }

    // necessary for deserialization
    public TestMessage() {}

    @Override
    public int getProcessorType() {
      return OperationExecutors.STANDARD_EXECUTOR;
    }

    @Override
    protected void process(final ClusterDistributionManager dm) {
      // TODO: put some validation support in here maybe
    }

    @Override
    public void toData(final DataOutput out, final SerializationContext context)
        throws IOException {
      super.toData(out, context);

      final int seed = 1234;
      final Random random = new Random(seed);

      final int length = random.nextInt(32 * 1024 + 1); // 32 KiB + 1

      out.writeInt(length);

      final byte[] payload = new byte[length];
      random.nextBytes(payload);

      out.write(payload);
    }

    @Override
    public void fromData(final DataInput in, final DeserializationContext context)
        throws IOException, ClassNotFoundException {
      super.fromData(in, context);

      final int length = in.readInt();

      final byte[] payload = new byte[length];

      in.readFully(payload);

      // TODO: remove this diagnostic
      System.out.println("RECEIVED: size: " + length);
    }

    @Override
    public int getDSFID() {
      return NO_FIXED_ID; // for testing only!
    }
  }
}
