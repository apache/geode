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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import junitparams.Parameters;
import org.jetbrains.annotations.NotNull;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.CertificateBuilder;
import org.apache.geode.cache.ssl.CertificateMaterial;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.DistributedExecutorServiceRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.MembershipTest;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;
import org.apache.geode.test.version.VersionManager;

/**
 * Tests one-way P2P messaging between two peers.
 * Many concurrent tasks compete on the sending side.
 * The main purpose of the test is to exercise
 * ByteBufferSharing and friends.
 *
 * Tests combinations of: conserve-sockets true/false,
 * TLS on/off, and socket-buffer-size for sender
 * and receiver both set to the default (and equal)
 * and set to the sender's buffer twice as big as the
 * receiver's buffer.
 *
 */
@Category({MembershipTest.class})
@RunWith(GeodeParamsRunner.class)
public class P2PMessagingConcurrencyDUnitTest {

  // how many messages will each sender generate?
  private static final int MESSAGES_PER_SENDER = 1_000;

  // number of concurrent (sending) tasks to run
  private static final int SENDER_COUNT = 10;

  // (exclusive) upper bound of random message size, in bytes
  private static final int LARGEST_MESSAGE_BOUND = 32 * 1024 + 2; // 32KiB + 2

  // random seed
  private static final int RANDOM_SEED = 1234;

  private static Properties securityProperties;

  @Rule
  public final ClusterStartupRule clusterStartupRule = new ClusterStartupRule(3);

  @ClassRule
  public static final DistributedExecutorServiceRule senderExecutorServiceRule =
      new DistributedExecutorServiceRule(SENDER_COUNT, 3);

  private MemberVM sender;
  private MemberVM receiver;

  /*
   * bytes sent on sender JVM, bytes received on receiver JVM
   * (not used in test JVM)
   */
  private static LongAdder bytesTransferredAdder;

  private void configure(
      final boolean conserveSockets,
      final boolean useTLS,
      final int sendSocketBufferSize,
      final int receiveSocketBufferSize) throws GeneralSecurityException, IOException {

    final Properties senderConfiguration =
        gemFireConfiguration(conserveSockets, useTLS, sendSocketBufferSize);
    final Properties receiverConfiguration =
        gemFireConfiguration(conserveSockets, useTLS, receiveSocketBufferSize);

    final MemberVM locator =
        clusterStartupRule.startLocatorVM(0, 0, VersionManager.CURRENT_VERSION,
            x -> x.withProperties(senderConfiguration).withConnectionToLocator()
                .withoutClusterConfigurationService().withoutManagementRestService());

    sender = clusterStartupRule.startServerVM(1, senderConfiguration, locator.getPort());
    receiver = clusterStartupRule.startServerVM(2, receiverConfiguration, locator.getPort());
  }

  @Test
  @Parameters({
      /*
       * all combinations of flags with buffer sizes:
       * (equal), larger/smaller, smaller/larger, minimal
       */
      "true, true, 32768, 32768",
      "true, true, 65536, 32768",
      "true, true, 32768, 65536",
      "true, true, 1024, 1024",
      "true, false, 32768, 32768",
      "true, false, 65536, 32768",
      "true, false, 32768, 65536",
      "true, false, 1024, 1024",
      "false, true, 32768, 32768",
      "false, true, 65536, 32768",
      "false, true, 32768, 65536",
      "false, true, 1024, 1024",
      "false, false, 32768, 32768",
      "false, false, 65536, 32768",
      "false, false, 32768, 65536",
      "false, false, 1024, 1024",
  })
  public void testP2PMessaging(
      final boolean conserveSockets,
      final boolean useTLS,
      final int sendSocketBufferSize,
      final int receiveSocketBufferSize) throws GeneralSecurityException, IOException {

    configure(conserveSockets, useTLS, sendSocketBufferSize, receiveSocketBufferSize);

    final InternalDistributedMember receiverMember =
        receiver.invoke(() -> {

          bytesTransferredAdder = new LongAdder();

          final ClusterDistributionManager cdm = getCDM();
          final InternalDistributedMember localMember = cdm.getDistribution().getLocalMember();
          return localMember;

        });

    sender.invoke(() -> {

      bytesTransferredAdder = new LongAdder();

      final ClusterDistributionManager cdm = getCDM();
      final AtomicInteger nextSenderId = new AtomicInteger();

      /*
       * When this comment was written DistributedExecutorServiceRule's
       * getExecutorService had no option to specify the number of threads.
       * If it had we might have liked to specify the number of CPU cores.
       * In an ideal world we'd want only as many threads as CPUs here.
       * OTOH the P2P messaging system at the time this comment was written,
       * used blocking I/O, so we were not, as it turns out, living in that
       * ideal world.
       */
      final ExecutorService executor = senderExecutorServiceRule.getExecutorService();

      final CountDownLatch startLatch = new CountDownLatch(SENDER_COUNT);
      final CountDownLatch stopLatch = new CountDownLatch(SENDER_COUNT);
      final LongAdder failedRecipientCount = new LongAdder();

      final Runnable doSending = () -> {
        final int senderId = nextSenderId.getAndIncrement();
        try {
          startLatch.countDown();
          startLatch.await();
        } catch (final InterruptedException e) {
          throw new RuntimeException("doSending failed", e);
        }
        final int firstMessageId = senderId * SENDER_COUNT;
        final Random random = new Random(RANDOM_SEED);
        for (int messageId = firstMessageId; messageId < firstMessageId
            + MESSAGES_PER_SENDER; messageId++) {
          final TestMessage msg = new TestMessage(receiverMember, random, messageId);

          /*
           * HERE is the Geode API entrypoint we intend to test (putOutgoing()).
           */
          final Set<InternalDistributedMember> failedRecipients = cdm.putOutgoing(msg);

          if (failedRecipients != null) {
            failedRecipientCount.add(failedRecipients.size());
          }
        }
        stopLatch.countDown();
      };

      for (int i = 0; i < SENDER_COUNT; ++i) {
        executor.submit(doSending);
      }

      stopLatch.await();

      assertThat(failedRecipientCount.sum()).as("message delivery failed N times").isZero();

    });

    final long bytesSent = getByteCount(sender);

    await().untilAsserted(
        () -> assertThat(getByteCount(receiver))
            .as("bytes received != bytes sent")
            .isEqualTo(bytesSent));
  }

  private long getByteCount(final MemberVM member) {
    return member.invoke(() -> bytesTransferredAdder.sum());
  }

  private static ClusterDistributionManager getCDM() {
    return (ClusterDistributionManager) ((InternalCache) CacheFactory.getAnyInstance())
        .getDistributionManager();
  }

  private static class TestMessage extends DistributionMessage {

    /*
     * When this comment was written, messageId wasn't used for anything.
     * The field was added during a misguided attempt to add SHA-256
     * digest verification on sender and receiver. Then I figured out
     * that there's no way to parallelize that (for the sender) so
     * I settled for merely validating the number of bytes transferred.
     * Left the field here in case it comes in handy later.
     */
    private volatile int messageId;
    private final Random random;

    TestMessage(final InternalDistributedMember receiver,
        final Random random, final int messageId) {
      setRecipient(receiver);
      this.random = random;
      this.messageId = messageId;
    }

    // necessary for deserialization
    public TestMessage() {
      random = null;
      messageId = 0;
    }

    @Override
    public int getProcessorType() {
      return OperationExecutors.STANDARD_EXECUTOR;
    }

    @Override
    protected void process(final ClusterDistributionManager dm) {}

    @Override
    public void toData(final DataOutput out, final SerializationContext context)
        throws IOException {
      super.toData(out, context);

      out.writeInt(messageId);

      final int length = random.nextInt(LARGEST_MESSAGE_BOUND);

      out.writeInt(length);

      final byte[] payload = new byte[length];
      random.nextBytes(payload);

      out.write(payload);

      /*
       * the LongAdder should ensure that we don't introduce any (much)
       * synchronization with other concurrent tasks here
       */
      bytesTransferredAdder.add(length);
    }

    @Override
    public void fromData(final DataInput in, final DeserializationContext context)
        throws IOException, ClassNotFoundException {
      super.fromData(in, context);

      messageId = in.readInt();

      final int length = in.readInt();

      final byte[] payload = new byte[length];

      in.readFully(payload);

      bytesTransferredAdder.add(length);
    }

    @Override
    public int getDSFID() {
      return NO_FIXED_ID; // for testing only!
    }
  }

  @NotNull
  private static Properties gemFireConfiguration(
      final boolean conserveSockets, final boolean useTLS,
      final int socketBufferSize)
      throws GeneralSecurityException, IOException {

    final Properties props;
    if (useTLS) {
      props = securityProperties();
    } else {
      props = new Properties();
    }

    props.setProperty("socket-buffer-size", String.valueOf(socketBufferSize));

    /*
     * This is something we intend to test!
     * Send all messages, from all threads, on a single socket per recipient.
     * maintenance tip: to see what kind of connection you're getting you can
     * uncomment logging over in DirectChannel.sendToMany()
     *
     * careful: if you set a boolean it doesn't take hold! setting a String
     */
    props.setProperty("conserve-sockets", String.valueOf(conserveSockets));

    return props;
  }

  @NotNull
  private static Properties securityProperties() throws GeneralSecurityException, IOException {
    // subsequent calls must return the same value so members agree on credentials
    if (securityProperties == null) {
      final CertificateMaterial ca = new CertificateBuilder()
          .commonName("Test CA")
          .isCA()
          .generate();

      final CertificateMaterial serverCertificate = new CertificateBuilder()
          .commonName("member")
          .issuedBy(ca)
          .generate();

      final CertStores memberStore = new CertStores("member");
      memberStore.withCertificate("member", serverCertificate);
      memberStore.trust("ca", ca);
      // we want to exercise the ByteBufferSharing code paths; we don't care about client auth etc
      securityProperties = memberStore.propertiesWith("all", false, false);
    }
    return securityProperties;
  }

}
