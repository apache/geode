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

import static org.apache.geode.distributed.ConfigurationProperties.SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_PROTOCOLS;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.security.GeneralSecurityException;
import java.security.Security;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import junitparams.Parameters;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
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
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.DistributedExecutorServiceRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.MembershipTest;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;
import org.apache.geode.test.version.VersionManager;

/**
 * In TLSv1.3, when a GCM-based cipher is used, there is a limit on the number
 * of bytes that may be encoded with a key. When that limit is reached, a TLS
 * KeyUpdate message is generated (by the SSLEngine). That message causes a new
 * key to be negotiated between the peer SSLEngines.
 *
 * This test arranges for a low encryption byte limit to be set in each of the
 * three members in turn: locator, sending member, receiving member. With the
 * low byte limit configured, the test sends P2P messages via TLS and verifies
 * that not only does one-way data transfer succeed, but also that no errors
 * are generated to the logs.
 *
 * The errors in the logs are the only way this test of one-way messaging can
 * detect a failure to handle KeyUpdate messages. That's because the Connection
 * framework transparently reconnects when connections are closed.
 */
@Category({MembershipTest.class})
@RunWith(GeodeParamsRunner.class)
public class P2PMessagingSSLTLSKeyUpdateDUnitTest {

  private static final String TLS_PROTOCOL = "TLSv1.3";
  private static final String TLS_CIPHER_SUITE = "TLS_AES_256_GCM_SHA384";

  private static final int ENCRYPTED_BYTES_LIMIT = 64 * 1024;

  // number of concurrent (sending) tasks to run
  private static final int SENDER_COUNT = 1;

  private static final int MESSAGE_SIZE = 1024;

  // how many messages will each sender generate?
  private static final int MESSAGES_PER_SENDER = 2 + ENCRYPTED_BYTES_LIMIT / MESSAGE_SIZE;

  {
    assertThat(MESSAGE_SIZE * MESSAGES_PER_SENDER > 10 * ENCRYPTED_BYTES_LIMIT);
  }

  private static Properties geodeConfigurationProperties;

  @Rule
  public final ClusterStartupRule clusterStartupRule = new ClusterStartupRule(3);

  @ClassRule
  public static final DistributedExecutorServiceRule senderExecutorServiceRule =
      new DistributedExecutorServiceRule(SENDER_COUNT, 3);

  private MemberVM sender;
  private MemberVM receiver;

  @After
  public void afterEach() {
    clusterStartupRule.getVM(0).bounceForcibly();
    clusterStartupRule.getVM(1).bounceForcibly();
    clusterStartupRule.getVM(2).bounceForcibly();
  }

  /*
   * bytes sent on sender JVM, bytes received on receiver JVM
   * (not used in test JVM)
   */
  private static LongAdder bytesTransferredAdder;

  private void configureJVMsAndStartClusterMembers(
      final long locatorEncryptedBytesLimit,
      final long senderEncryptedBytesLimit,
      final long receiverEncryptedBytesLimit)
      throws GeneralSecurityException, IOException {

    clusterStartupRule.getVM(0).invoke(
        setSecurityProperties(locatorEncryptedBytesLimit));
    clusterStartupRule.getVM(1).invoke(
        setSecurityProperties(senderEncryptedBytesLimit));
    clusterStartupRule.getVM(2).invoke(
        setSecurityProperties(receiverEncryptedBytesLimit));

    final Properties senderConfiguration = geodeConfigurationProperties();
    final Properties receiverConfiguration = geodeConfigurationProperties();

    final MemberVM locator =
        clusterStartupRule.startLocatorVM(0, 0, VersionManager.CURRENT_VERSION,
            x -> x.withProperties(senderConfiguration).withConnectionToLocator()
                .withoutClusterConfigurationService().withoutManagementRestService());

    sender = clusterStartupRule.startServerVM(1, senderConfiguration, locator.getPort());
    receiver = clusterStartupRule.startServerVM(2, receiverConfiguration, locator.getPort());
  }

  @NotNull
  private SerializableRunnableIF setSecurityProperties(final long encryptedBytesLimit) {
    return () -> {
      Security.setProperty("jdk.tls.keyLimits",
          "AES/GCM/NoPadding KeyUpdate " + encryptedBytesLimit);

      final Class<?> sslCipher = Class.forName("sun.security.ssl.SSLCipher");
      final Field cipherLimits = sslCipher.getDeclaredField("cipherLimits");
      cipherLimits.setAccessible(true);
      assertThat((Map<String, Long>) cipherLimits.get(null)).containsEntry(
          "AES/GCM/NOPADDING:KEYUPDATE",
          encryptedBytesLimit);
    };
  }

  @Test
  @Parameters({
      "65536, 137438953472, 137438953472",
      "137438953472, 65536, 137438953472",
      "137438953472, 137438953472, 65536",
  })
  public void testP2PMessagingWithKeyUpdate(
      final long locatorEncryptedBytesLimit,
      final long senderEncryptedBytesLimit,
      final long receiverEncryptedBytesLimit)
      throws GeneralSecurityException, IOException {

    configureJVMsAndStartClusterMembers(locatorEncryptedBytesLimit, senderEncryptedBytesLimit,
        receiverEncryptedBytesLimit);

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
        for (int messageId = firstMessageId; messageId < firstMessageId
            + MESSAGES_PER_SENDER; messageId++) {
          final TestMessage msg = new TestMessage(receiverMember, messageId);

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

    await().timeout(Duration.ofSeconds(10)).untilAsserted(
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

    TestMessage(final InternalDistributedMember receiver,
        final int messageId) {
      setRecipient(receiver);
      this.messageId = messageId;
    }

    // necessary for deserialization
    public TestMessage() {
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

      final ThreadLocalRandom random = ThreadLocalRandom.current();
      final int length = MESSAGE_SIZE;

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
  private static Properties geodeConfigurationProperties()
      throws GeneralSecurityException, IOException {
    // subsequent calls must return the same value so members agree on credentials
    if (geodeConfigurationProperties == null) {
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
      final Properties props = memberStore.propertiesWith("all", false, false);
      props.setProperty(SSL_PROTOCOLS, TLS_PROTOCOL);
      props.setProperty(SSL_CIPHERS, TLS_CIPHER_SUITE);
      geodeConfigurationProperties = props;
    }
    return geodeConfigurationProperties;
  }
}
