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
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
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
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.MembershipTest;
import org.apache.geode.test.version.VersionManager;

/**
 * In TLSv1.3, when a GCM-based cipher is used, there is a limit on the number
 * of bytes that may be encoded with a key. When that limit is reached, a TLS
 * KeyUpdate message is generated (by the SSLEngine). That message causes a new
 * key to be negotiated between the peer SSLEngines.
 *
 * This test arranges for a low encryption byte limit to be set for the sending member
 * and then for the receiving member. With the low byte limit configured, the test
 * sends P2P messages via TLS and verifies request-reply message processing.
 */
@Category({MembershipTest.class})
@RunWith(JUnitParamsRunner.class)
public class P2pMessagingSslTlsKeyUpdateDistributedTest {

  private static final String TLS_PROTOCOL = "TLSv1.3";
  private static final String TLS_CIPHER_SUITE = "TLS_AES_256_GCM_SHA384";

  private static final int ENCRYPTED_BYTES_LIMIT = 64 * 1024;

  private static final int MESSAGE_SIZE = 1024;

  /*
   * How many messages will be generated? We generate enough to cause KeyUpdate
   * to be generated, and then we generate many more beyond that. Even with buggy
   * wrap/unwrap logic, the retries in DirectChannel.sendToMany() and the transparent
   * connection reestablishment in ConnectionTable can mask those bugs. So to reliably
   * fail in the presence of bugs we need to generate lots of extra messages.
   */
  private static final int MESSAGES_PER_SENDER = ENCRYPTED_BYTES_LIMIT / MESSAGE_SIZE + 2000;

  {
    assertThat(MESSAGE_SIZE * MESSAGES_PER_SENDER > 10 * ENCRYPTED_BYTES_LIMIT);
  }

  public static final int MAX_REPLY_WAIT_MILLIS = 1_000;

  private static Properties geodeConfigurationProperties;

  @Rule
  public final ClusterStartupRule clusterStartupRule = new ClusterStartupRule(3);

  private MemberVM sender;
  private MemberVM receiver;

  @After
  public void afterEach() {
    /*
     * Disconnect DSs before killing JVMs.
     */
    clusterStartupRule.getVM(2).invoke(
        () -> ClusterStartupRule.getCache().close());
    clusterStartupRule.getVM(1).invoke(
        () -> ClusterStartupRule.getCache().close());
    clusterStartupRule.getVM(0).invoke(
        () -> ClusterStartupRule.getCache().close());

    clusterStartupRule.getVM(0).bounceForcibly();
    clusterStartupRule.getVM(1).bounceForcibly();
    clusterStartupRule.getVM(2).bounceForcibly();
  }

  /*
   * bytes sent on sender JVM, bytes received on receiver JVM
   * (not used in test JVM)
   */
  private static LongAdder bytesTransferredAdder;

  // in receiver JVM only
  private static LongAdder repliesGeneratedAdder;

  // in sender JVM only
  private static LongAdder repliesReceivedAdder;


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

  private @NotNull SerializableRunnableIF setSecurityProperties(final long encryptedBytesLimit) {
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
        receiver.invoke("get receiving member id", () -> {

          bytesTransferredAdder = new LongAdder();
          repliesGeneratedAdder = new LongAdder();

          final ClusterDistributionManager cdm = getCDM();
          final InternalDistributedMember localMember = cdm.getDistribution().getLocalMember();
          return localMember;
        });

    // by returning a value from the invoked lambda we make invocation synchronous
    final Boolean sendingComplete =
        sender.invoke("message sending and reply counting", () -> {

          bytesTransferredAdder = new LongAdder();
          repliesReceivedAdder = new LongAdder();

          final ClusterDistributionManager cdm = getCDM();

          int failedRecipientCount = 0;
          int droppedRepliesCount = 0;

          final ReplyProcessor21[] replyProcessors = new ReplyProcessor21[MESSAGES_PER_SENDER];

          // this loop sends request messages
          for (int messageId = 0; messageId < MESSAGES_PER_SENDER; messageId++) {

            final ReplyProcessor21 replyProcessor = new ReplyProcessor21(cdm, receiverMember);
            replyProcessors[messageId] = replyProcessor;
            final TestMessage msg = new TestMessage(messageId, receiverMember,
                replyProcessor.getProcessorId());

            final Set<InternalDistributedMember> failedRecipients = cdm.putOutgoing(msg);
            if (failedRecipients == null) {
              bytesTransferredAdder.add(MESSAGE_SIZE);
            } else {
              failedRecipientCount += failedRecipients.size();
            }
          }

          // this loop counts reply arrivals
          for (int messageId = 0; messageId < MESSAGES_PER_SENDER; messageId++) {
            final ReplyProcessor21 replyProcessor = replyProcessors[messageId];
            final boolean receivedReply =
                replyProcessor.waitForRepliesUninterruptibly(MAX_REPLY_WAIT_MILLIS);
            if (receivedReply) {
              repliesReceivedAdder.increment();
            } else {
              droppedRepliesCount += 1;
            }
          }

          assertThat((long) failedRecipientCount).as("message delivery failed N times").isZero();
          assertThat((long) droppedRepliesCount).as("some replies were dropped").isZero();
          return true;
        });

    // at this point, sender is done sending
    final long bytesSent = getByteCount(sender);

    await().untilAsserted(
        () -> {
          assertThat(getRepliesGenerated()).isEqualTo(MESSAGES_PER_SENDER);
          assertThat(getRepliesReceived()).isEqualTo(MESSAGES_PER_SENDER);
          assertThat(getByteCount(receiver))
              .as("bytes received != bytes sent")
              .isEqualTo(bytesSent);
        });
  }

  private long getRepliesGenerated() {
    return receiver.invoke(() -> repliesGeneratedAdder.sum());
  }

  private long getRepliesReceived() {
    return sender.invoke(() -> repliesReceivedAdder.sum());
  }

  private long getByteCount(final MemberVM member) {
    return member.invoke(() -> bytesTransferredAdder.sum());
  }

  private static ClusterDistributionManager getCDM() {
    return (ClusterDistributionManager) ((InternalCache) CacheFactory.getAnyInstance())
        .getDistributionManager();
  }

  private static class TestMessage extends DistributionMessage {
    private volatile int messageId;
    private volatile int replyProcessorId;
    private volatile int length;

    TestMessage(final int messageId,
        final InternalDistributedMember receiver,
        final int replyProcessorId) {
      setRecipient(receiver);
      this.messageId = messageId;
      this.replyProcessorId = replyProcessorId;
    }

    // necessary for deserialization
    public TestMessage() {
      messageId = 0;
      replyProcessorId = 0;
    }

    @Override
    public int getProcessorType() {
      return OperationExecutors.STANDARD_EXECUTOR;
    }

    @Override
    protected void process(final ClusterDistributionManager dm) {

      // In case bugs cause fromData to be called more times than this method,
      // we don't count the bytes as "transferred" until we're in this method.
      bytesTransferredAdder.add(length);

      final ReplyMessage replyMsg = new ReplyMessage();
      replyMsg.setRecipient(getSender());
      replyMsg.setProcessorId(replyProcessorId);
      replyMsg.setReturnValue("howdy!");
      dm.putOutgoing(replyMsg);
      repliesGeneratedAdder.increment();
    }

    @Override
    public void toData(final DataOutput out, final SerializationContext context)
        throws IOException {
      super.toData(out, context);

      out.writeInt(messageId);
      out.writeInt(replyProcessorId);

      final ThreadLocalRandom random = ThreadLocalRandom.current();
      final int length = MESSAGE_SIZE;

      out.writeInt(length);

      final byte[] payload = new byte[length];
      random.nextBytes(payload);

      out.write(payload);
    }

    @Override
    public void fromData(final DataInput in, final DeserializationContext context)
        throws IOException, ClassNotFoundException {
      super.fromData(in, context);

      messageId = in.readInt();
      replyProcessorId = in.readInt();

      length = in.readInt();

      final byte[] payload = new byte[length];

      in.readFully(payload);
    }

    @Override
    public int getDSFID() {
      return NO_FIXED_ID; // for testing only!
    }
  }

  private static @NotNull Properties geodeConfigurationProperties()
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
