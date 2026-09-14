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
package org.apache.geode.cache.client.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.internal.cache.tier.InterestType;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

/**
 * Exercises, over a real client connection to a running server, how the register-interest command
 * reads the message part that carries its interest result policy.
 *
 * <p>
 * A client op builds a register-interest request whose policy part carries a type other than the
 * policy argument, and sends it. The helper type records whether an instance of it is created on
 * the server while the part is read. The server must read the part only as its expected policy type
 * and refuse a part carrying any other type.
 */
@Category({ClientServerTest.class})
public class RegisterInterestPolicyPartIntegrationTest {

  private static final String REGION_NAME = "region";

  @Rule
  public ServerStarterRule server =
      new ServerStarterRule().withRegion(RegionShortcut.REPLICATE, REGION_NAME).withAutoStart();

  private PoolImpl pool;

  @Before
  public void setUp() {
    OtherPartType.reset();
    final PoolFactory poolFactory = PoolManager.createFactory();
    poolFactory.addServer("localhost", server.getPort());
    poolFactory.setReadTimeout(10_000);
    poolFactory.setMinConnections(1);
    pool = (PoolImpl) poolFactory.create("testPool");
  }

  @After
  public void tearDown() {
    if (pool != null) {
      pool.destroy();
    }
  }

  @Test
  public void serverDoesNotProduceAnotherTypeFromThePolicyPart() {
    try {
      pool.execute(new PolicyPartOfAnotherTypeOp(REGION_NAME));
    } catch (final Exception ignored) {
      // The request does not complete: the point of interest is which type the server produced
      // while reading the part, which is recorded independently below.
    }

    assertThat(OtherPartType.instantiated)
        .as("reading the policy part must not produce a type other than the policy on the server")
        .isFalse();
  }

  /**
   * A register-interest request whose policy part carries a type other than the policy argument.
   * Sends the request and does not attempt to interpret the response.
   */
  private static class PolicyPartOfAnotherTypeOp extends AbstractOp {

    PolicyPartOfAnotherTypeOp(final String region) {
      super(MessageType.REGISTER_INTEREST, 7);
      getMessage().addStringPart(region, true);
      getMessage().addIntPart(InterestType.KEY.ordinal());
      getMessage().addObjPart(new OtherPartType());
      getMessage().addBytesPart(new byte[] {(byte) 0x00});
      getMessage().addStringOrObjPart("key");
      getMessage().addBytesPart(new byte[] {(byte) 0x00});
      getMessage().addBytesPart(new byte[] {(byte) DataPolicy.REPLICATE.ordinal(), (byte) 0x01});
    }

    @Override
    protected Message createResponseMessage() {
      return new ChunkedMessage(1, KnownVersion.CURRENT);
    }

    @Override
    protected Object processResponse(final Message msg) throws Exception {
      // Drain the whole response so this op does not return until the server has finished
      // handling the request.
      final ChunkedMessage chunkedMessage = (ChunkedMessage) msg;
      chunkedMessage.readHeader();
      do {
        chunkedMessage.receiveChunk();
      } while (!chunkedMessage.isLastChunk());
      return null;
    }

    @Override
    protected boolean isErrorResponse(final MessageType msgType) {
      return false;
    }

    @Override
    protected long startAttempt(final ConnectionStats stats) {
      return 0;
    }

    @Override
    protected void endSendAttempt(final ConnectionStats stats, final long start) {}

    @Override
    protected void endAttempt(final ConnectionStats stats, final long start) {}
  }

  /**
   * A serializable type other than the register-interest policy argument. It records whether an
   * instance of it is created, so a test can tell which type a part produced.
   */
  public static class OtherPartType implements Serializable {
    private static final long serialVersionUID = 1L;

    static volatile boolean instantiated = false;

    static void reset() {
      instantiated = false;
    }

    private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
      in.defaultReadObject();
      instantiated = true;
    }
  }
}
