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
package org.apache.geode.internal.cache.tier.sockets;

import static org.apache.geode.internal.cache.tier.sockets.BaseCommand.readInterestResultPolicy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.internal.util.BlobHelper;
import org.apache.geode.test.junit.categories.ClientServerTest;

/**
 * Verifies how the register-interest commands read the message {@link Part} that carries the
 * interest result policy.
 *
 * <p>
 * The part is read as an {@link InterestResultPolicy}: it is accepted only in the form the client
 * writes it in, and a part carrying any other type is refused and that type is not produced. The
 * helper type below records whether an instance of it is created while a part is read.
 */
@Category({ClientServerTest.class})
public class RegisterInterestObjectPartTest {

  @Before
  public void setUp() {
    OtherPartType.reset();
  }

  @Test
  public void policyPartOfAnotherTypeIsRefusedWithoutProducingThatType() throws Exception {
    final byte[] objectPartBytes = BlobHelper.serializeToBlob(new OtherPartType());

    final Part part = new Part();
    part.setPartState(objectPartBytes, true);

    assertThat(catchThrowable(() -> readInterestResultPolicy(part)))
        .as("a policy part holding another type is refused")
        .isInstanceOf(IOException.class);

    assertThat(OtherPartType.instantiated)
        .as("reading the policy part must not produce a type other than the policy")
        .isFalse();
  }

  @Test
  public void nonObjectPolicyPartIsRefused() {
    final Part part = new Part();
    part.setPartState(new byte[] {0x01, 0x25, 0x02}, false);

    assertThat(catchThrowable(() -> readInterestResultPolicy(part)))
        .as("a policy part that is not object typed is refused")
        .isInstanceOf(IOException.class);
  }

  @Test
  public void eachPolicyValueRoundTripsThroughThePart() throws Exception {
    for (final InterestResultPolicy expected : new InterestResultPolicy[] {
        InterestResultPolicy.NONE, InterestResultPolicy.KEYS, InterestResultPolicy.KEYS_VALUES}) {
      final Part part = new Part();
      part.setPartState(BlobHelper.serializeToBlob(expected), true);

      assertThat(readInterestResultPolicy(part))
          .as("policy %s survives a write and read of the policy part", expected)
          .isSameAs(expected);
    }
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
