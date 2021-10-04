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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.cache.ha.ThreadIdentifier;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.VersionedDataInputStream;
import org.apache.geode.test.junit.Repeat;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;
import org.apache.geode.test.junit.rules.RepeatRule;

public class EventIDTest {
  @Rule
  public ExecutorServiceRule executorService = new ExecutorServiceRule();

  @Rule
  public RepeatRule repeat = new RepeatRule();

  @Test
  public void emptyEventIdCanBeSerializedWithCurrentVersion()
      throws IOException, ClassNotFoundException {
    emptyEventIdCanBeSerialized(KnownVersion.CURRENT);

  }

  @Test
  public void emptyEventIdCanBeSerializedToGeode100() throws IOException, ClassNotFoundException {
    emptyEventIdCanBeSerialized(KnownVersion.GFE_90);
  }

  private void emptyEventIdCanBeSerialized(KnownVersion version)
      throws IOException, ClassNotFoundException {
    EventID eventID = new EventID();
    HeapDataOutputStream out = new HeapDataOutputStream(version);
    DataSerializer.writeObject(eventID, out);

    EventID result = DataSerializer.readObject(
        new VersionedDataInputStream(new ByteArrayInputStream(out.toByteArray()), version));

    assertThat(result.getMembershipID()).isEqualTo(eventID.getMembershipID());
  }

  @Test
  @Repeat(10)
  public void threadIDIsWrappedAround() throws Exception {
    EventID.ThreadAndSequenceIDWrapper wrapper = new EventID.ThreadAndSequenceIDWrapper();
    long start = wrapper.threadID;

    int numberOfThreads = 100000;

    List<Future<Long>> futures = new ArrayList<>();
    for (int i = 0; i < numberOfThreads; i++) {
      futures.add(executorService.submit(this::getThreadID));
    }
    for (Future<Long> future : futures) {
      future.get();
    }
    long lastThreadID = executorService.submit(this::getThreadID).get();
    long expected = start + numberOfThreads + 1;
    if (expected >= ThreadIdentifier.MAX_THREAD_PER_CLIENT) {
      // wrap around ThreadIdentifier.MAX_THREAD_PER_CLIENT (1,000,000) and 1,000,000
      // is never used.
      assertThat(lastThreadID).isEqualTo(expected - ThreadIdentifier.MAX_THREAD_PER_CLIENT + 1);
    } else {
      assertThat(lastThreadID).isEqualTo(expected);
    }
  }

  private long getThreadID() {
    EventID.ThreadAndSequenceIDWrapper wrapper = new EventID.ThreadAndSequenceIDWrapper();
    return wrapper.threadID;
  }
}
