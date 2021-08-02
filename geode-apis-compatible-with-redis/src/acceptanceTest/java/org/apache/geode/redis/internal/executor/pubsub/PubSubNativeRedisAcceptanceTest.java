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

package org.apache.geode.redis.internal.executor.pubsub;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;
import org.buildobjects.process.ProcBuilder;
import org.buildobjects.process.StreamConsumer;
import org.junit.AfterClass;
import org.junit.ClassRule;

import org.apache.geode.NativeRedisTestRule;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public class PubSubNativeRedisAcceptanceTest extends AbstractPubSubIntegrationTest {

  private static final Logger logger = LogService.getLogger();

  @ClassRule
  public static NativeRedisTestRule redis = new NativeRedisTestRule();

  @AfterClass
  public static void cleanup() {
    // This test consumes a lot of sockets and any subsequent tests may fail because of spurious
    // bind exceptions. Even though sockets are closed, they will remain in TIME_WAIT state so we
    // need to wait for that to clear up. It shouldn't take more than a minute or so.
    GeodeAwaitility.await().pollInterval(Duration.ofSeconds(10))
        .until(() -> countTimeWait() < 100);
  }

  @Override
  public int getPort() {
    return redis.getPort();
  }

  private static int countTimeWait() {
    AtomicInteger timeWaits = new AtomicInteger(0);

    StreamConsumer consumer = stream -> {
      InputStreamReader inputStreamReader = new InputStreamReader(stream);
      BufferedReader bufReader = new BufferedReader(inputStreamReader);
      String line;
      while ((line = bufReader.readLine()) != null) {
        if (line.contains("TIME_WAIT")) {
          timeWaits.incrementAndGet();
        }
      }
    };

    new ProcBuilder("netstat")
        .withArgs("-na")
        .withOutputConsumer(consumer)
        .withTimeoutMillis(60000)
        .run();

    logger.info("DEBUG: sockets in TIME_WAIT state: {}", timeWaits.get());

    return timeWaits.get();
  }

}
