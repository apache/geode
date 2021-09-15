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
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.lang3.SystemUtils;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import org.apache.geode.NativeRedisTestRule;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class PubSubNativeRedisAcceptanceTest extends AbstractPubSubIntegrationTest {

  private static final Logger logger = LogService.getLogger();
  private static long socketTimeWaitMsec = 240000;

  @ClassRule
  public static NativeRedisTestRule redis = new NativeRedisTestRule();

  @BeforeClass
  public static void runOnce() throws IOException {
    if (SystemUtils.IS_OS_LINUX) {
      try {
        String line = getCommandOutput("cat /proc/sys/net/ipv4/tcp_fin_timeout");
        socketTimeWaitMsec = Long.parseLong(line.trim());
      } catch (NumberFormatException | IOException ignored) {
      }
    } else if (SystemUtils.IS_OS_MAC) {
      try {
        String line = getCommandOutput("sysctl net.inet.tcp.msl");
        String[] parts = line.split(":");
        if (parts.length == 2) {
          socketTimeWaitMsec = 2 * Long.parseLong(parts[1].trim());
        }
      } catch (NumberFormatException | IOException ignored) {
      }
    }
    // Just leave timeout at the default if it's some other OS or there's a problem getting OS value
  }

  private static String getCommandOutput(String commandString) throws IOException {
    Process process = Runtime.getRuntime().exec(commandString);
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(process.getInputStream()));
    return reader.readLine();
  }

  @AfterClass
  public static void cleanup() throws InterruptedException {
    // This test consumes a lot of sockets and any subsequent tests may fail because of spurious
    // bind exceptions. Even though sockets are closed, they will remain in TIME_WAIT state so we
    // need to wait for that to clear up. It shouldn't take more than a minute or so.
    // For now a thread sleep is the simplest way to wait for the sockets to be out of the TIME_WAIT
    // state. The default timeout of 240 sec was chosen because that is the default duration for
    // TIME_WAIT on Windows. The timeouts for both mac and linux are significantly shorter.
    Thread.sleep(socketTimeWaitMsec);
  }

  @Override
  public int getPort() {
    return redis.getPort();
  }
}
