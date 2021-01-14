/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.modules;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;
import org.junit.Test;


public class MemcachedAcceptanceTest extends AbstractDockerizedAcceptanceTest {
  public MemcachedAcceptanceTest(String launchCommand) throws IOException, InterruptedException {
    launch(launchCommand);
  }

  @Test
  public void testMemcached() throws IOException, ExecutionException, InterruptedException {
    MemcachedClient client = new MemcachedClient(
        Collections.singletonList(new InetSocketAddress(InetAddress.getLocalHost(), memcachePort)));

    OperationFuture<Boolean> add1 = client.add("key1", 1, "value1");
    assertThat(add1.get()).isTrue();
    OperationFuture<Boolean> add2 = client.add("key2", 1, "value2");
    assertThat(add2.get()).isTrue();

    assertThat(client.get("key1")).isEqualTo("value1");
    assertThat(client.get("key2")).isEqualTo("value2");

    client.shutdown();
  }
}
