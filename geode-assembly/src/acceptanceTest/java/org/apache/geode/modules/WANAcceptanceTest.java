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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

@RunWith(Parameterized.class)
public class WANAcceptanceTest {

  @ClassRule
  public static GfshRule gfshRule = new GfshRule();

  public WANAcceptanceTest(String serverCommand) {
    // Site 1
    GfshScript.of(
        "start locator --name=locatorSite1 --port=10334 --J=-Dgemfire.distributed-system-id=1 --J=-Dgemfire.remote-locators=localhost[10335]",
        "start server --name=serverSite1 --locators=localhost[10334] --J=-Dgemfire.distributed-system-id=1 "
            + serverCommand,
        "create gateway-sender --id=sender1 --parallel=true --remote-distributed-system-id=2",
        "create region --name=TestRegion --type=PARTITION --gateway-sender-id=sender1",
        "create gateway-receiver",
        "put --region=/TestRegion --key-class=java.lang.Long --key=1 --value-class=java.lang.String --value=hello",
        "put --region=/TestRegion --key-class=java.lang.Long --key=2 --value-class=java.lang.String --value=hola")
        .execute(gfshRule);


    // Site 2
    GfshScript.of(
        "start locator --name=locatorSite2 --port=10335 --J=-Dgemfire.distributed-system-id=2 --J=-Dgemfire.remote-locators=localhost[10334] --http-service-port=7071 --J=-Dgemfire.jmx-manager-port=1098",
        "start server --name=serverSite2 --server-port=40405 --locators=localhost[10335] --J=-Dgemfire.distributed-system-id=2 "
            + serverCommand,
        "create gateway-sender --id=sender2 --parallel=true --remote-distributed-system-id=1",
        "create region --name=TestRegion --type=PARTITION --gateway-sender-id=sender2",
        "create gateway-receiver",
        "put --region=/TestRegion --key-class=java.lang.Long --key=3 --value-class=java.lang.String --value=hallo",
        "put --region=/TestRegion --key-class=java.lang.Long --key=4 --value-class=java.lang.String --value=\"bon jour\"")
        .execute(gfshRule);
  }

  @After
  public void teardown() {
    GfshScript.of("connect --locator=localhost[10334]", "destroy region --name=TestRegion",
        "destroy gateway-sender --id=sender1", "destroy gateway-receiver").execute(gfshRule);

    GfshScript.of("connect --locator=localhost[10335]", "destroy region --name=TestRegion",
        "destroy gateway-sender --id=sender2", "destroy gateway-receiver").execute(gfshRule);
  }

  @Test
  public void testReplicationOccurs() {
    GeodeAwaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
      assertThat(
          GfshScript.of("connect --locator=localhost[10334]",
              "query --query=\"SELECT * FROM /TestRegion\"").execute(gfshRule).getOutputText())
                  .contains("hallo", "bon jour", "hello", "hola");
    });

    GeodeAwaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
      assertThat(
          GfshScript.of("connect --locator=localhost[10335]",
              "query --query=\"SELECT * FROM /TestRegion\"").execute(gfshRule).getOutputText())
                  .contains("hallo", "bon jour", "hello", "hola");
    });
  }

  @Parameterized.Parameters
  public static List<String> getStartServerCommand() {
    return Arrays.asList("", "--experimental");
  }
}
