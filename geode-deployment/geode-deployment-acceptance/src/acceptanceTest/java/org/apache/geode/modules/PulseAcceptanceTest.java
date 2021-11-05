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

import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.RestTemplate;

public class PulseAcceptanceTest extends AbstractDockerizedAcceptanceTest {

  private static RestTemplate restTemplate;

  public PulseAcceptanceTest(String launchCommand) throws IOException, InterruptedException {
    startDockerContainer(launchCommand);
    runGfshCommandInContainer(
        "start server --name=server1 --server-port=40404 --J=-Dgemfire.jmx-manager=true --J=-Dgemfire.jmx-manager-start=true "
            + launchCommand);
  }

  @BeforeClass
  public static void setup() {
    restTemplate = new RestTemplate();
  }

  @Test
  public void testPulseStarts() {
    assertThat(
        restTemplate
            .getForEntity("http://localhost:" + getMappedPort(7070) + "/pulse/login.html",
                String.class)
            .getStatusCode())
                .isEqualTo(HttpStatus.OK);
  }
}
