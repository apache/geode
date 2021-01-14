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

import org.junit.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.RestTemplate;


public class HttpServiceAcceptanceTest extends AbstractDockerizedAcceptanceTest {

  public HttpServiceAcceptanceTest(String launchCommand) throws IOException, InterruptedException {
    launch(launchCommand);
  }

  @Test
  public void httpServiceStarts() {
    // The two http ports set on the servers
    int[] httpPorts = new int[] {9090, 9091};
    for (int port : httpPorts) {
      int serverHttpPort = getMappedPort(port);
      RestTemplate restTemplate = new RestTemplate();
      HttpStatus statusCode = restTemplate
          .getForEntity("http://localhost:" + serverHttpPort + "/geode/v1/", String.class)
          .getStatusCode();
      assertThat(
          statusCode)
              .isEqualTo(HttpStatus.OK);
      assertThat(restTemplate
          .getForEntity("http://localhost:" + serverHttpPort + "/geode/docs/index.html",
              String.class)
          .getStatusCode())
              .isEqualTo(HttpStatus.OK);
      assertThat(restTemplate
          .getForEntity("http://localhost:" + serverHttpPort + "/geode/swagger-ui.html",
              String.class)
          .getStatusCode())
              .isEqualTo(HttpStatus.OK);
    }
  }
}
