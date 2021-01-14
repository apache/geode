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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.test.junit.rules.gfsh.GfshScript;

public class IndexAcceptanceTest extends AbstractDockerizedAcceptanceTest {

  public IndexAcceptanceTest(String launchCommand) throws IOException, InterruptedException {
    launch(launchCommand);
  }

  @Before
  public void setup() {
    GfshScript.of(getLocatorGFSHConnectionString(),
        "create region --name=TestRegion --type=PARTITION",
        "create index --name=firstNameIndex --region=/TestRegion --expression=firstName")
        .execute(gfshRule);
  }

  @After
  public void teardown() {
    GfshScript.of(getLocatorGFSHConnectionString(), "destroy index --name=firstNameIndex",
        "destroy region --name=TestRegion").execute(gfshRule);
  }

  @Test
  public void testIndexCreated() {
    assertThat(GfshScript.of(getLocatorGFSHConnectionString(), "list indexes")
        .execute(gfshRule).getOutputText()).contains("firstNameIndex")
            .doesNotContain("No Indexes Found");
  }
}
