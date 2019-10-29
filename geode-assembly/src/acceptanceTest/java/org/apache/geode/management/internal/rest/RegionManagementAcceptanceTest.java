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
package org.apache.geode.management.internal.rest;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.junit.rules.GeodeDevRestClient;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

public class RegionManagementAcceptanceTest {

  @Rule
  public GfshRule gfsh = new GfshRule();

  @Test
  public void sanityCheck() throws Exception {
    GfshScript.of("start locator --port=0").execute(gfsh);

    // verify the management rest api is started correctly
    GeodeDevRestClient client =
        new GeodeDevRestClient("/management/v1", "localhost", 7070, false);

    client.doGetAndAssert("/ping").hasStatusCode(200).hasResponseBody().isEqualTo("pong");
  }
}
