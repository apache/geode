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

package org.apache.geode.test.junit.rules;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.dunit.standalone.VersionManager;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

@Category({UnitTest.class, GfshTest.class})
public class GfshRuleTest {

  @Rule
  public GfshRule gfsh130 = new GfshRule(VersionManager.GEODE_130);

  @Rule
  public GfshRule gfshDefault = new GfshRule();

  @Test
  public void checkGfshDefault() {
    assertThat(gfshDefault.getGfshPath().toString())
        .contains("geode-assembly/build/install/apache-geode/bin/gfsh");
  }

  @Test
  public void checkGfsh130() {
    assertThat(gfsh130.getGfshPath().toString())
        .contains("geode-test/build/apache-geode-1.3.0/bin/gfsh");
  }

}
