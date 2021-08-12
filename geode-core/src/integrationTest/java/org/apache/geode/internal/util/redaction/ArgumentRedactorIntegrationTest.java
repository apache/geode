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
package org.apache.geode.internal.util.redaction;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import org.apache.geode.internal.logging.Banner;

public class ArgumentRedactorIntegrationTest {

  private static final String someProperty = "redactorTest.someProperty";
  private static final String somePasswordProperty = "redactorTest.aPassword";
  private static final String someOtherPasswordProperty =
      "redactorTest.aPassword-withCharactersAfterward";

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Test
  public void systemPropertiesGetRedactedInBanner() {
    System.setProperty(someProperty, "isNotRedacted");
    System.setProperty(somePasswordProperty, "isRedacted");
    System.setProperty(someOtherPasswordProperty, "isRedacted");

    List<String> args = asList("--user=me", "--password=isRedacted",
        "--another-password-for-some-reason =isRedacted", "--yet-another-password = isRedacted",
        "--one-more-password isRedacted");

    String banner = new Banner().getString(args.toArray(new String[0]));

    assertThat(banner).doesNotContain("isRedacted");
  }
}
