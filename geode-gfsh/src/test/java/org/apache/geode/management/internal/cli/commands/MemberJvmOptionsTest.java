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
 *
 */

package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.management.internal.cli.commands.MemberJvmOptions.getMemberJvmOptions;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledForJreRange;
import org.junit.jupiter.api.condition.EnabledOnJre;
import org.junit.jupiter.api.condition.JRE;

class MemberJvmOptionsTest {
  @Test
  @EnabledOnJre(JRE.JAVA_8)
  void java8Options() {
    assertThat(getMemberJvmOptions())
        .isEmpty();
  }

  @Test
  @EnabledForJreRange(min = JRE.JAVA_11)
  void java11Options() {
    List<String> expectedOptions = Arrays.asList(
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-exports=java.management/com.sun.jmx.remote.security=ALL-UNNAMED");

    assertThat(getMemberJvmOptions())
        .containsExactlyElementsOf(expectedOptions);
  }
}
