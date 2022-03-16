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
package org.apache.geode.test.process;

import static java.util.Collections.shuffle;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.condition.JRE.JAVA_9;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledForJreRange;

class JavaModuleHelperTest {
  @Test
  void getJvmModuleOptions_returnsEmptyListIfNoAddOpensOrAddExportsOptions() throws IOException {
    // Options that are not module-related
    List<String> jvmOptions = Arrays.asList(
        "-Dsome.system.property=some.value",
        "-ea",
        "-XX:+HeapDumpOnOutOfMemoryError",
        "-Xmx512m");

    Process queryProcess = startQueryProcess(jvmOptions);
    List<String> queriedModuleOptions = stdOutputFrom(queryProcess);

    assertThat(queriedModuleOptions)
        .isEmpty();
  }

  @EnabledForJreRange(min = JAVA_9, disabledReason = "JRE does not recognize module options")
  @Test
  void getJvmModuleOptions_returnsAllAddOpensAndAddExportsOptionsInOrder() throws IOException {
    List<String> moduleOptions = Arrays.asList(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.text=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED",
        "--add-opens=java.base/javax.net.ssl=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.ssl=ALL-UNNAMED",
        "--add-opens=java.rmi/sun.rmi.transport=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-exports=java.base/sun.security.x509=ALL-UNNAMED",
        "--add-exports=java.base/sun.util.locale=ALL-UNNAMED",
        "--add-exports=java.management/com.sun.jmx.remote.security=ALL-UNNAMED");
    shuffle(moduleOptions);

    Process queryProcess = startQueryProcess(moduleOptions);
    List<String> queriedModuleOptions = stdOutputFrom(queryProcess);

    assertThat(queriedModuleOptions)
        .containsExactlyElementsOf(moduleOptions);
  }

  @EnabledForJreRange(min = JAVA_9, disabledReason = "JRE does not recognize module options")
  @Test
  void getJvmModuleOptions_doesNotIncludeOptionsOtherThanAddOpensAndAddExports()
      throws IOException {
    List<String> moduleOptions = Arrays.asList(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-exports=java.base/sun.security.x509=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.text=ALL-UNNAMED");

    // Add non-module options in between the module options
    List<String> jvmOptions = new ArrayList<>(moduleOptions);
    jvmOptions.add(4, "-Dsome.system.property=some.value");
    jvmOptions.add(3, "-ea");
    jvmOptions.add(2, "-XX:+HeapDumpOnOutOfMemoryError");
    jvmOptions.add(1, "-Xmx512m");

    Process queryProcess = startQueryProcess(jvmOptions);
    List<String> queriedModuleOptions = stdOutputFrom(queryProcess);

    assertThat(queriedModuleOptions)
        .containsExactlyElementsOf(moduleOptions);
  }

  private static List<String> stdOutputFrom(Process process)
      throws IOException {

    try (InputStream stdOut = process.getInputStream()) {
      return new BufferedReader(new InputStreamReader(stdOut))
          .lines()
          .collect(toList());
    }
  }

  // Starts a query process with the given options.
  private static Process startQueryProcess(List<String> options) throws IOException {
    String javaHome = System.getProperty("java.home");
    String javaBin = Paths.get(javaHome, "bin", "java").toString();
    String classpath = System.getProperty("java.class.path");
    List<String> commandLine = new ArrayList<>(Arrays.asList(javaBin, "-classpath", classpath));
    commandLine.addAll(options);
    commandLine.add(JavaModuleHelperTest.class.getName());

    return new ProcessBuilder(commandLine)
        .redirectErrorStream(true)
        .start();
  }

  // Queries its JVM's module options and writes each to stdout
  public static void main(String[] args) {
    JavaModuleHelper.getJvmModuleOptions()
        .forEach(System.out::println);
  }
}
