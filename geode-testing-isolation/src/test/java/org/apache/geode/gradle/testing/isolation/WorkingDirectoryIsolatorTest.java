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
package org.apache.geode.gradle.testing.isolation;

import java.io.File;
import java.util.Arrays;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class WorkingDirectoryIsolatorTest {

  @Test
  public void updatesRelativeUnixPaths() {
    ProcessBuilder processBuilder = new ProcessBuilder();
    processBuilder.directory(new File(""));
    processBuilder.command("/bin/java", "-javaagent:../some/jacocoagent.jar=destfile=../jacoco/integrationTest.exec", "GradleWorkerMain");
    new WorkingDirectoryIsolator().accept(processBuilder);

    Assertions.assertEquals(Arrays.asList("/bin/java", "-javaagent:../../some/jacocoagent.jar=destfile=../../jacoco/integrationTest.exec", "GradleWorkerMain"), processBuilder.command());
  }

  @Test
  public void updatesRelativeWindowsPaths() {
    ProcessBuilder processBuilder = new ProcessBuilder();
    processBuilder.directory(new File(""));
    processBuilder.command("/bin/java", "-javaagent:..\\some\\jacocoagent.jar=destfile=..\\jacoco\\integrationTest.exec", "GradleWorkerMain");
    new WorkingDirectoryIsolator().accept(processBuilder);

    Assertions.assertEquals(Arrays.asList("/bin/java", "-javaagent:..\\..\\some\\jacocoagent.jar=destfile=..\\..\\jacoco\\integrationTest.exec", "GradleWorkerMain"), processBuilder.command());
  }

}
