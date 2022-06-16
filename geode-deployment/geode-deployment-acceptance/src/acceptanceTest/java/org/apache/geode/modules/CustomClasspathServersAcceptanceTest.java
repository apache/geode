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

import java.io.File;
import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.geode.test.compiler.JarBuilder;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

public class CustomClasspathServersAcceptanceTest extends AbstractDockerizedAcceptanceTest {
  public CustomClasspathServersAcceptanceTest(String launchCommand)
      throws IOException, InterruptedException {
    launch(launchCommand);
  }

  private static File myJar1;
  private static File myJar2;

  @BeforeClass
  public static void setup() throws IOException {
    JarBuilder jarBuilder = new JarBuilder();
    File stagingDir = stagingTempDir.newFolder("staging");
    myJar1 = new File(stagingDir, "myJar-1.0.jar");
    myJar2 = new File(stagingDir, "myJar-2.0.jar");
    jarBuilder.buildJarFromClassNames(myJar1, "SomeClass");
    jarBuilder.buildJarFromClassNames(myJar2, "SomeClass2");
  }

  @Override
  protected String getServer1SpecificGfshCommands() {
    return "--classpath=" + myJar1.getAbsolutePath();
  }

  @Override
  protected String getServer2SpecificGfshCommands() {
    return "--classpath=" + myJar2.getAbsolutePath();
  }

  @Test
  public void testServersStarted() {
    assertThat(GfshScript.of(getLocatorGFSHConnectionString(),
        "list members").execute(gfshRule).getOutputText()).contains("server1", "server2");
  }
}
