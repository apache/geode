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
package org.apache.geode.internal;

import static org.apache.geode.internal.Assert.fail;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.CyclicBarrier;

@Category(IntegrationTest.class)
public class JarDeployerIntegrationTest {

  private ClassBuilder classBuilder;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setup() {
    System.setProperty("user.dir", temporaryFolder.getRoot().getAbsolutePath());
    classBuilder = new ClassBuilder();
    ClassPathLoader.setLatestToDefault();
  }

  @Test
  public void testDeployFileAndChange() throws Exception {
    final JarDeployer jarDeployer = new JarDeployer();

    // First deploy of the JAR file
    byte[] jarBytes = this.classBuilder.createJarFromName("ClassA");
    JarClassLoader jarClassLoader =
        jarDeployer.deploy(new String[] {"JarDeployerDUnit.jar"}, new byte[][] {jarBytes})[0];
    File deployedJar = new File(jarClassLoader.getFileCanonicalPath());

    assertThat(deployedJar).exists();
    assertThat(deployedJar.getName()).contains("#1");
    assertThat(deployedJar.getName()).doesNotContain("#2");

    assertThat(ClassPathLoader.getLatest().forName("ClassA")).isNotNull();

    assertThat(doesFileMatchBytes(deployedJar, jarBytes));

    // Now deploy an updated JAR file and make sure that the next version of the JAR file
    // was created and the first one was deleted.
    jarBytes = this.classBuilder.createJarFromName("ClassB");
    JarClassLoader newJarClassLoader =
        jarDeployer.deploy(new String[] {"JarDeployerDUnit.jar"}, new byte[][] {jarBytes})[0];
    File nextDeployedJar = new File(newJarClassLoader.getFileCanonicalPath());

    assertThat(nextDeployedJar.exists());
    assertThat(nextDeployedJar.getName()).contains("#2");
    assertThat(doesFileMatchBytes(nextDeployedJar, jarBytes));

    assertThat(ClassPathLoader.getLatest().forName("ClassB")).isNotNull();

    assertThatThrownBy(() -> ClassPathLoader.getLatest().forName("ClassA"))
        .isExactlyInstanceOf(ClassNotFoundException.class);

    assertThat(jarDeployer.findSortedOldVersionsOfJar("JarDeployerDUnit.jar")).hasSize(1);
    assertThat(jarDeployer.findDistinctDeployedJars()).hasSize(1);
  }

  @Test
  public void testDeployNoUpdateWhenNoChange() throws Exception {
    final JarDeployer jarDeployer = new JarDeployer();

    // First deploy of the JAR file
    byte[] jarBytes = this.classBuilder.createJarFromName("JarDeployerDUnitDNUWNC");
    JarClassLoader jarClassLoader =
        jarDeployer.deploy(new String[] {"JarDeployerDUnit2.jar"}, new byte[][] {jarBytes})[0];
    File deployedJar = new File(jarClassLoader.getFileCanonicalPath());

    assertThat(deployedJar).exists();
    assertThat(deployedJar.getName()).contains("#1");
    JarClassLoader newJarClassLoader =
        jarDeployer.deploy(new String[] {"JarDeployerDUnit2.jar"}, new byte[][] {jarBytes})[0];
    assertThat(newJarClassLoader).isNull();
  }

  @Test
  public void testDeployToInvalidDirectory() throws IOException, ClassNotFoundException {
    final File alternateDir = new File(temporaryFolder.getRoot(), "JarDeployerDUnit");
    alternateDir.delete();

    final JarDeployer jarDeployer = new JarDeployer(alternateDir);
    final CyclicBarrier barrier = new CyclicBarrier(2);
    final byte[] jarBytes = this.classBuilder.createJarFromName("JarDeployerDUnitDTID");

    // Test to verify that deployment fails if the directory doesn't exist.
    assertThatThrownBy(() -> {
      jarDeployer.deploy(new String[] {"JarDeployerDUnit.jar"}, new byte[][] {jarBytes});
    }).isInstanceOf(IOException.class);

    // Test to verify that deployment succeeds if the directory doesn't
    // initially exist, but is then created while the JarDeployer is looping
    // looking for a valid directory.
    Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          barrier.await();
        } catch (Exception e) {
          fail(e);
        }

        try {
          jarDeployer.deploy(new String[] {"JarDeployerDUnit.jar"}, new byte[][] {jarBytes});
        } catch (Exception e) {
          fail(e);
        }
      }
    };
    thread.start();

    try {
      barrier.await();
      Thread.sleep(500);
      alternateDir.mkdir();
      thread.join();
    } catch (Exception e) {
      fail(e);
    }
  }

  @Test
  public void testVersionNumberCreation() throws Exception {
    JarDeployer jarDeployer = new JarDeployer();

    File versionedName = jarDeployer.getNextVersionJarFile("myJar.jar");
    assertThat(versionedName.getName()).isEqualTo(JarDeployer.JAR_PREFIX + "myJar.jar" + "#1");

    byte[] jarBytes = this.classBuilder.createJarFromName("ClassA");
    JarClassLoader jarClassLoader =
        jarDeployer.deploy(new String[] {"myJar.jar"}, new byte[][] {jarBytes})[0];
    File deployedJar = new File(jarClassLoader.getFileCanonicalPath());

    assertThat(deployedJar.getName()).isEqualTo(JarDeployer.JAR_PREFIX + "myJar.jar" + "#1");
    assertThat(jarDeployer.getNextVersionJarFile(deployedJar.getName()).getName())
        .isEqualTo(JarDeployer.JAR_PREFIX + "myJar.jar" + "#2");

  }

  @Test
  public void testVersionNumberMatcher() throws Exception {
    JarDeployer jarDeployer = new JarDeployer();
    int version = jarDeployer.extractVersionFromFilename(
        temporaryFolder.newFile(JarDeployer.JAR_PREFIX + "MyJar.jar" + "#1"));

    assertThat(version).isEqualTo(1);
  }

  private boolean doesFileMatchBytes(final File file, final byte[] bytes) throws IOException {
    return bytes == Files.readAllBytes(file.toPath());
  }

}
