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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

/**
 * Unit tests for the JarDeployer class
 * 
 * @since GemFire 7.0
 */
@Category(DistributedTest.class)
@SuppressWarnings("serial")
public class JarDeployerDUnitTest extends JUnit4CacheTestCase {

  static FileLock savedFileLock = null;
  private final ClassBuilder classBuilder = new ClassBuilder();

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    JarDeployer jarDeployer = new JarDeployer();
    for (JarClassLoader jarClassLoader : jarDeployer.findJarClassLoaders()) {
      if (jarClassLoader.getJarName().startsWith("JarDeployerDUnit")) {
        jarDeployer.undeploy(jarClassLoader.getJarName());
      }
    }
    for (String functionName : FunctionService.getRegisteredFunctions().keySet()) {
      if (functionName.startsWith("JarDeployerDUnit")) {
        FunctionService.unregisterFunction(functionName);
      }
    }
    disconnectAllFromDS();
    deleteSavedJarFiles();
  }



  @Test
  public void testDeployExclusiveLock() throws IOException, ClassNotFoundException {
    final JarDeployer jarDeployer = new JarDeployer();
    final File currentDir = new File(".").getAbsoluteFile();
    final VM vm = Host.getHost(0).getVM(0);

    // Deploy the Class JAR file
    final File jarFile1 = jarDeployer.getNextVersionJarFile("JarDeployerDUnit.jar");
    byte[] jarBytes = this.classBuilder.createJarFromName("JarDeployerDUnitDELA");
    jarDeployer.deploy(new String[] {"JarDeployerDUnit.jar"}, new byte[][] {jarBytes});

    try {
      ClassPathLoader.getLatest().forName("JarDeployerDUnitDELA");
    } catch (ClassNotFoundException cnfex) {
      fail("JAR file not correctly added to Classpath");
    }

    assertNotNull(ClassPathLoader.getLatest().getResource("JarDeployerDUnitDELA.class"));

    // Attempt to acquire an exclusive lock in the other VM
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        FileOutputStream outStream = null;
        FileLock fileLock = null;

        try {
          outStream = new FileOutputStream(jarFile1, true);
          fileLock = outStream.getChannel().tryLock(0, 1, false);
          if (fileLock != null) {
            fail("Should not have been able to obtain exclusive lock on file:"
                + jarFile1.getAbsolutePath());
          }
        } catch (FileNotFoundException fnfex) {
          Assert.fail("JAR file not found where expected", fnfex);
        } catch (IOException ioex) {
          Assert.fail("IOException when trying to obtain exclusive lock", ioex);
        } finally {
          if (outStream != null) {
            try {
              outStream.close();
            } catch (IOException ioex) {
              fail("Could not close lock file output stream");
            }
          }
          if (fileLock != null) {
            try {
              fileLock.channel().close();
            } catch (IOException ioex) {
              fail("Could not close lock file channel");
            }
          }
        }
      }
    });
  }

  @Test
  public void testDeploySharedLock() throws IOException, ClassNotFoundException {
    final JarDeployer jarDeployer = new JarDeployer();
    final File currentDir = new File(".").getAbsoluteFile();
    final VM vm = Host.getHost(0).getVM(0);

    // Deploy the JAR file
    final File jarFile1 = jarDeployer.getNextVersionJarFile("JarDeployerDUnit.jar");
    byte[] jarBytes = this.classBuilder.createJarFromName("JarDeployerDUnitDSLA");
    jarDeployer.deploy(new String[] {"JarDeployerDUnit.jar"}, new byte[][] {jarBytes});

    try {
      ClassPathLoader.getLatest().forName("JarDeployerDUnitDSLA");
    } catch (ClassNotFoundException cnfex) {
      fail("JAR file not correctly added to Classpath");
    }

    // Acquire a shared lock in the other VM
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        if (!jarFile1.exists()) {
          fail("JAR file not found where expected: " + jarFile1.getName());
        }
        try {
          JarDeployerDUnitTest.savedFileLock = acquireSharedLock(jarFile1);
        } catch (IOException ioex) {
          fail("Unable to acquire the shared file lock");
        }
      }
    });

    // Now update the JAR file and make sure the first one isn't deleted
    jarBytes = this.classBuilder.createJarFromName("JarDeployerDUnitDSLB");
    jarDeployer.deploy(new String[] {"JarDeployerDUnit.jar"}, new byte[][] {jarBytes});

    try {
      ClassPathLoader.getLatest().forName("JarDeployerDUnitDSLB");
    } catch (ClassNotFoundException cnfex) {
      fail("JAR file not correctly added to Classpath");
    }

    try {
      ClassPathLoader.getLatest().forName("JarDeployerDUnitC");
      fail("Class should not be found on Classpath: JarDeployerDUniDSLA");
    } catch (ClassNotFoundException expected) { // expected
    }

    if (!jarFile1.exists()) {
      fail("JAR file should not have been deleted: " + jarFile1.getName());
    }

    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        try {
          releaseLock(JarDeployerDUnitTest.savedFileLock, jarFile1);
        } catch (IOException ioex) {
          fail("Unable to release the shared file lock");
        }
      }
    });
  }

  @Test
  public void testUndeploySharedLock() throws IOException, ClassNotFoundException {
    final JarDeployer jarDeployer = new JarDeployer();
    final File currentDir = new File(".").getAbsoluteFile();
    final VM vm = Host.getHost(0).getVM(0);

    // Deploy the JAR file
    final File jarFile1 = jarDeployer.getNextVersionJarFile("JarDeployerDUnit.jar");
    byte[] jarBytes = this.classBuilder.createJarFromName("JarDeployerDUnitUSL");
    jarDeployer.deploy(new String[] {"JarDeployerDUnit.jar"}, new byte[][] {jarBytes});

    try {
      ClassPathLoader.getLatest().forName("JarDeployerDUnitUSL");
    } catch (ClassNotFoundException cnfex) {
      fail("JAR file not correctly added to Classpath");
    }

    // Acquire a shared lock in the other VM
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        if (!jarFile1.exists()) {
          fail("JAR file not found where expected: " + jarFile1.getName());
        }
        try {
          JarDeployerDUnitTest.savedFileLock = acquireSharedLock(jarFile1);
        } catch (IOException ioex) {
          fail("Unable to acquire the shared file lock");
        }
      }
    });

    // Now undeploy the JAR file and make sure the first one isn't deleted
    jarDeployer.undeploy("JarDeployerDUnit.jar");

    if (!jarFile1.exists()) {
      fail("JAR file should not have been deleted: " + jarFile1.getName());
    }

    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        try {
          releaseLock(JarDeployerDUnitTest.savedFileLock, jarFile1);
        } catch (IOException ioex) {
          fail("Unable to release the shared file lock");
        }
      }
    });
  }

  @Test
  public void testDeployUpdateByAnotherVM() throws IOException, ClassNotFoundException {
    final JarDeployer jarDeployer = new JarDeployer();
    final File currentDir = new File(".").getAbsoluteFile();
    final VM vm = Host.getHost(0).getVM(0);

    final File jarFile1 = jarDeployer.getNextVersionJarFile("JarDeployerDUnit.jar");
    byte[] jarBytes = this.classBuilder.createJarFromName("JarDeployerDUnitDUBAVMA");
    jarDeployer.deploy(new String[] {"JarDeployerDUnit.jar"}, new byte[][] {jarBytes});

    try {
      ClassPathLoader.getLatest().forName("JarDeployerDUnitDUBAVMA");
    } catch (ClassNotFoundException cnfex) {
      fail("JAR file not correctly added to Classpath");
    }

    final File jarFile2 = jarDeployer.getNextVersionJarFile(jarFile1.getName());
    final byte[] vmJarBytes = this.classBuilder.createJarFromName("JarDeployerDUnitDUBAVMB");
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        if (!jarFile1.exists()) {
          fail("JAR file not found where expected: " + jarFile1.getName());
        }

        // The other VM writes out a newer version of the JAR file.
        try {
          writeJarBytesToFile(jarFile2, vmJarBytes);
        } catch (IOException ioex) {
          fail("Could not write JAR File");
        }
      }
    });

    // This VM is told to deploy the same JAR file.
    jarDeployer.deploy(new String[] {"JarDeployerDUnit.jar"}, new byte[][] {vmJarBytes});

    try {
      ClassPathLoader.getLatest().forName("JarDeployerDUnitDUBAVMB");
    } catch (ClassNotFoundException cnfex) {
      fail("JAR file not correctly added to Classpath");
    }

    try {
      ClassPathLoader.getLatest().forName("JarDeployerDUnitDUBAVMA");
      fail("Class should not be found on Classpath: JarDeployerDUnitDUBAVMA");
    } catch (ClassNotFoundException expected) { // expected
    }

    if (!jarFile2.exists()) {
      fail("JAR file should not have been deleted: " + jarFile2.getName());
    }

    // Make sure the second deploy didn't create a 3rd version of the JAR file.
    final File jarFile3 = jarDeployer.getNextVersionJarFile(jarFile2.getName());
    if (jarFile3.exists()) {
      fail("JAR file should not have been created: " + jarFile3.getName());
    }
  }

  @Test
  public void testLoadPreviouslyDeployedJars() throws IOException {
    final File parentJarFile = new File(JarDeployer.JAR_PREFIX + "JarDeployerDUnitAParent.jar#1");
    final File usesJarFile = new File(JarDeployer.JAR_PREFIX + "JarDeployerDUnitUses.jar#1");
    final File functionJarFile =
        new File(JarDeployer.JAR_PREFIX + "JarDeployerDUnitFunction.jar#1");

    // Write out a JAR files.
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("package jddunit.parent;");
    stringBuffer.append("public class JarDeployerDUnitParent {");
    stringBuffer.append("public String getValueParent() {");
    stringBuffer.append("return \"PARENT\";}}");

    byte[] jarBytes = this.classBuilder.createJarFromClassContent(
        "jddunit/parent/JarDeployerDUnitParent", stringBuffer.toString());
    FileOutputStream outStream = new FileOutputStream(parentJarFile);
    outStream.write(jarBytes);
    outStream.close();

    stringBuffer = new StringBuffer();
    stringBuffer.append("package jddunit.uses;");
    stringBuffer.append("public class JarDeployerDUnitUses {");
    stringBuffer.append("public String getValueUses() {");
    stringBuffer.append("return \"USES\";}}");

    jarBytes = this.classBuilder.createJarFromClassContent("jddunit/uses/JarDeployerDUnitUses",
        stringBuffer.toString());
    outStream = new FileOutputStream(usesJarFile);
    outStream.write(jarBytes);
    outStream.close();

    stringBuffer = new StringBuffer();
    stringBuffer.append("package jddunit.function;");
    stringBuffer.append("import jddunit.parent.JarDeployerDUnitParent;");
    stringBuffer.append("import jddunit.uses.JarDeployerDUnitUses;");
    stringBuffer.append("import org.apache.geode.cache.execute.Function;");
    stringBuffer.append("import org.apache.geode.cache.execute.FunctionContext;");
    stringBuffer.append(
        "public class JarDeployerDUnitFunction  extends JarDeployerDUnitParent implements Function {");
    stringBuffer.append("private JarDeployerDUnitUses uses = new JarDeployerDUnitUses();");
    stringBuffer.append("public boolean hasResult() {return true;}");
    stringBuffer.append(
        "public void execute(FunctionContext context) {context.getResultSender().lastResult(getValueParent() + \":\" + uses.getValueUses());}");
    stringBuffer.append("public String getId() {return \"JarDeployerDUnitFunction\";}");
    stringBuffer.append("public boolean optimizeForWrite() {return false;}");
    stringBuffer.append("public boolean isHA() {return false;}}");

    ClassBuilder functionClassBuilder = new ClassBuilder();
    functionClassBuilder.addToClassPath(parentJarFile.getAbsolutePath());
    functionClassBuilder.addToClassPath(usesJarFile.getAbsolutePath());
    jarBytes = functionClassBuilder.createJarFromClassContent(
        "jddunit/function/JarDeployerDUnitFunction", stringBuffer.toString());
    outStream = new FileOutputStream(functionJarFile);
    outStream.write(jarBytes);
    outStream.close();

    // Start the distributed system and check to see if the function executes correctly
    DistributedSystem distributedSystem = getSystem();
    getCache();

    Execution execution =
        FunctionService.onMember(distributedSystem, distributedSystem.getDistributedMember());
    ResultCollector resultCollector = execution.execute("JarDeployerDUnitFunction");
    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) resultCollector.getResult();
    assertEquals("PARENT:USES", result.get(0));
  }

  @Test
  public void testDeployToAlternateDirectory() throws IOException, ClassNotFoundException {
    final File alternateDir = new File("JarDeployerDUnit");
    alternateDir.mkdir();

    // Add the alternate directory to the distributed system, get it back out, and then create
    // a JarDeployer object with it.
    Properties properties = new Properties();
    properties.put(ConfigurationProperties.DEPLOY_WORKING_DIR, alternateDir.getAbsolutePath());
    InternalDistributedSystem distributedSystem = getSystem(properties);
    final JarDeployer jarDeployer =
        new JarDeployer(distributedSystem.getConfig().getDeployWorkingDir());

    File jarFile = jarDeployer.getNextVersionJarFile("JarDeployerDUnit.jar");
    byte[] jarBytes = this.classBuilder.createJarFromName("JarDeployerDUnitDTAC");
    jarDeployer.deploy(new String[] {"JarDeployerDUnit.jar"}, new byte[][] {jarBytes});

    try {
      ClassPathLoader.getLatest().forName("JarDeployerDUnitDTAC");
    } catch (ClassNotFoundException cnfex) {
      fail("JAR file not correctly added to Classpath");
    }

    if (!jarFile.exists()) {
      fail("JAR file not found where expected: " + jarFile.getName());
    }

    if (!doesFileMatchBytes(jarFile, jarBytes)) {
      fail("Contents of JAR file do not match those provided: " + jarFile.getName());
    }
  }



  @Test
  public void testSuspendAndResume() throws IOException, ClassNotFoundException {
    AtomicReference<Boolean> okayToResume = new AtomicReference<>(false);

    final JarDeployer jarDeployer = new JarDeployer();
    byte[] jarBytes = this.classBuilder.createJarFromName("JarDeployerDUnitSAR");
    final JarDeployer suspendingJarDeployer = new JarDeployer();
    final CountDownLatch latch = new CountDownLatch(1);

    Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          suspendingJarDeployer.suspendAll();
          latch.countDown();
          Thread.sleep(3000);
        } catch (InterruptedException iex) {
          // It doesn't matter, just fail the test
        }
        okayToResume.set(true);
        suspendingJarDeployer.resumeAll();
      }
    };
    thread.start();

    try {
      latch.await();
    } catch (InterruptedException iex) {
      // It doesn't matter, just fail the test
    }
    jarDeployer.deploy(new String[] {"JarDeployerDUnit.jar"}, new byte[][] {jarBytes});
    if (!okayToResume.get()) {
      fail("JarDeployer did not suspend as expected");
    }
  }


  @Test
  public void testZeroLengthFile() throws IOException, ClassNotFoundException {
    final JarDeployer jarDeployer = new JarDeployer();

    try {
      jarDeployer.deploy(new String[] {"JarDeployerDUnitZLF.jar"}, new byte[][] {new byte[0]});
      fail("Zero length files are not deployable");
    } catch (IllegalArgumentException expected) {
      // Expected
    }

    try {
      jarDeployer.deploy(new String[] {"JarDeployerDUnitZLF1.jar", "JarDeployerDUnitZLF2.jar"},
          new byte[][] {this.classBuilder.createJarFromName("JarDeployerDUnitZLF1"), new byte[0]});
      fail("Zero length files are not deployable");
    } catch (IllegalArgumentException expected) {
      // Expected
    }
  }

  @Test
  public void testInvalidJarFile() throws IOException, ClassNotFoundException {
    final JarDeployer jarDeployer = new JarDeployer();

    try {
      jarDeployer.deploy(new String[] {"JarDeployerDUnitIJF.jar"},
          new byte[][] {"INVALID JAR CONTENT".getBytes()});
      fail("Non-JAR files are not deployable");
    } catch (IllegalArgumentException expected) {
      // Expected
    }

    try {
      jarDeployer.deploy(new String[] {"JarDeployerDUnitIJF1.jar", "JarDeployerDUnitIJF2.jar"},
          new byte[][] {this.classBuilder.createJarFromName("JarDeployerDUnitIJF1"),
              "INVALID JAR CONTENT".getBytes()});
      fail("Non-JAR files are not deployable");
    } catch (IllegalArgumentException expected) {
      // Expected
    }

    final VM vm = Host.getHost(0).getVM(1);
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        File invalidFile = new File(JarDeployer.JAR_PREFIX + "JarDeployerDUnitIJF.jar#3");
        try {
          RandomAccessFile randomAccessFile = new RandomAccessFile(invalidFile, "rw");
          randomAccessFile.write("GARBAGE".getBytes(), 0, 7);
          randomAccessFile.close();
        } catch (IOException ioex) {
          Assert.fail("Error trying to create garbage file for test", ioex);
        }

        getSystem();
        getCache();

        if (invalidFile.exists()) {
          fail("Invalid JAR file should have been deleted at startup");
        }
      }
    });
  }

  FileLock acquireSharedLock(final File file) throws IOException {
    return new FileInputStream(file).getChannel().lock(0, 1, true);
  }

  void releaseLock(final FileLock fileLock, final File lockFile) throws IOException {
    if (lockFile == null) {
      return;
    }

    try {
      if (fileLock != null) {
        fileLock.release();
        fileLock.channel().close();
      }
    } finally {
      if (!lockFile.delete()) {
        lockFile.deleteOnExit();
      }
    }
  }

  protected boolean doesFileMatchBytes(final File file, final byte[] bytes) throws IOException {
    // If the don't have the same number of bytes then nothing to do
    if (file.length() != bytes.length) {
      return false;
    }

    // Open the file then loop comparing each byte
    int index = 0;
    try (InputStream inStream = new FileInputStream(file)) {
      for (; index < bytes.length; index++) {
        if (((byte) inStream.read()) != bytes[index])
          break;
      }
    }

    // If we didn't get to the end then something was different
    return index >= bytes.length;
  }

  private void deleteSavedJarFiles() throws IOException {
    Pattern pattern = Pattern.compile("^" + JarDeployer.JAR_PREFIX + "JarDeployerDUnit.*#\\d++$");
    File[] files = new File(".").listFiles((dir1, name) -> pattern.matcher(name).matches());
    if (files != null) {
      for (File file : files) {
        Files.delete(file.toPath());
      }
    }
  }

  void writeJarBytesToFile(File jarFile, byte[] jarBytes) throws IOException {
    final OutputStream outStream = new FileOutputStream(jarFile);
    outStream.write(jarBytes);
    outStream.close();
  }
}
