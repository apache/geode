/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Unit tests for the JarDeployer class
 * 
 * @author David Hoots
 * @since 7.0
 */
public class JarDeployerDUnitTest extends CacheTestCase {
  private static final long serialVersionUID = 1L;
  static FileLock savedFileLock = null;
  private final ClassBuilder classBuilder = new ClassBuilder();

  public JarDeployerDUnitTest(String name) {
    super(name);
  }

  @Override
  public void tearDown2() throws Exception {
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
    super.tearDown2();
  }
  
  @Test
  public void testDeployFileAndChange() throws IOException, ClassNotFoundException {
    final JarDeployer jarDeployer = new JarDeployer();
    final File currentDir = new File(".").getAbsoluteFile();

    // First deploy of the JAR file
    File jarFile = getFirstVersionForTest(currentDir, "JarDeployerDUnit.jar");
    byte[] jarBytes = this.classBuilder.createJarFromName("JarDeployerDUnitDFACA");
    jarDeployer.deploy(new String[] { "JarDeployerDUnit.jar" }, new byte[][] { jarBytes });

    try {
      ClassPathLoader.getLatest().forName("JarDeployerDUnitDFACA");
    } catch (ClassNotFoundException cnfex) {
      fail("JAR file not correctly added to Classpath");
    }

    if (!jarFile.exists()) {
      fail("JAR file not found where expected: " + jarFile.getName());
    }

    if (!doesFileMatchBytes(jarFile, jarBytes)) {
      fail("Contents of JAR file do not match those provided: " + jarFile.getName());
    }

    // Now deploy an updated JAR file and make sure that the next version of the JAR file
    // was created and the first one was deleted.
    jarFile = getNextVersionJarFile(jarFile);
    jarBytes = this.classBuilder.createJarFromName("JarDeployerDUnitDFACB");
    jarDeployer.deploy(new String[] { "JarDeployerDUnit.jar" }, new byte[][] { jarBytes });

    if (!jarFile.exists()) {
      fail("JAR file not found where expected: " + jarFile.getName());
    }

    if (!doesFileMatchBytes(jarFile, jarBytes)) {
      fail("Contents of JAR file do not match those provided: " + jarFile.getName());
    }

    try {
      ClassPathLoader.getLatest().forName("JarDeployerDUnitDFACB");
    } catch (ClassNotFoundException cnfex) {
      fail("JAR file not correctly added to Classpath");
    }

    try {
      ClassPathLoader.getLatest().forName("JarDeployerDUnitDFACA");
      fail("Class should not be found on Classpath: JarDeployerDUnitDFACA");
    } catch (ClassNotFoundException expected) { // expected
    }
  }

  @Test
  public void testDeployNoUpdateWhenNoChange() throws IOException, ClassNotFoundException {
    final JarDeployer jarDeployer = new JarDeployer();
    final File currentDir = new File(".").getAbsoluteFile();

    // First deploy of the JAR file
    File jarFile = getFirstVersionForTest(currentDir, "JarDeployerDUnit.jar");
    byte[] jarBytes = this.classBuilder.createJarFromName("JarDeployerDUnitDNUWNC");
    jarDeployer.deploy(new String[] { "JarDeployerDUnit.jar" }, new byte[][] { jarBytes });

    if (!jarFile.exists()) {
      fail("JAR file not found where expected: " + jarFile.getName());
    }

    // Now deploy the same JAR file
    jarDeployer.deploy(new String[] { "JarDeployerDUnit.jar" }, new byte[][] { jarBytes });

    if (!jarFile.exists()) {
      fail("JAR file not found where expected: " + jarFile.getName());
    }

    jarFile = getNextVersionJarFile(jarFile);
    if (jarFile.exists()) {
      fail("JAR file should not have been created: " + jarFile.getName());
    }
  }

  @Test
  @SuppressWarnings("serial")
  public void testDeployExclusiveLock() throws IOException, ClassNotFoundException {
    final JarDeployer jarDeployer = new JarDeployer();
    final File currentDir = new File(".").getAbsoluteFile();
    final VM vm = Host.getHost(0).getVM(0);

    // Deploy the Class JAR file
    final File jarFile1 = getFirstVersionForTest(currentDir, "JarDeployerDUnit.jar");
    byte[] jarBytes = this.classBuilder.createJarFromName("JarDeployerDUnitDELA");
    jarDeployer.deploy(new String[] { "JarDeployerDUnit.jar" }, new byte[][] { jarBytes });

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
            fail("Should not have been able to obtain exclusive lock on file:" + jarFile1.getAbsolutePath());
          }
        } catch (FileNotFoundException fnfex) {
          fail("JAR file not found where expected", fnfex);
        } catch (IOException ioex) {
          fail("IOException when trying to obtain exclusive lock", ioex);
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
  @SuppressWarnings("serial")
  public void testDeploySharedLock() throws IOException, ClassNotFoundException {
    final JarDeployer jarDeployer = new JarDeployer();
    final File currentDir = new File(".").getAbsoluteFile();
    final VM vm = Host.getHost(0).getVM(0);

    // Deploy the JAR file
    final File jarFile1 = getFirstVersionForTest(currentDir, "JarDeployerDUnit.jar");
    byte[] jarBytes = this.classBuilder.createJarFromName("JarDeployerDUnitDSLA");
    jarDeployer.deploy(new String[] { "JarDeployerDUnit.jar" }, new byte[][] { jarBytes });

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
    jarDeployer.deploy(new String[] { "JarDeployerDUnit.jar" }, new byte[][] { jarBytes });

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
  @SuppressWarnings("serial")
  public void testUndeploySharedLock() throws IOException, ClassNotFoundException {
    final JarDeployer jarDeployer = new JarDeployer();
    final File currentDir = new File(".").getAbsoluteFile();
    final VM vm = Host.getHost(0).getVM(0);

    // Deploy the JAR file
    final File jarFile1 = getFirstVersionForTest(currentDir, "JarDeployerDUnit.jar");
    byte[] jarBytes = this.classBuilder.createJarFromName("JarDeployerDUnitUSL");
    jarDeployer.deploy(new String[] { "JarDeployerDUnit.jar" }, new byte[][] { jarBytes });

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
  @SuppressWarnings("serial")
  public void testDeployUpdateByAnotherVM() throws IOException, ClassNotFoundException {
    final JarDeployer jarDeployer = new JarDeployer();
    final File currentDir = new File(".").getAbsoluteFile();
    final VM vm = Host.getHost(0).getVM(0);

    final File jarFile1 = getFirstVersionForTest(currentDir, "JarDeployerDUnit.jar");
    byte[] jarBytes = this.classBuilder.createJarFromName("JarDeployerDUnitDUBAVMA");
    jarDeployer.deploy(new String[] { "JarDeployerDUnit.jar" }, new byte[][] { jarBytes });

    try {
      ClassPathLoader.getLatest().forName("JarDeployerDUnitDUBAVMA");
    } catch (ClassNotFoundException cnfex) {
      fail("JAR file not correctly added to Classpath");
    }

    final File jarFile2 = getNextVersionJarFile(jarFile1);
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
    jarDeployer.deploy(new String[] { "JarDeployerDUnit.jar" }, new byte[][] { vmJarBytes });

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
    final File jarFile3 = getNextVersionJarFile(jarFile2);
    if (jarFile3.exists()) {
      fail("JAR file should not have been created: " + jarFile3.getName());
    }
  }

  @Test
  public void testLoadPreviouslyDeployedJars() throws IOException {
    final File parentJarFile = new File(JarDeployer.JAR_PREFIX + "JarDeployerDUnitAParent.jar#1");
    final File usesJarFile = new File(JarDeployer.JAR_PREFIX + "JarDeployerDUnitUses.jar#1");
    final File functionJarFile = new File(JarDeployer.JAR_PREFIX + "JarDeployerDUnitFunction.jar#1");

    // Write out a JAR files.
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("package jddunit.parent;");
    stringBuffer.append("public class JarDeployerDUnitParent {");
    stringBuffer.append("public String getValueParent() {");
    stringBuffer.append("return \"PARENT\";}}");

    byte[] jarBytes = this.classBuilder.createJarFromClassContent("jddunit/parent/JarDeployerDUnitParent", stringBuffer.toString());
    FileOutputStream outStream = new FileOutputStream(parentJarFile);
    outStream.write(jarBytes);
    outStream.close();

    stringBuffer = new StringBuffer();
    stringBuffer.append("package jddunit.uses;");
    stringBuffer.append("public class JarDeployerDUnitUses {");
    stringBuffer.append("public String getValueUses() {");
    stringBuffer.append("return \"USES\";}}");

    jarBytes = this.classBuilder.createJarFromClassContent("jddunit/uses/JarDeployerDUnitUses", stringBuffer.toString());
    outStream = new FileOutputStream(usesJarFile);
    outStream.write(jarBytes);
    outStream.close();

    stringBuffer = new StringBuffer();
    stringBuffer.append("package jddunit.function;");
    stringBuffer.append("import jddunit.parent.JarDeployerDUnitParent;");
    stringBuffer.append("import jddunit.uses.JarDeployerDUnitUses;");
    stringBuffer.append("import com.gemstone.gemfire.cache.execute.Function;");
    stringBuffer.append("import com.gemstone.gemfire.cache.execute.FunctionContext;");
    stringBuffer.append("public class JarDeployerDUnitFunction  extends JarDeployerDUnitParent implements Function {");
    stringBuffer.append("private JarDeployerDUnitUses uses = new JarDeployerDUnitUses();");
    stringBuffer.append("public boolean hasResult() {return true;}");
    stringBuffer
        .append("public void execute(FunctionContext context) {context.getResultSender().lastResult(getValueParent() + \":\" + uses.getValueUses());}");
    stringBuffer.append("public String getId() {return \"JarDeployerDUnitFunction\";}");
    stringBuffer.append("public boolean optimizeForWrite() {return false;}");
    stringBuffer.append("public boolean isHA() {return false;}}");

    ClassBuilder functionClassBuilder = new ClassBuilder();
    functionClassBuilder.addToClassPath(parentJarFile.getAbsolutePath());
    functionClassBuilder.addToClassPath(usesJarFile.getAbsolutePath());
    jarBytes = functionClassBuilder.createJarFromClassContent("jddunit/function/JarDeployerDUnitFunction", stringBuffer.toString());
    outStream = new FileOutputStream(functionJarFile);
    outStream.write(jarBytes);
    outStream.close();
    
    // Start the distributed system and check to see if the function executes correctly
    DistributedSystem distributedSystem = getSystem();
    getCache();

    Execution execution = FunctionService.onMember(distributedSystem, distributedSystem.getDistributedMember());
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
    properties.put(DistributionConfig.DEPLOY_WORKING_DIR, alternateDir.getAbsolutePath());
    InternalDistributedSystem distributedSystem = getSystem(properties);
    final JarDeployer jarDeployer = new JarDeployer(distributedSystem.getConfig().getDeployWorkingDir());

    File jarFile = getFirstVersionForTest(alternateDir, "JarDeployerDUnit.jar");
    byte[] jarBytes = this.classBuilder.createJarFromName("JarDeployerDUnitDTAC");
    jarDeployer.deploy(new String[] { "JarDeployerDUnit.jar" }, new byte[][] { jarBytes });

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
  public void testDeployToInvalidDirectory() throws IOException, ClassNotFoundException {
    final File alternateDir = new File("JarDeployerDUnit");
    FileUtil.delete(new File("JarDeployerDUnit"));
    
    final JarDeployer jarDeployer = new JarDeployer(alternateDir);
    final CyclicBarrier barrier = new CyclicBarrier(2);
    final byte[] jarBytes = this.classBuilder.createJarFromName("JarDeployerDUnitDTID");
    
    // Test to verify that deployment fails if the directory doesn't exist.
    try {
      jarDeployer.deploy(new String[] { "JarDeployerDUnit.jar" }, new byte[][] { jarBytes });
      fail("Exception should have been thrown due to missing deployment directory.");
    } catch (IOException expected) {
      // Expected.
    }
    
    // Test to verify that deployment succeeds if the directory doesn't
    // initially exist, but is then created while the JarDeployer is looping
    // looking for a valid directory.
    Thread thread = new Thread () {
      @Override
      public void run() {
        try {
          barrier.await();
        } catch (InterruptedException iex) {
          fail("Interrupted while waiting.");
        } catch (BrokenBarrierException bbex) {
          fail("Broken barrier.");
        }
        
        try {
          jarDeployer.deploy(new String[] { "JarDeployerDUnit.jar" }, new byte[][] { jarBytes });
        } catch (IOException ioex) {
          fail("IOException received unexpectedly.");
        } catch (ClassNotFoundException cnfex) {
          fail("ClassNotFoundException received unexpectedly.");
        }
      }
    };
    thread.start();
    
    try {
      barrier.await();
      Thread.sleep(500);
      alternateDir.mkdir();
      thread.join();
    } catch (InterruptedException iex) {
      fail("Interrupted while waiting.");
    } catch (BrokenBarrierException bbex) {
      fail("Broken barrier.");
    }
  }

  boolean okayToResume;
  @Test
  public void testSuspendAndResume() throws IOException, ClassNotFoundException {    
    final JarDeployer jarDeployer = new JarDeployer();
    byte[] jarBytes = this.classBuilder.createJarFromName("JarDeployerDUnitSAR");
    final JarDeployer suspendingJarDeployer = new JarDeployer();
    final CountDownLatch latch = new CountDownLatch(1);
    
    Thread thread = new Thread () {
      @Override
      public void run() {
        try {
          suspendingJarDeployer.suspendAll();
          latch.countDown();
          Thread.sleep(3000);
        } catch (InterruptedException iex) {
          // It doesn't matter, just fail the test
        }
        JarDeployerDUnitTest.this.okayToResume = true;
        suspendingJarDeployer.resumeAll();
      }
    };
    thread.start();
    
    try {
      latch.await();
    } catch (InterruptedException iex) {
      // It doesn't matter, just fail the test
    }
    jarDeployer.deploy(new String[] { "JarDeployerDUnit.jar" }, new byte[][] { jarBytes });
    if (!this.okayToResume) {
      fail("JarDeployer did not suspend as expected");
    }
  }
  

  @Test
  public void testZeroLengthFile() throws IOException, ClassNotFoundException {
    final JarDeployer jarDeployer = new JarDeployer();

    try {
      jarDeployer.deploy(new String[] { "JarDeployerDUnitZLF.jar" }, new byte[][] { new byte[0] });
      fail("Zero length files are not deployable");
    } catch (IllegalArgumentException expected) {
      // Expected
    }
    
    try {
      jarDeployer.deploy(new String[] { "JarDeployerDUnitZLF1.jar", "JarDeployerDUnitZLF2.jar" }, new byte[][] {
          this.classBuilder.createJarFromName("JarDeployerDUnitZLF1"), new byte[0] });
      fail("Zero length files are not deployable");
    } catch (IllegalArgumentException expected) { 
      // Expected
    }
  }

  @Test
  public void testInvalidJarFile() throws IOException, ClassNotFoundException {
    final JarDeployer jarDeployer = new JarDeployer();

    try {
      jarDeployer.deploy(new String[] { "JarDeployerDUnitIJF.jar" }, new byte[][] { "INVALID JAR CONTENT".getBytes() });
      fail("Non-JAR files are not deployable");
    } catch (IllegalArgumentException expected) {
      // Expected
    }

    try {
      jarDeployer.deploy(new String[] { "JarDeployerDUnitIJF1.jar", "JarDeployerDUnitIJF2.jar" }, new byte[][] {
          this.classBuilder.createJarFromName("JarDeployerDUnitIJF1"), "INVALID JAR CONTENT".getBytes() });
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
          fail("Error trying to create garbage file for test", ioex);
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
    @SuppressWarnings("resource")
    FileLock fileLock = new FileInputStream(file).getChannel().lock(0, 1, true);
    return fileLock;
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
    InputStream inStream = new FileInputStream(file);
    int index = 0;
    try {
      for (; index < bytes.length; index++) {
        if (((byte) inStream.read()) != bytes[index])
          break;
      }
    } finally {
      inStream.close();
    }

    // If we didn't get to the end then something was different
    if (index < bytes.length)
      return false;

    return true;
  }

  private void deleteSavedJarFiles() throws IOException {
    FileUtil.deleteMatching(new File("."), "^" + JarDeployer.JAR_PREFIX + "JarDeployerDUnit.*#\\d++$");
    FileUtil.delete(new File("JarDeployerDUnit"));
  }

  private File getFirstVersionForTest(final File saveDirfile, final String jarFilename) {
    final File[] oldJarFiles = findSortedOldVersionsOfJar(saveDirfile, jarFilename);
    if (oldJarFiles.length == 0) {
      return new File(saveDirfile, JarDeployer.JAR_PREFIX + jarFilename + "#1");
    }
    return getNextVersionJarFile(oldJarFiles[0]);
  }

  private File getNextVersionJarFile(final File oldJarFile) {
    final Matcher matcher = JarDeployer.versionedPattern.matcher(oldJarFile.getName());
    matcher.find();
    String newFileName = matcher.group(1) + "#" + (Integer.parseInt(matcher.group(2)) + 1);

    return new File(oldJarFile.getParentFile(), newFileName);
  }

  private File[] findSortedOldVersionsOfJar(final File saveDirfile, final String jarFilename) {
    // Find all matching files
    final Pattern pattern = Pattern.compile("^" + JarDeployer.JAR_PREFIX + jarFilename + "#\\d++$");
    final File[] oldJarFiles = saveDirfile.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(final File file, final String name) {
        return pattern.matcher(name).matches();
      }
    });

    // Sort them in order from newest (highest version) to oldest
    Arrays.sort(oldJarFiles, new Comparator<File>() {
      @Override
      public int compare(final File file1, final File file2) {
        int file1Version = extractVersionFromFilename(file1);
        int file2Version = extractVersionFromFilename(file2);
        return file2Version - file1Version;
      }
    });

    return oldJarFiles;
  }

  int extractVersionFromFilename(final File file) {
    final Matcher matcher = JarDeployer.versionedPattern.matcher(file.getName());
    matcher.find();
    return Integer.parseInt(matcher.group(2));
  }

  void writeJarBytesToFile(File jarFile, byte[] jarBytes) throws IOException {
    final OutputStream outStream = new FileOutputStream(jarFile);
    outStream.write(jarBytes);
    outStream.close();
  }
}
