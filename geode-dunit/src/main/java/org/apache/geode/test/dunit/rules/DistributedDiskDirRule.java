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
package org.apache.geode.test.dunit.rules;

import static org.apache.geode.internal.lang.SystemProperty.getProductStringProperty;
import static org.apache.geode.internal.lang.SystemPropertyHelper.DEFAULT_DISK_DIRS_PROPERTY;
import static org.apache.geode.test.dunit.VM.DEFAULT_VM_COUNT;
import static org.apache.geode.test.dunit.VM.getCurrentVMNum;

import java.io.File;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.Optional;

import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.Description;

import org.apache.geode.internal.lang.SystemProperty;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.VMEventListener;
import org.apache.geode.test.dunit.internal.DUnitLauncher;
import org.apache.geode.test.junit.rules.DiskDirRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;
import org.apache.geode.test.junit.rules.serializable.SerializableTestRule;

/**
 * JUnit Rule that overrides the default DiskDirs directory in all DUnit VMs (except for the hidden
 * Locator VM). Internally, SerializableTemporaryFolder and SerializableTestName are used by this
 * rule to define the directory locations and names.
 *
 * <p>
 * Each JVM will have its own default DiskDir directory in which that JVM will create the default
 * disk store (if one is created). Each DiskDir name is defined as:
 *
 * <pre>
 * "VM" + VM.getCurrentVMNum() + "-" + testClass + "_" + testName.getMethodName() + "-diskDirs"
 * </pre>
 *
 * Using DistributedDiskDirRule will produce unique DiskDirs for each DUnit VM including the main
 * controller VM (-1) but not the locator VM (-2):
 *
 * <pre>
 * /var/folders/28/m__9dv1906n60kmz7t71wm680000gn/T/junit7783603075891789189/
 *     VM-1-DistributedDiskDirRuleDistributedTest_setsDefaultDiskDirsPropertyInEveryVm-diskDirs
 *     VM0-DistributedDiskDirRuleDistributedTest_setsDefaultDiskDirsPropertyInEveryVm-diskDirs
 *     VM1-DistributedDiskDirRuleDistributedTest_setsDefaultDiskDirsPropertyInEveryVm-diskDirs
 *     VM2-DistributedDiskDirRuleDistributedTest_setsDefaultDiskDirsPropertyInEveryVm-diskDirs
 *     VM3-DistributedDiskDirRuleDistributedTest_setsDefaultDiskDirsPropertyInEveryVm-diskDirs
 * </pre>
 *
 * <p>
 * Example of test using DistributedDiskDirRule:
 *
 * <pre>
 * public class DistributedDiskDirRuleDistributedTest implements Serializable {
 *
 *   {@literal @}Rule
 *   public DistributedRule distributedRule = new DistributedRule();
 *
 *   {@literal @}Rule
 *   public DistributedDiskDirRule distributedDiskDirRule = new DistributedDiskDirRule();
 * </pre>
 */
@SuppressWarnings("serial,unused")
public class DistributedDiskDirRule extends DiskDirRule implements SerializableTestRule {

  private static volatile DistributedDiskDirRuleData data;

  private final SerializableTemporaryFolder temporaryFolder;
  private final SerializableTestName testName;
  private final int vmCount;
  private final RemoteInvoker invoker;
  private final VMEventListener vmEventListener;

  private String testClassName;

  public DistributedDiskDirRule() {
    this(DEFAULT_VM_COUNT, new SerializableTemporaryFolder(), new SerializableTestName());
  }

  public DistributedDiskDirRule(int vmCount) {
    this(vmCount, new SerializableTemporaryFolder(), new SerializableTestName());
  }

  private DistributedDiskDirRule(int vmCount, SerializableTemporaryFolder temporaryFolder,
      SerializableTestName testName) {
    this(vmCount, temporaryFolder, testName, new RemoteInvoker());
  }

  private DistributedDiskDirRule(int vmCount, SerializableTemporaryFolder temporaryFolder,
      SerializableTestName testName, RemoteInvoker invoker) {
    super(null, null);
    this.temporaryFolder = temporaryFolder;
    this.testName = testName;
    this.vmCount = vmCount;
    this.invoker = invoker;
    vmEventListener = new InternalVMEventListener();
  }

  /**
   * Returns the current default disk dirs value for the specified VM.
   */
  public File getDiskDirFor(VM vm) {
    return new File(
        vm.invoke(
            () -> System.getProperty(SystemProperty.DEFAULT_PREFIX + DEFAULT_DISK_DIRS_PROPERTY)));
  }

  @Override
  protected void before(Description description) throws Exception {
    DUnitLauncher.launchIfNeeded(vmCount);
    VM.addVMEventListener(vmEventListener);

    initializeHelperRules(description);

    testClassName = getTestClassName(description);
    invoker.invokeInEveryVMAndController(() -> doBefore(this));
  }

  @Override
  protected void after(Description description) {
    VM.removeVMEventListener(vmEventListener);
    invoker.invokeInEveryVMAndController(this::doAfter);
  }

  private String getDiskDirName(String testClass) {
    return "VM" + getCurrentVMNum() + "-" + testClass + "_" + testName.getMethodName()
        + "-diskDirs";
  }

  private void initializeHelperRules(Description description) throws Exception {
    if (temporaryFolder != null) {
      Method method = TemporaryFolder.class.getDeclaredMethod(BEFORE);
      method.setAccessible(true);
      method.invoke(temporaryFolder);
    }

    if (testName != null) {
      Method method = TestName.class.getDeclaredMethod(STARTING, Description.class);
      method.setAccessible(true);
      method.invoke(testName, description);
    }
  }

  private void afterCreateVM(VM vm) {
    vm.invoke(() -> doBefore(this));
  }

  private void afterBounceVM(VM vm) {
    vm.invoke(() -> doBefore(this));
  }

  private void doBefore(DistributedDiskDirRule diskDirRule) throws Exception {
    data = new DistributedDiskDirRuleData(diskDirRule);

    Optional<String> value = getProductStringProperty(DEFAULT_DISK_DIRS_PROPERTY);
    value.ifPresent(s -> data.setOriginalValue(s));

    File diskDir = new File(data.temporaryFolder().getRoot(), getDiskDirName(testClassName));
    if (!diskDir.exists()) {
      Files.createDirectory(diskDir.toPath());
    }

    System.setProperty(SystemProperty.DEFAULT_PREFIX + DEFAULT_DISK_DIRS_PROPERTY,
        diskDir.getAbsolutePath());
  }

  private void doAfter() {
    if (data == null) {
      throw new Error("Failed to invoke " + getClass().getSimpleName() + ".before in VM-"
          + getCurrentVMNum() + ". Rule does not support VM.bounce().");
    }
    if (data.originalValue() == null) {
      System.clearProperty(SystemProperty.DEFAULT_PREFIX + DEFAULT_DISK_DIRS_PROPERTY);
    } else {
      System.setProperty(SystemProperty.DEFAULT_PREFIX + DEFAULT_DISK_DIRS_PROPERTY,
          data.originalValue());
    }
  }

  /**
   * Data for DistributedDiskDirRule for each DUnit child VM.
   */
  private static class DistributedDiskDirRuleData {
    private final SerializableTemporaryFolder temporaryFolder;
    private final SerializableTestName testName;

    private volatile String originalValue;

    DistributedDiskDirRuleData(DistributedDiskDirRule diskDirRule) {
      this(diskDirRule.temporaryFolder, diskDirRule.testName);
    }

    private DistributedDiskDirRuleData(SerializableTemporaryFolder temporaryFolder,
        SerializableTestName testName) {
      this.temporaryFolder = temporaryFolder;
      this.testName = testName;
    }

    SerializableTemporaryFolder temporaryFolder() {
      return temporaryFolder;
    }

    SerializableTestName testName() {
      return testName;
    }

    String originalValue() {
      return originalValue;
    }

    void setOriginalValue(String originalValue) {
      this.originalValue = originalValue;
    }
  }

  /**
   * VMEventListener for DistributedDiskDirRule.
   */
  private class InternalVMEventListener implements VMEventListener, Serializable {

    @Override
    public void afterCreateVM(VM vm) {
      DistributedDiskDirRule.this.afterCreateVM(vm);
    }

    @Override
    public void afterBounceVM(VM vm) {
      DistributedDiskDirRule.this.afterBounceVM(vm);
    }
  }
}
