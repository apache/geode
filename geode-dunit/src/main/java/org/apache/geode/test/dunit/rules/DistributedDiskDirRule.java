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

import static org.apache.geode.internal.lang.SystemPropertyHelper.DEFAULT_DISK_DIRS_PROPERTY;
import static org.apache.geode.internal.lang.SystemPropertyHelper.GEODE_PREFIX;
import static org.apache.geode.internal.lang.SystemPropertyHelper.getProductStringProperty;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.lang.reflect.Method;
import java.util.Optional;

import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.Description;

import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
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
 * You may either pass in instances of SerializableTemporaryFolder and SerializableTestName from
 * the test or the DistributedDiskDirRule will create its own instances. Either way, it will invoke
 * SerializableTemporaryFolder.before and SerializableTestName.starting(Description). If the test
 * provides its own instances of these rules defined, please do not annotate these instances with
 * {@code @Rule}.
 *
 * <p>
 * Each JVM will have its own default DiskDirs directory in which that JVM will create the default
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
 * /var/folders/28/m__9dv1906n60kmz7t71wm680000gn/T/junit1766147044000254810
 *     VM-1-PRAccessorWithOverflowRegressionTest_testPROverflow-diskDirs
 *     VM0-PRAccessorWithOverflowRegressionTest_testPROverflow-diskDirs
 *     VM1-PRAccessorWithOverflowRegressionTest_testPROverflow-diskDirs
 *     VM2-PRAccessorWithOverflowRegressionTest_testPROverflow-diskDirs
 *     VM3-PRAccessorWithOverflowRegressionTest_testPROverflow-diskDirs
 * </pre>
 *
 * <p>
 * Example of test using DistributedDiskDirRule:
 *
 * <pre>
 * {@literal @}Category(DistributedTest.class)
 * public class PRAccessorWithOverflowRegressionTest extends CacheTestCase {
 *
 *     {@literal @}Rule
 *     public DistributedDiskDirRule diskDirsRule = new DistributedDiskDirRule();
 * </pre>
 */
@SuppressWarnings("serial,unused")
public class DistributedDiskDirRule extends DiskDirRule implements SerializableTestRule {

  private static volatile DistributedDiskDirRuleData data;

  private final SerializableTemporaryFolder temporaryFolder;
  private final SerializableTestName testName;
  private final RemoteInvoker invoker;

  private volatile int beforeVmCount;

  public DistributedDiskDirRule() {
    this(new Builder());
  }

  public DistributedDiskDirRule(SerializableTemporaryFolder temporaryFolder) {
    this(new Builder().temporaryFolder(temporaryFolder));
  }

  public DistributedDiskDirRule(SerializableTestName testName) {
    this(new Builder().testName(testName));
  }

  public DistributedDiskDirRule(SerializableTemporaryFolder temporaryFolder,
      SerializableTestName testName) {
    this(new Builder().temporaryFolder(temporaryFolder).testName(testName));
  }

  public DistributedDiskDirRule(Builder builder) {
    this(builder, new RemoteInvoker());
  }

  protected DistributedDiskDirRule(Builder builder, RemoteInvoker invoker) {
    this(builder.initializeHelperRules, builder.temporaryFolder, builder.testName, invoker);
  }

  protected DistributedDiskDirRule(boolean initializeHelperRules,
      SerializableTemporaryFolder temporaryFolder, SerializableTestName testName,
      RemoteInvoker invoker) {
    super(initializeHelperRules, null, null);
    this.temporaryFolder = temporaryFolder;
    this.testName = testName;
    this.invoker = invoker;
  }

  public File getDiskDirFor(VM vm) {
    return new File(vm.invoke(() -> System.getProperty(GEODE_PREFIX + DEFAULT_DISK_DIRS_PROPERTY)));
  }

  @Override
  protected void before(Description description) throws Exception {
    DUnitLauncher.launchIfNeeded();
    beforeVmCount = getVMCount();

    if (initializeHelperRules) {
      initializeHelperRules(description);
    }

    invoker.invokeInEveryVMAndController(() -> doBefore(this, description));
  }

  @Override
  protected void initializeHelperRules(Description description) throws Exception {
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

  @Override
  protected void after(Description description) {
    assertThat(getVMCount()).isEqualTo(beforeVmCount);

    invoker.invokeInEveryVMAndController(() -> doAfter());
  }

  @Override
  protected String getDiskDirName(String testClass) {
    return "VM" + VM.getCurrentVMNum() + "-" + testClass + "_" + testName.getMethodName()
        + "-diskDirs";
  }

  private void doBefore(DistributedDiskDirRule diskDirRule, Description description)
      throws Exception {
    data = new DistributedDiskDirRuleData(diskDirRule);

    Optional<String> value = getProductStringProperty(DEFAULT_DISK_DIRS_PROPERTY);
    value.ifPresent(s -> data.setOriginalValue(s));

    File diskDir = data.temporaryFolder().newFolder(getDiskDirName(getTestClassName(description)));

    System.setProperty(GEODE_PREFIX + DEFAULT_DISK_DIRS_PROPERTY, diskDir.getAbsolutePath());
  }

  private void doAfter() {
    if (data == null) {
      throw new Error("Failed to invoke " + getClass().getSimpleName() + ".before in VM-"
          + VM.getCurrentVMNum() + ". Rule does not support VM.bounce().");
    }
    if (data.originalValue() == null) {
      System.clearProperty(GEODE_PREFIX + DEFAULT_DISK_DIRS_PROPERTY);
    } else {
      System.setProperty(GEODE_PREFIX + DEFAULT_DISK_DIRS_PROPERTY, data.originalValue());
    }
  }

  private int getVMCount() {
    try {
      return getHost(0).getVMCount();
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException("DUnit VMs have not been launched");
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
   * Builds an instance of DistributedDiskDirRule
   */
  public static class Builder {
    private boolean initializeHelperRules = true;
    private SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();
    private SerializableTestName testName = new SerializableTestName();

    public Builder() {
      // nothing
    }

    /**
     * Specify false to disable initializing SerializableTemporaryFolder and SerializableTestName
     * during DistributedDiskDirRule initialization. If this is enabled then do NOT annotate these
     * helper rules in the test or combine them with RuleChain or RuleList. Default value is true.
     */
    public Builder initializeHelperRules(boolean value) {
      initializeHelperRules = value;
      return this;
    }

    public Builder temporaryFolder(SerializableTemporaryFolder temporaryFolder) {
      this.temporaryFolder = temporaryFolder;
      return this;
    }

    public Builder testName(SerializableTestName testName) {
      this.testName = testName;
      return this;
    }

    public DistributedDiskDirRule build() {
      return new DistributedDiskDirRule(this);
    }
  }
}
