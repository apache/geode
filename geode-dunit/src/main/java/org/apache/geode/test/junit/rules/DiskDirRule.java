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
package org.apache.geode.test.junit.rules;

import static org.apache.geode.internal.lang.SystemPropertyHelper.DEFAULT_DISK_DIRS_PROPERTY;
import static org.apache.geode.internal.lang.SystemPropertyHelper.GEODE_PREFIX;
import static org.apache.geode.internal.lang.SystemPropertyHelper.getProductStringProperty;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;

import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.Description;

@SuppressWarnings("unused")
public class DiskDirRule extends DescribedExternalResource {

  protected static final String BEFORE = "before";
  protected static final String AFTER = "after";
  protected static final String STARTING = "starting";

  protected final boolean initializeHelperRules;

  private final TemporaryFolder temporaryFolder;
  private final TestName testName;

  private String originalValue;

  public DiskDirRule() {
    this(new Builder());
  }

  public DiskDirRule(TemporaryFolder temporaryFolder) {
    this(new Builder().temporaryFolder(temporaryFolder));
  }

  public DiskDirRule(TestName testName) {
    this(new Builder().testName(testName));
  }

  public DiskDirRule(TemporaryFolder temporaryFolder, TestName testName) {
    this(new Builder().temporaryFolder(temporaryFolder).testName(testName));
  }

  public DiskDirRule(Builder builder) {
    this(builder.initializeHelperRules, builder.temporaryFolder, builder.testName);
  }

  protected DiskDirRule(boolean initializeHelperRules, TemporaryFolder temporaryFolder,
      TestName testName) {
    this.initializeHelperRules = initializeHelperRules;
    this.temporaryFolder = temporaryFolder;
    this.testName = testName;
  }

  @Override
  protected void before(Description description) throws Exception {
    Optional<String> value = getProductStringProperty(DEFAULT_DISK_DIRS_PROPERTY);
    value.ifPresent(s -> originalValue = s);

    if (initializeHelperRules) {
      initializeHelperRules(description);
    }

    File diskDir =
        temporaryFolder.newFolder(getDiskDirName(getDiskDirName(description.getClassName())));

    System.setProperty(GEODE_PREFIX + DEFAULT_DISK_DIRS_PROPERTY, diskDir.getAbsolutePath());
  }

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
    if (originalValue == null) {
      System.clearProperty(GEODE_PREFIX + DEFAULT_DISK_DIRS_PROPERTY);
    } else {
      System.setProperty(GEODE_PREFIX + DEFAULT_DISK_DIRS_PROPERTY, originalValue);
    }
  }

  protected String getDiskDirName(String testClass) {
    return testClass + "_" + testName.getMethodName() + "-diskDirs";
  }

  protected String getTestClassName(Description description) {
    return description.getTestClass().getSimpleName();
  }

  protected void invokeTemporaryFolderBefore(TemporaryFolder temporaryFolder) {
    if (temporaryFolder != null) {
      invoke(TemporaryFolder.class, temporaryFolder, BEFORE);
    }
  }

  protected void invokeTestNameBefore(TestName testName) {
    if (testName != null) {
      invoke(TestName.class, testName, BEFORE);
    }
  }

  protected <V> V invoke(Class<?> targetClass, Object targetInstance, String methodName) {
    try {
      Method method = targetClass.getDeclaredMethod(methodName);
      method.setAccessible(true);
      return (V) method.invoke(targetInstance);
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new Error(e);
    }
  }

  /**
   * Builds an instance of DiskDirRule
   */
  public static class Builder {
    private boolean initializeHelperRules = true;
    private TemporaryFolder temporaryFolder = new TemporaryFolder();
    private TestName testName = new TestName();

    public Builder() {
      // nothing
    }

    /**
     * Specify false to disable initializing TemporaryFolder and TestName during DiskDirRule
     * initialization. If this is enabled then do NOT annotate these helper rules in the test or
     * combine them with RuleChain or RuleList. Default value is true.
     */
    public Builder initializeHelperRules(boolean value) {
      initializeHelperRules = value;
      return this;
    }

    public Builder temporaryFolder(TemporaryFolder temporaryFolder) {
      this.temporaryFolder = temporaryFolder;
      return this;
    }

    public Builder testName(TestName testName) {
      this.testName = testName;
      return this;
    }

    public DiskDirRule build() {
      return new DiskDirRule(this);
    }
  }
}
