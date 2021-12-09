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

import static org.apache.geode.internal.lang.SystemProperty.getProductStringProperty;
import static org.apache.geode.internal.lang.SystemPropertyHelper.DEFAULT_DISK_DIRS_PROPERTY;

import java.io.File;
import java.lang.reflect.Method;
import java.util.Optional;

import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.Description;

import org.apache.geode.internal.lang.SystemProperty;

/**
 * JUnit Rule that overrides the default DiskDirs directory. Internally, TemporaryFolder and
 * TestName are used by this rule to define the directory locations and names.
 *
 * <p>
 * Using DiskDirRule will produce a unique DiskDir such as:
 *
 * <pre>
 * /var/folders/28/m__9dv1906n60kmz7t71wm680000gn/T/junit2791461987256197453/org.apache.geode.test.junit.rules.DiskDirRuleIntegrationTest_diskDirPathContainsTestClassName-diskDirs
 * </pre>
 *
 * <p>
 * Example of test using DiskDirRule:
 *
 * <pre>
 * public class PersistentRegionIntegrationTest {
 *
 *   {@literal @}Rule
 *   public DiskDirRule diskDirRule = new DiskDirRule();
 * </pre>
 */
public class DiskDirRule extends DescribedExternalResource {

  protected static final String BEFORE = "before";
  protected static final String AFTER = "after";
  protected static final String STARTING = "starting";

  private final TemporaryFolder temporaryFolder;
  private final TestName testName;

  private String originalValue;

  public DiskDirRule() {
    this(new TemporaryFolder(), new TestName());
  }

  protected DiskDirRule(TemporaryFolder temporaryFolder, TestName testName) {
    this.temporaryFolder = temporaryFolder;
    this.testName = testName;
  }

  /**
   * Returns the current default disk dirs value.
   */
  public File getDiskDir() {
    return new File(System.getProperty(SystemProperty.DEFAULT_PREFIX + DEFAULT_DISK_DIRS_PROPERTY));
  }

  @Override
  protected void before(Description description) throws Exception {
    Optional<String> value = getProductStringProperty(DEFAULT_DISK_DIRS_PROPERTY);
    value.ifPresent(s -> originalValue = s);

    initializeHelperRules(description);

    File diskDir = temporaryFolder.newFolder(getDiskDirName(description.getClassName()));

    System.setProperty(SystemProperty.DEFAULT_PREFIX + DEFAULT_DISK_DIRS_PROPERTY,
        diskDir.getAbsolutePath());
  }

  @Override
  protected void after(Description description) {
    if (originalValue == null) {
      System.clearProperty(SystemProperty.DEFAULT_PREFIX + DEFAULT_DISK_DIRS_PROPERTY);
    } else {
      System.setProperty(SystemProperty.DEFAULT_PREFIX + DEFAULT_DISK_DIRS_PROPERTY, originalValue);
    }
  }

  protected String getTestClassName(Description description) {
    return description.getTestClass().getSimpleName();
  }

  private String getDiskDirName(String testClass) {
    return testClass + "_" + testName.getMethodName() + "-diskDirs";
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
}
