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

package com.gemstone.gemfire.distributed;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import org.junit.Rule;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

/**
 * The CommonLauncherTestSuite is a base class for encapsulating reusable functionality across the various, specific
 * launcher test suites.
 * </p>
 * @author John Blum
 * @see com.gemstone.gemfire.distributed.AbstractLauncherJUnitTest
 * @see com.gemstone.gemfire.distributed.LocatorLauncherJUnitTest
 * @see com.gemstone.gemfire.distributed.ServerLauncherJUnitTest
 * @since 7.0
 */
public abstract class CommonLauncherTestSuite {

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();
  
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  
  @Rule
  public final TestName testName = new TestName();
  
  protected File writeGemFirePropertiesToFile(final Properties gemfireProperties,
                                              final String filename,
                                              final String comment)
  {
    try {
      final File gemfirePropertiesFile = this.temporaryFolder.newFile(filename);
      gemfireProperties.store(new FileWriter(gemfirePropertiesFile, false), comment);
      return gemfirePropertiesFile;
    }
    catch (IOException e) {
      return null;
    }
  }

}
