/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
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
