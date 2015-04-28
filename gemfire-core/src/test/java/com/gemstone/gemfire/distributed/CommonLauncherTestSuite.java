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
@SuppressWarnings("unused")
public abstract class CommonLauncherTestSuite {

  protected static File writeGemFirePropertiesToFile(final Properties gemfireProperties,
                                                     final String filename,
                                                     final String comment)
  {
    final File gemfirePropertiesFile = new File(System.getProperty("user.dir"), filename);

    try {
      gemfireProperties.store(new FileWriter(gemfirePropertiesFile, false), comment);
      return gemfirePropertiesFile;
    }
    catch (IOException e) {
      return null;
    }
  }

}
