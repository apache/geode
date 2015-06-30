/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.vmware.gemfire.tools.pulse.tests;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class JMXProperties extends Properties {
  private static final long serialVersionUID = -6210901350494570026L;

  private static JMXProperties props = new JMXProperties();

  public static JMXProperties getInstance() {
    return props;
  }

  public void load(String propFile) throws IOException {
    if (propFile != null) {
      FileInputStream fin;
      fin = new FileInputStream(new File(propFile));
      if (fin != null) {
        clear();
        load(fin);
      }

      fin.close();
    }
  }
}
