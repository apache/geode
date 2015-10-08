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
package com.gemstone.gemfire.distributed.internal;

import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.org.jgroups.conf.XmlConfigurator;
import java.io.*;

/**
 * This class represents the configuration of a distribution manager.
 * Currently, it configures the {@link
 * com.gemstone.org.jgroups.JChannel} that is used to connect to
 * JGroups.
 *
 * @see XmlConfigurator
 */
public class DistributionManagerConfig {

  /** The name of the file in which the configuration information
   * resides. */
  public static final String FILE_NAME = "javagroups.xml";

  //////////////////////  Instance Fields  ///////////////////////

  /** A string representation of this config */
  private String string;

  ///////////////////////  Static Methods  ///////////////////////

  /**
   * Returns the default configuration for a distribution manager
   *
   * @param dir
   *        The directory in which the configuration file resides
   *
   * @throws IllegalArgumentException
   *         If <code>dir</code> does not exist or it does not contain
   *         a file named {@link #FILE_NAME}, or we had difficulties
   *         reading or parsing the file.
   */
 static DistributionManagerConfig getConfig(File dir) {
    if (!dir.exists()) {
      throw new IllegalArgumentException(LocalizedStrings.DistributionManagerConfig_CANNOT_READ_DISTRIBUTION_MANAGER_CONFIG_DIRECTORY_0_DOES_NOT_EXIST.toLocalizedString(dir));
    }

    File file = new File(dir, FILE_NAME);
    if (!file.exists()) {
      throw new IllegalArgumentException(LocalizedStrings.DistributionManagerConfig_CANNOT_READ_DISTRIBUTION_MANAGER_CONFIG_CONFIGURATION_FILE_0_DOES_NOT_EXIST.toLocalizedString(file));
    }

    try {
      return parse(file);

    } catch (Exception ex) {
      throw new IllegalArgumentException(LocalizedStrings.DistributionManagerConfig_WHILE_PARSING_0_1.toLocalizedString(new Object[] {file, ex}));
    }
  }

  /**
   * Parses the contents of a distribution manager config file and
   * from it creates a new <code>DistributionManagerConfig</code>.
   */
  private static DistributionManagerConfig parse(File file) 
    throws IOException {
    DistributionManagerConfig config = new DistributionManagerConfig();
    FileInputStream fis = new FileInputStream(file);
    try {
      XmlConfigurator conf =  XmlConfigurator.getInstance(fis);
      config.string = conf.getProtocolStackString();
    }
    finally {
      fis.close();
    }
    return config;
  }

  ////////////////////  Instance Methods  ////////////////////
  
  /**
   * Returns a String representation of this config.  Note that this
   * method is package protected because we may not need it in the
   * future and thus is should not be for public consumption.
   */
  String asString() {
    return this.string;
  }

}
