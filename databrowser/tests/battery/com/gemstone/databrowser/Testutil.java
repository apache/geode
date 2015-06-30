package com.gemstone.databrowser;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import hydra.Log;

/**
 * Util Class for accessing file containing user defined values of property
 * variables.
 *
 * @author vreddy
 *
 */
public class Testutil {

  public final static String TEST_PREFERENCE_FILE = "com/gemstone/databrowser/objects/TestConfig.properties";

  private static Properties preferenceProperties = new Properties();
  static {
    ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();
    URL resource = systemClassLoader.getResource(TEST_PREFERENCE_FILE);
    try {
      InputStream openStream = resource.openStream();
      preferenceProperties.load(openStream);
    }
    catch (IOException e1) {
      Log.getLogWriter().error(e1);

    }
  }

  public static String getProperty(String key) {
    return preferenceProperties.getProperty(key);
  }
}
