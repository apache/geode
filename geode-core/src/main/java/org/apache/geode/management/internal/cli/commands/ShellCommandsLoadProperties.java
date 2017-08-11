package org.apache.geode.management.internal.cli.commands;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.i18n.CliStrings;

class ShellCommandsLoadProperties {
  static Properties loadProperties(File propertyFile) {
    try {
      return loadProperties(propertyFile.toURI().toURL());
    } catch (MalformedURLException e) {
      throw new RuntimeException(
          CliStrings.format("Failed to load configuration properties from pathname (%1$s)!",
              propertyFile.getAbsolutePath()),
          e);
    }
  }

  private static Properties loadProperties(URL url) {
    Properties properties = new Properties();

    if (url == null) {
      return properties;
    }

    try (InputStream inputStream = url.openStream()) {
      properties.load(inputStream);
    } catch (IOException io) {
      throw new RuntimeException(
          CliStrings.format(CliStrings.CONNECT__MSG__COULD_NOT_READ_CONFIG_FROM_0,
              CliUtil.decodeWithDefaultCharSet(url.getPath())),
          io);
    }

    return properties;
  }
}
