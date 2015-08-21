package com.gemstone.gemfire.internal.logging;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.logging.log4j.Level;

public class LogServiceIntegrationTestSupport {

  public static void writeConfigFile(final File configFile, final Level level) throws IOException {
    final BufferedWriter writer = new BufferedWriter(new FileWriter(configFile));
    writer.write(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
        "<Configuration>" +
          "<Loggers>" +
            "<Root level=\"" + level.name() + "\"/>" +
          "</Loggers>" +
         "</Configuration>"
         );
    writer.close();
  }
}
