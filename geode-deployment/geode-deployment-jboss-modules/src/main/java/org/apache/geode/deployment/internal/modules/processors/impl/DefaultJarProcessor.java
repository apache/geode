package org.apache.geode.deployment.internal.modules.processors.impl;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.jar.JarFile;

import org.apache.logging.log4j.Logger;

import org.apache.geode.deployment.internal.modules.processors.JarProcessor;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class DefaultJarProcessor implements JarProcessor {
  public static final String DEFAULT_IDENTIFIER = "default";
  private static final Logger logger = LogService.getLogger();

  @Override
  public String getIdentifier() {
    return DEFAULT_IDENTIFIER;
  }

  @Override
  public boolean canProcess(File file) {
    try (JarFile ignored = new JarFile(file)) {
      return true;
    } catch (IOException e) {
      logger.warn(e);
    }
    return false;
  }

  @Override
  public List<String> getResourcesFromJarFile(File file) {
    return Collections.singletonList(file.getAbsolutePath());
  }
}
