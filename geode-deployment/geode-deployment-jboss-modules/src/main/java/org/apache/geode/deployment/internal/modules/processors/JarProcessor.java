package org.apache.geode.deployment.internal.modules.processors;

import java.io.File;
import java.util.List;

public interface JarProcessor {
  String getIdentifier();

  boolean canProcess(File file);

  List<String> getResourcesFromJarFile(File file);
}
