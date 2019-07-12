package org.apache.geode.unsafe.internal.com.sun.jmx.remote.security;

import java.io.IOException;
import java.util.Properties;

import javax.management.MBeanServer;

public class MBeanServerFileAccessController
    extends com.sun.jmx.remote.security.MBeanServerFileAccessController {
  public MBeanServerFileAccessController(String accessFileName) throws IOException {
    super(accessFileName);
  }

  public MBeanServerFileAccessController(String accessFileName, MBeanServer mbs)
      throws IOException {
    super(accessFileName, mbs);
  }

  public MBeanServerFileAccessController(Properties accessFileProps) throws IOException {
    super(accessFileProps);
  }

  public MBeanServerFileAccessController(Properties accessFileProps,
      MBeanServer mbs) throws IOException {
    super(accessFileProps, mbs);
  }
}
