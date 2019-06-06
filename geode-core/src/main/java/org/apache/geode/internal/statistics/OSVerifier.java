package org.apache.geode.internal.statistics;

import org.apache.geode.InternalGemFireException;

public class OSVerifier {

  /**
   * Logic extracted from HostStatHelper static initializer.
   * Having a separated class for it, makes testing far easier.
   */
  public OSVerifier(){
    String osName = System.getProperty("os.name", "unknown");
    if (!osName.startsWith("Linux")){
      throw new InternalGemFireException(String.format("Unsupported OS %s. Only Linux(x86) OSs is supported.",osName));
    }
  }
}
