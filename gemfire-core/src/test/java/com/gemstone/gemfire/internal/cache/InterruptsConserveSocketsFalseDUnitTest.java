package com.gemstone.gemfire.internal.cache;

import java.util.Properties;

public class InterruptsConserveSocketsFalseDUnitTest extends
    InterruptsDUnitTest {

  public InterruptsConserveSocketsFalseDUnitTest(String name) {
    super(name);
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = super.getDistributedSystemProperties();
    props.setProperty("conserve-sockets", "false");
    return props;
  }
  
  

}
