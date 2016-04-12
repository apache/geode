package com.gemstone.gemfire.management.internal.cli.commands;

import com.gemstone.gemfire.management.internal.cli.HeadlessGfsh;

import java.util.Properties;

public class ConfigCommandsSecurityDunitTest extends ConfigCommandsDUnitTest {

  public ConfigCommandsSecurityDunitTest(String name) {
    super(name);
  }

  protected HeadlessGfsh createDefaultSetup(Properties props){
    if (props==null) {
      props = new Properties();
    }
    props.put("jsonFile", "cacheServer.json");


    return super.createDefaultSetup(props);
  }
}
