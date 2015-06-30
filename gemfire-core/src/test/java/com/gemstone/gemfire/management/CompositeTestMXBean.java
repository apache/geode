package com.gemstone.gemfire.management;

import java.util.Map;

public interface CompositeTestMXBean {
  //[A] MBean Attributes
  //0. Basic
  public CompositeStats   getCompositeStats();
  
  public CompositeStats   listCompositeStats();
  
  public Map<String,Integer>   getMap();
  
  public Integer[]  getIntegerArray();
  
  public CompositeStats[]  getCompositeArray();
}
