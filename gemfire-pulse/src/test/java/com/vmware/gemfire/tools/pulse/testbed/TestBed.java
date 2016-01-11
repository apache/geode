/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.vmware.gemfire.tools.pulse.testbed;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class TestBed {
  
  private String fileName=null;
  PropFileHelper propertiesFile =null;
  GemFireDistributedSystem ds = null;
  
  public TestBed(String fileName) throws FileNotFoundException, IOException{
    this.fileName = fileName;
    propertiesFile = new PropFileHelper(fileName);
    ds = new GemFireDistributedSystem("t1", propertiesFile.getProperties());
  }
  
  
  public TestBed(String fileName,boolean flag) throws FileNotFoundException, IOException{    
//    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
//    InputStream inputStream = classLoader.getResourceAsStream("testbed.properties");
//    System.out.println("Inputstream : " + inputStream);
    Properties properties = new Properties();
    try {
      properties.load(new FileInputStream(new File(fileName)));
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }    
    this.fileName = fileName;
    propertiesFile = new PropFileHelper(properties);
    ds = new GemFireDistributedSystem("t1", propertiesFile.getProperties());
  }
  
  
  public String getBrowserForDriver(){
    return propertiesFile.readKey("browser");
  }
  
  public String getBrowserVersionForDriver(String browser){
    return propertiesFile.readKey("browserVersion");
  }
  
  public GemFireDistributedSystem getRootDs(){
    return ds;
  }  

}