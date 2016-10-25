/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.geode.tools.pulse.testbed;

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

  public TestBed(){
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    InputStream inputStream = classLoader.getResourceAsStream("testbed.properties");
    Properties properties = new Properties();
    try {
      properties.load(inputStream);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    propertiesFile = new PropFileHelper(properties);
    ds = new GemFireDistributedSystem("t1", propertiesFile.getProperties());
  }

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