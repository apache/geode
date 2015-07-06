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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PropFileHelper {
  
  private String filePath=null;
  private Properties pr=null;
  
  public PropFileHelper(String filePath) throws FileNotFoundException, IOException{
    this.filePath = filePath;
    pr = new Properties();
    pr.load(new FileInputStream(new File(this.filePath)));
  }
  
  public PropFileHelper(Properties pr2) {
    this.pr =pr2;
  }

  public String[] readValues(String property){
    return readValues(property,",");
  }
  
  public String[] readValues(String property, String separator){
    String value = readKey(property);
    if(value!=null){
      String[] array = value.split(separator);
      return array;
    }else{
      return new String[0];
    }
  }
  
  public String readKey(String key){
    String value = pr.getProperty(key);
    if(value!=null)
      return value.trim();
    else return value;
  }
  
  public Map<String,String> readObject(String leadingkey){
    Map<String,String> map = new HashMap<String,String>();
    String leadingKeyString = leadingkey+"."; 
    for(Object keyObject : pr.keySet()){
      String key = (String)keyObject;
      String value = readKey(key);
      if(key.startsWith(leadingKeyString)){
        String innerProp = key.substring(leadingKeyString.length());
        /* inner object stuff
        if(checkForMultipleValues){
          if(innerProp.contains(separator)){
            String array[] = readValues(key);
          }
        }else*/
        {
          //System.out.println("Adding prop with key " + innerProp + " k=" + leadingkey);
          map.put(innerProp, value);
        }      
      }
    }    
    return map;
  }
  
  public static void main(String[] args) {
    
    Properties pr = new Properties();
    pr.put("topologies", "t1,t2");
    pr.put("t1.id", "1");
    pr.put("t2.id", "2");
    
    pr.put("t1.prop1", "prop11");
    pr.put("t1.prop2", "prop12");
    pr.put("t1.prop3", "prop13");
    pr.put("t2.prop1", "1");
    pr.put("t2.prop2", "2");
    pr.put("t2.prop3", "3");
    
    PropFileHelper helper = new PropFileHelper(pr);
    String topologies[] = helper.readValues("topologies");
    for(String topology : topologies){
      Map<String,String> topologyMap = helper.readObject(topology);
      System.out.println(topologyMap);
    }
    
  }

  public Properties getProperties() {
    return pr;
  }

}
