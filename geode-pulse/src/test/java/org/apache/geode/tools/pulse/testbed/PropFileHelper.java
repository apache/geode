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
