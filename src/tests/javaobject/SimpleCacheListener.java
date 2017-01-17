/*
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
 */
package javaobject;

import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.util.CacheListenerAdapter;
import java.util.Properties;
import org.apache.geode.pdx.JSONFormatter;

public class SimpleCacheListener<K,V> extends CacheListenerAdapter<K,V> implements Declarable {

  final String jsonCustomer =  "{"
			        + "\"firstName\": \"John\","
			        + "\"lastName\": \"Smith\","
			        + "\"age\": 25,"
			        + "\"kids\":"
			        + "[\"Manan\", \"Nishka\" ]"
			        /*
			        + "\"address\":"
			        + "{"
			        + "\"streetAddress\": \"21 2nd Street\","
			        + "\"city\": \"New York\","
			        + "\"state\": \"NY\","
			        + "\"postalCode\": \"10021\""
			        + "}",
			        + "\"phoneNumber\":"
			        + "["
			        + "{"
			        + " \"type\": \"home\","
			        + "\"number\": \"212 555-1234\""
			        + "},"
			        + "{"
			        + " \"type\": \"fax\","
			        + "\"number\": \"646 555-4567\""
			        + "}"
			        + "]"
			        */
			        + "}";
  
 
  public void afterCreate(EntryEvent<K,V> e) {
    if(e.getKey().equals("success") || (e.getKey().equals("putFromjava")))
      return;
  //  printBytes( e.getSerializedNewValue().getSerializedValue());  
  try
  {
    System.out.println("    Received afterCreate event for entry: " +
      e.getKey() + ", " + e.getNewValue());
    Region reg = e.getRegion();
    reg.put("success", true);
    reg.put("putFromjava", new PdxTests.PdxType());
     
    System.out.println("NILKANTH JSON documents added into Cache: " + jsonCustomer);
    System.out.println();
    reg.put("jsondoc1", JSONFormatter.fromJSON(jsonCustomer));
    
    }catch(Exception ex)
    {
      Region reg = e.getRegion();
    reg.put("success", false);
    }
  }
  
  void printBytes(byte[] bytes)
  {
    for(int i = 0; i< bytes.length; i++)
    {
      
      System.out.print((int)bytes[i]);
      System.out.print(' ');
    }
    
  }
  public void afterUpdate(EntryEvent<K,V> e) {
    
    if(e.getKey().equals("success") || (e.getKey().equals("putFromjava")) || (e.getKey().equals("jsondoc1")) )
      return;
    printBytes( e.getSerializedNewValue().getSerializedValue());
    
    try
    {
      System.out.println("    Received afterUpdate event for entry: " +
        e.getKey() + ", " + e.getNewValue());
    
      Region reg = e.getRegion();
       reg.put("putFromjava", new PdxTests.PdxType());
      reg.put("success", true);
      
      reg.put("jsondoc1", JSONFormatter.fromJSON(jsonCustomer));
      }catch(Exception ex)
      {
        Region reg = e.getRegion();
      reg.put("success", false);
      }
  }
  
  public void afterDestroy(EntryEvent<K,V> e) {
    System.out.println("    Received afterDestroy event for entry: " +
      e.getKey());
  }

  public void afterInvalidate(EntryEvent<K,V> e) {
    System.out.println("    Received afterInvalidate event for entry: " +
      e.getKey());
  }

  public void afterRegionLive(RegionEvent e) {
    System.out.println("    Received afterRegionLive event, sent to durable clients after \nthe server has finished replaying stored events.  ");
  }

  public void init(Properties props) {
    // do nothing
  }

}
