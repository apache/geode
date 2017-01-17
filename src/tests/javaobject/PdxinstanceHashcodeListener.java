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
import org.apache.geode.pdx.PdxInstance;

import java.util.Properties;

public class PdxinstanceHashcodeListener<K,V> extends CacheListenerAdapter<K,V> implements Declarable {

  public void afterCreate(EntryEvent<K,V> e) {
    if(e.getKey().equals("javaPdxHC") || e.getKey().equals("success"))
      return;
    Object value = e.getNewValue();
    Region reg = e.getRegion();
    if (value instanceof PdxInstance) 
    {
     System.out.println("   got pdxInstance: " + value.toString());  
 System.out.println("   got pdxInstance hashcode: " + value.hashCode());      
     reg.put("javaPdxHC", value.hashCode());
    }
    else
    {
      System.out.println("   NOT GOT pdxInstance: " + e.getKey()); 
      reg.put("javaPdxHC", false);
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
    if(e.getKey().equals("success"))
      return;
    Object value = e.getNewValue();
    Region reg = e.getRegion();
    if (value instanceof PdxInstance) 
    {
     System.out.println("   AU got pdxInstance: " + value.toString());
     reg.put("success", true);
    }
    else
    {
      System.out.println("   AU NOT GOT pdxInstance: "); 
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
