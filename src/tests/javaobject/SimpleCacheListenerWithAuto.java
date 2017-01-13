/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;

import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.util.CacheListenerAdapter;
import java.util.Properties;

public class SimpleCacheListenerWithAuto<K,V> extends CacheListenerAdapter<K,V> implements Declarable {

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
     reg.put("putFromjava", new PdxTests.PdxTestsWithAuto());
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
    
    if(e.getKey().equals("success") || (e.getKey().equals("putFromjava")))
      return;
    printBytes( e.getSerializedNewValue().getSerializedValue());
    
    try
    {
      System.out.println("    Received afterUpdate event for entry: " +
        e.getKey() + ", " + e.getNewValue());
    
      Region reg = e.getRegion();
       reg.put("putFromjava", new PdxTests.PdxTestsWithAuto());
      reg.put("success", true);
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
