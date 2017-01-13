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
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.management.membership.*;

import java.util.Properties;
import org.apache.geode.cache.util.*;

public class PdxCacheListener<K,V> extends CacheListenerAdapter<K,V> implements Declarable {

  public void afterCreate(EntryEvent<K,V> e) {
    if(e.getKey().equals("success"))
      return;
    Object value = e.getNewValue();
    Region reg = e.getRegion();
    if (value instanceof PdxInstance) 
    {
     System.out.println("   got pdxInstance: " + value.toString());     
     reg.put("success", true);
    }
    else
    {
      System.out.println("   NOT GOT pdxInstance: "); 
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
    System.out.println(" DEBUG_LOG :: PdxCacheListener::init() ::  Called " );
    ClientMembership.registerClientMembershipListener(new BridgeClientMembershipListener());
    System.out.println(" DEBUG_LOG ::PdxCacheListener::init() :: registered BridgeMembershipListener successfully " );
  }

}
