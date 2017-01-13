/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;
import java.util.*;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Declarable;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.LogWriter;

public class PutAllTimeout implements Declarable, CacheListener {

  public PutAllTimeout() 
  {
  }
  
  public void init(Properties props) {
    CacheClientProxy.isSlowStartForTesting = true;
  }

  public void afterCreate(EntryEvent event) {
    try
    {
     if (event.getKey().toString().indexOf("timeout") >= 0) {
        
        int sleepVal = Integer.parseInt( event.getNewValue().toString() );
        System.out.println("Put all timeout value " + event.getNewValue().toString()); 
        System.out.println("Put all timeout " + sleepVal);
        Thread.sleep(sleepVal);
        System.out.println("Put all sleep done");
      }              
    }catch(InterruptedException e)
    {
      
    }
    catch(Exception e)
    {
      System.out.println("Exception: After create " + e.getMessage());
    }
   }
   public void afterUpdate(EntryEvent event) 
   {
     try
     {
       if (event.getKey().toString().indexOf("timeout") >= 0) {
         int sleepVal = Integer.parseInt( event.getNewValue().toString() );
         System.out.println("Put all timeout " + sleepVal);
         Thread.sleep(sleepVal);
         System.out.println("Put all sleep done");
       }              
     }catch(InterruptedException e)
     {
       
     }
     catch(Exception e)
     {
       System.out.println("Exception: After update " + e.getMessage());
     }
   }
  
    public void afterInvalidate(EntryEvent event)
    {
    }

    public void afterDestroy(EntryEvent event)
    {
    }

    public void afterRegionInvalidate(RegionEvent event)
    {
    }

    public void afterRegionDestroy(RegionEvent event)
    {
    }

    public void afterRegionClear(RegionEvent event)
    {
    }

    public void afterRegionCreate(RegionEvent event)
    {
    }
    
    public void afterRegionLive(RegionEvent event)
    {
    }

    public void close()
    {
    }
}
