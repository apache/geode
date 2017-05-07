/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;
import java.util.*;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import com.gemstone.gemfire.LogWriter;

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
