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
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.cache.query.IndexExistsException;
import org.apache.geode.cache.query.IndexNameConflictException;
import org.apache.geode.cache.Declarable;
import org.apache.geode.LogWriter;

public class CacheListenerForQueryIndex implements Declarable, CacheListener {

  public CacheListenerForQueryIndex() 
  {
    LogWriter logger = CacheFactory.getAnyInstance().getLogger();
    logger.info("Create cache listener for dummy region"); 
  }
	
  public void init(Properties props) {
  }

  public void afterCreate(EntryEvent event) {

    QueryService qs = CacheFactory.getAnyInstance().getQueryService();
    LogWriter logger = CacheFactory.getAnyInstance().getLogger();
    if( event.getKey().toString().trim().equalsIgnoreCase("port2-20") == true ) { 
      qs.removeIndexes();
      logger.info( "NOW removed Index on the regions");
      return;
    }
    if( event.getKey().toString().trim().equalsIgnoreCase("port1-20") == false ) { 
      return;
    }

    if(logger != null) {
      logger.info( "NOW Lets create an Index on the regions");
    }

    if(qs != null) {
      try{
        qs.createIndex("index1", IndexType.FUNCTIONAL, "ID", "/Portfolios");
        qs.createIndex("index2", IndexType.FUNCTIONAL, "ID", "/Portfolios2");
        qs.createIndex("index3", IndexType.FUNCTIONAL, "ID", "/Portfolios3");
        //qs.createIndex("index4", IndexType.FUNCTIONAL, "key", "/Portfolios.entrySet");
        //qs.createIndex("index5", IndexType.FUNCTIONAL, "p_k", "/Portfolios.keySet p_k");
        qs.createIndex("index6", IndexType.FUNCTIONAL, "secId", "/Positions");
        //qs.createIndex("index10", IndexType.FUNCTIONAL, "toString", "/Portfolios.keySet");
        qs.createIndex("index10", IndexType.FUNCTIONAL, "toString", "/Portfolios.keys");
        qs.createIndex("index11", IndexType.FUNCTIONAL, "ID", "/Portfolios.values");
        qs.createIndex("index12", IndexType.FUNCTIONAL, "nvl(k.position2.toString(),'nopes')", "/Portfolios.values k");
        qs.createIndex("index13", IndexType.FUNCTIONAL, "k", "/Portfolios.keys k");
      } catch (IndexNameConflictException e) {
         logger.info("Caught IndexNameConflictException");
      }  
      catch (IndexExistsException e) {
        logger.info("Caught IndexExistsException");
      } catch(QueryException e) {
         logger.info(e.getMessage());
      }
    } 
   }
   public void afterUpdate(EntryEvent event) 
   {
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
