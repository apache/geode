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
