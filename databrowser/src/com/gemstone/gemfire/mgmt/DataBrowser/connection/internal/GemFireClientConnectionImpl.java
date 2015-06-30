/*=========================================================================
 * (c)Copyright 2002-2011, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.connection.internal;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.query.CqAttributes;
import com.gemstone.gemfire.cache.query.CqAttributesFactory;
import com.gemstone.gemfire.cache.query.CqListener;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.GFMemberDiscovery;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.GemFireConnection;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.GemFireConnectionListener;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.GemFireMemberListener;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.GemFireMember;
import com.gemstone.gemfire.mgmt.DataBrowser.prefs.DataBrowserPreferences;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionRepository;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryExecutionException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryResult;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryUtil;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.CQException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.CQQuery;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.CQQueryImpl;
import com.gemstone.gemfire.mgmt.DataBrowser.query.cq.CQResult;
import com.gemstone.gemfire.mgmt.DataBrowser.query.internal.PdxHelper;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;

public class GemFireClientConnectionImpl implements GemFireConnection {

  static final String GEMFIRE_LIMIT_AVL = "6.*";
  private DistributedSystem system;
  private Cache             cache;
  private GFMemberDiscovery discovery;
  private Map<String, CQQuery> cqs;
  private volatile boolean closed;
  private boolean limitClauseAvl = false;

  public GemFireClientConnectionImpl(DistributedSystem sys,
      GFMemberDiscovery dscvry) {
    if (sys == null) {
      throw new IllegalArgumentException(
          "A valid GemFire client connection is required.");
    } else if (dscvry == null) {
      throw new IllegalArgumentException(
          "A valid member discovery mechanism must be provided.");
    }

    this.system = sys;
    this.cache = createCache(this.system);
    this.discovery = dscvry;
    this.cqs = Collections.synchronizedMap(new HashMap<String, CQQuery>());
    this.closed = false;
    
    String version = discovery.getGemFireSystemVersion();
    
    /**
     * A check is added to verify if the current version of GemFire supports LIMIT clause.
     * LIMIT clause is available from GemFire 6.0 onwards.
     **/
    if(null == version) {
      this.limitClauseAvl = false;
    } else {
      this.limitClauseAvl = version.matches(GEMFIRE_LIMIT_AVL); 
    }
    String gemFireJarVersion = GemFireVersion.getGemFireVersion();
    LogUtil.info("Version of GemFire jar in classpath: "+gemFireJarVersion);
    LogUtil.info("The GemFire system version is :"+version);
    LogUtil.info("This version of GemFire system supports LIMIT clause :"+this.limitClauseAvl);
    
    //See comment for IntrospectionRepository.removePdxIntrospectionResults. 
    //Done only if Pdx is supported
    IntrospectionRepository repos = IntrospectionRepository.singleton();
    if (PdxHelper.getInstance().isPdxSupported()) {
      if (repos.removePdxIntrospectionResults()) {
        LogUtil.fine("Cleared cached PDX Type information");
      }
    }
  }

  public void close() {
    if (this.system != null) {
      this.closed = true;
      
      //Stop All CQs.
      List<String> currCqs = new ArrayList<String>();
      currCqs.addAll(this.cqs.keySet());
      for(String name : currCqs) {
       try {
          closeCQ(name);
        }
        catch (CQException e) {
          LogUtil.warning("Failed to close CQ with name :" + name);
        } 
      }
      
      this.discovery.close();
      this.cache.close();
      this.system.disconnect();
      this.cache = null;
      this.system = null;
    }
  }

  public QueryResult executeQuery(String query, GemFireMember member)
      throws QueryExecutionException {
    boolean param = limitClauseAvl && DataBrowserPreferences.getEnforceLimitToQuery();
    QueryService service = getQueryService(member);
    return QueryUtil.executeQuery(service, query, param);
  }
  
  public CQQuery newCQ(String name, String queryStr, GemFireMember member)
      throws QueryExecutionException {
    if(this.closed) {
      throw new IllegalStateException("This connection is closed."); 
    }
    
    if(member == null)
     throw new IllegalArgumentException("Please specify a non-null member attribute");
    if(name == null)
     throw new IllegalArgumentException("Please specify a non-null name attribute for CQ."); 
    
    if(!member.isNotifyBySubscriptionEnabled()) {
     throw new CQException("notify-by-subscription is not enabled on this server "+member.getId()); 
    }
    
    if(this.cqs.containsKey(name)) {
      LogUtil.info("A CQ with name :"+name+" already exists. Hence closing the this CQ.");
      closeCQ(name);
    }
    
    QueryService service = getQueryService(member);
    
    CQResult result = new CQResult();
    CqListener[] listeners = new CqListener[] {result};
    
    CqAttributesFactory cqAf = new CqAttributesFactory();
    cqAf.initCqListeners(listeners); 
    
    CqAttributes attrs = cqAf.create();
    
    try {
      CqQuery query = service.newCq(name, queryStr, attrs);
      CQQueryImpl wrapper = new CQQueryImpl(member, query, result);
      
      this.cqs.put(name, wrapper);
      
      return wrapper;
    }
    catch (Exception e) {
      throw new CQException(e);      
    }
  }

  public CQQuery getCQ(String name) {
    if(name == null)
      throw new IllegalArgumentException("Please specify a non-null name attribute for CQ.");    
    
    return this.cqs.get(name); 
  }
  

  public void closeCQ(String name) throws CQException {
    CQQuery query = getCQ(name);
    
    if((query != null) && (!query.isClosed())) {
      query.close(); 
      LogUtil.info("A CQ with name :"+name+" is closed.");
    }    
    
    //Make sure to remove the reference of CQ.
    this.cqs.remove(name);
  }
  
  public void stopCQ(String name) throws CQException {
    CQQuery query = getCQ(name);
    if((query != null) && (query.isRunning())) {
      query.stop();
    }    
  }

  public void addGemFireMemberListener(GemFireMemberListener listener) {
    this.discovery.addGemFireMemberListener(listener);
  }

  public GemFireMember getMember(String id) {
    return this.discovery.getMember(id);
  }

  public GemFireMember[] getMembers() {
    return this.discovery.getMembers();
  }

  public QueryService getQueryService(GemFireMember member) {
    return this.discovery.getQueryService(member);
  }

  public void removeGemFireMemberListener(GemFireMemberListener listener) {
    this.discovery.removeGemFireMemberListener(listener);
  }
  

  public void addConnectionNotificationListener(GemFireConnectionListener listener) {
    this.discovery.addConnectionNotificationListener(listener);   
  }
  

  public void removeConnectionNotificationListener(GemFireConnectionListener listener) {
    this.discovery.removeConnectionNotificationListener(listener);    
  }
  
  public String getGemFireSystemVersion() {
    return this.discovery.getGemFireSystemVersion();
  }
  
  public long getRefreshInterval() {
    return this.discovery.getRefreshInterval();
  }
  
  public void setRefreshInterval(long time) {
    this.discovery.setRefreshInterval(time);    
  }

  /**
   * Create cache. For version prior to 6.6 uses
   * CacheFactory.create(DistributedSystem). For 6.6 & later it uses reflection
   * to create the cache. Using reflection we also set configuration required to
   * receive PdxInstance for PdxSerializable objects. The cache is loner as per
   * the properties set in DistributedSystem.
   * 
   * @param system
   *          Distributed System instance
   * @return Cache instance
   */
  private Cache createCache(DistributedSystem system) {
    Cache cache = null;
    boolean pdxSupported = PdxHelper.getInstance().isPdxSupported();
    
    if (pdxSupported) {
      // Not using ClientCacheFactory as 6.0 didn't have it.
      CacheFactory factory = new CacheFactory();
      Class<? extends CacheFactory> klass = factory.getClass();
      Exception failure = null;
      try {
        Class<?>[] booleanArgs = new Class<?>[] {boolean.class};
        Object[]   booleanVals = new Object[] {Boolean.TRUE};

        Method setPdxIgnoreUnreadFields = klass.getMethod("setPdxIgnoreUnreadFields", booleanArgs);
        Method setPdxReadSerialized     = klass.getMethod("setPdxReadSerialized", booleanArgs);
        Method create                   = klass.getMethod("create", new Class<?>[0]);
        
        //CacheFactory.setPdxIgnoreUnreadFields(boolean) - as Data Browser will only be reading data
        setPdxIgnoreUnreadFields.invoke(factory, booleanVals);
        //CacheFactory.setPdxReadSerialized(boolean)
        setPdxReadSerialized.invoke(factory, booleanVals);
        //CacheFactory.create()
        cache = (Cache) create.invoke(factory, new Object[0]);
      } catch (NoSuchMethodException e) {
        failure = e;
      } catch (IllegalAccessException e) {
        failure = e;
      } catch (InvocationTargetException e) {
        failure = e;
      } finally {
        if (failure != null) {
          LogUtil.error("Failed to create GemFire(" + CacheFactory.getVersion() 
              + ") Cache.", failure);
          throw new UnsupportedOperationException("Failed to create cache " +
          		"with PDX Support. Verify GemFire product directory is accessible.");
        }
      }
    } else {
      cache = CacheFactory.create(system);
    }
    LogUtil.info("GemFire("+CacheFactory.getVersion()+") Cache is created. " +
                 "PDX support is" + (pdxSupported ? "" : " NOT") + " available.");
    
    return cache;
  }
}
