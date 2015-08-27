/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.admin.jmx.internal;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.modelmbean.ModelMBean;

import org.apache.commons.modeler.ManagedBean;
import org.apache.logging.log4j.Level;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.admin.SystemMemberCacheServer;
import com.gemstone.gemfire.admin.SystemMemberRegion;
import com.gemstone.gemfire.admin.internal.SystemMemberBridgeServerImpl;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.admin.AdminBridgeServer;
import com.gemstone.gemfire.internal.admin.GemFireVM;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;

/**
 * MBean representation of {@link com.gemstone.gemfire.admin.SystemMemberCache}.
 *
 * @author    Darrel Schneider
 * @author    Kirk Lund
 * @since     3.5
 */
public class SystemMemberCacheJmxImpl 
extends com.gemstone.gemfire.admin.internal.SystemMemberCacheImpl
implements com.gemstone.gemfire.admin.jmx.internal.ManagedResource {

  /** The object name of this managed resource */
  private ObjectName objectName;
  
  /** collection to collect all the resources created for this member */
  private Map<String, SystemMemberRegionJmxImpl> managedRegionResourcesMap = new HashMap<String, SystemMemberRegionJmxImpl>();
  private Map<Number, SystemMemberBridgeServerJmxImpl> managedCacheServerResourcesMap = new HashMap<Number, SystemMemberBridgeServerJmxImpl>();

  // -------------------------------------------------------------------------
  //   Constructor(s)
  // -------------------------------------------------------------------------
  
  /** 
   * Constructs an instance of SystemMemberCacheJmxImpl.
   *
   * @param vm
   *        The vm owning the cache this object will manage
   */
  public SystemMemberCacheJmxImpl(GemFireVM vm)
  throws com.gemstone.gemfire.admin.AdminException { 
    super(vm);
    initializeMBean();
  }

  /** Create and register the MBean to manage this resource */
  private void initializeMBean() 
  throws com.gemstone.gemfire.admin.AdminException {
    this.mbeanName = new StringBuffer("GemFire.Cache:")
        .append("name=")
        .append(MBeanUtil.makeCompliantMBeanNameProperty(getName()))
        .append(",id=")
        .append(getId())
        .append(",owner=")
        .append(MBeanUtil.makeCompliantMBeanNameProperty(vm.getId().toString()))
        .append(",type=Cache").toString();
      
    this.objectName = 
      MBeanUtil.createMBean(this, 
                            addDynamicAttributes(MBeanUtil.lookupManagedBean(this)));
  }
  
  // -------------------------------------------------------------------------
  //   Template methods overriden from superclass...
  // -------------------------------------------------------------------------

  /**
   * Override createSystemMemberRegion by instantiating
   * SystemMemberRegionJmxImpl. This instance is also added to the
   * managedResources collection.
   * 
   * @param r
   *          reference to Region instance for which this JMX resource is to be
   *          created
   * @return SystemMemberRegionJmxImpl - JMX Implementation of
   *         SystemMemberRegion
   * @throws AdminException
   *           if constructing SystemMemberRegionJmxImpl instance fails
   */
  @Override  
  protected SystemMemberRegion createSystemMemberRegion(Region r)
    throws com.gemstone.gemfire.admin.AdminException {
    SystemMemberRegionJmxImpl managedSystemMemberRegion = null;
    boolean needsRefresh = false;
    synchronized (this.managedRegionResourcesMap) {
      /* 
       * Ensuring that a single instance of System Member Region is created 
       * per Region.
       */
      SystemMemberRegionJmxImpl managedResource = managedRegionResourcesMap.get(r.getFullPath());
      if (managedResource != null) {
        managedSystemMemberRegion = managedResource;
      } else {
        managedSystemMemberRegion = new SystemMemberRegionJmxImpl(this, r);
        managedRegionResourcesMap.put(r.getFullPath(), managedSystemMemberRegion);
        needsRefresh = true;
      }
    }
    if (needsRefresh) {
      managedSystemMemberRegion.refresh();
    }
    return managedSystemMemberRegion;
  }

  /**
   * Creates a SystemMemberBridgeServerJmxImpl instance. This instance is also
   * added to the managedResources collection.
   * 
   * @param bridge
   *          reference to AdminBridgeServer for which this JMX resource is to
   *          be created
   * @return SystemMemberBridgeServerJmxImpl - JMX Implementation of
   *         SystemMemberBridgeServerImpl
   * @throws AdminException
   *           if constructing SystemMemberBridgeServerJmxImpl instance fails
   */
  @Override  
  protected SystemMemberBridgeServerImpl
    createSystemMemberBridgeServer(AdminBridgeServer bridge) 
    throws AdminException {
    SystemMemberBridgeServerJmxImpl managedSystemMemberBridgeServer = null;
    synchronized (this.managedCacheServerResourcesMap) {
      /* 
       * Ensuring that a single instance of SystemMember BridgeServer is 
       * created per AdminBridgeServer.
       */
      SystemMemberBridgeServerJmxImpl managedCacheServerResource = managedCacheServerResourcesMap.get(bridge.getId());
      if (managedCacheServerResource != null) {
        managedSystemMemberBridgeServer = managedCacheServerResource;
      } else {
        managedSystemMemberBridgeServer = new SystemMemberBridgeServerJmxImpl(this, bridge);
        managedCacheServerResourcesMap.put(bridge.getId(), managedSystemMemberBridgeServer);
      }
    }
    return managedSystemMemberBridgeServer;
  }

  // -------------------------------------------------------------------------
  //   Create MBean attributes for each Statistic
  // -------------------------------------------------------------------------
  
  /**
   * Add MBean attribute definitions for each Statistic.
   *
   * @param managed   the mbean definition to add attributes to
   * @return a new instance of ManagedBean copied from <code>managed</code> but 
   *         with the new attributes added
   */
  ManagedBean addDynamicAttributes(ManagedBean managed)
  throws com.gemstone.gemfire.admin.AdminException {
    if (managed == null) {
      throw new IllegalArgumentException(LocalizedStrings.SystemMemberCacheJmxImpl_MANAGEDBEAN_IS_NULL.toLocalizedString());
    }
    
    refresh(); // to get the stats...
    
    // need to create a new instance of ManagedBean to clean the "slate"...
    ManagedBean newManagedBean = new DynamicManagedBean(managed);
    for (int i = 0; i < this.statistics.length; i++) {
      StatisticAttributeInfo attrInfo = new StatisticAttributeInfo();

      attrInfo.setName(this.statistics[i].getName());
      attrInfo.setDisplayName(this.statistics[i].getName());
      attrInfo.setDescription(this.statistics[i].getDescription());
      attrInfo.setType("java.lang.Number");

      attrInfo.setIs(false);
      attrInfo.setReadable(true);
      attrInfo.setWriteable(false);

      attrInfo.setStat(this.statistics[i]);
      
      newManagedBean.addAttribute(attrInfo);
    }

    return newManagedBean;
  }

  // -------------------------------------------------------------------------
  //   MBean Operations
  // -------------------------------------------------------------------------

  /** 
   * Returns the ObjectName of the Region for the specified path.
   *
   * @throws AdminException
   *         If no region with path <code>path</code> exists
   */
  public ObjectName manageRegion(String path)
  throws AdminException, MalformedObjectNameException {
    try {
      SystemMemberRegionJmxImpl region = null;

      try {
        region = (SystemMemberRegionJmxImpl) getRegion(path);

      } catch (AdminException e) {
        MBeanUtil.logStackTrace(Level.WARN, e);
        throw e;
      }

      if (region == null) {
        throw new AdminException(LocalizedStrings.SystemMemberCacheJmxImpl_THIS_CACHE_DOES_NOT_CONTAIN_REGION_0.toLocalizedString(path));
          
      } else {
        return ObjectName.getInstance(region.getMBeanName());
      }
    } catch (RuntimeException e) { 
      MBeanUtil.logStackTrace(Level.WARN, e);
      throw e; 
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Error e) { 
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      MBeanUtil.logStackTrace(Level.ERROR, e);
      throw e; 
    }
  }

  /**
   * Creates a new cache server MBean and returns its
   * <code>ObjectName</code>.
   *
   * @since 5.7
   */
  public ObjectName manageCacheServer()
    throws AdminException, MalformedObjectNameException {

    try {
      SystemMemberBridgeServerJmxImpl bridge =
        (SystemMemberBridgeServerJmxImpl) addCacheServer();
      return ObjectName.getInstance(bridge.getMBeanName());
    } catch (AdminException e) { 
 	  MBeanUtil.logStackTrace(Level.WARN, e);
 	  throw e; 
    } catch (RuntimeException e) { 
	  MBeanUtil.logStackTrace(Level.WARN, e); 
	  throw e;
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Error e) { 
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      MBeanUtil.logStackTrace(Level.ERROR, e); 
      throw e; 
    }
  }

  /**
   * Returns the MBean <code>ObjectName</code>s for all cache servers
   * that serve this cache to clients.
   *
   * @since 4.0
   */
  public ObjectName[] manageCacheServers()
    throws AdminException, MalformedObjectNameException {

    try {
      SystemMemberCacheServer[] bridges = getCacheServers();
      ObjectName[] names = new ObjectName[bridges.length];
      for (int i = 0; i < bridges.length; i++) {
        SystemMemberBridgeServerJmxImpl bridge =
          (SystemMemberBridgeServerJmxImpl) bridges[i];
        names[i] = ObjectName.getInstance(bridge.getMBeanName());
      }

      return names;
    } catch (AdminException e) { 
  	  MBeanUtil.logStackTrace(Level.WARN, e); 
  	  throw e;
    } catch (RuntimeException e) { 
      MBeanUtil.logStackTrace(Level.WARN, e); 
      throw e; 
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Error e) { 
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      MBeanUtil.logStackTrace(Level.ERROR, e); 
      throw e;
    }
  }

  /**
   * Returns the MBean <code>ObjectName</code>s for all bridge servers
   * that serve this cache.
   *
   * @since 4.0
   * @deprecated as of 5.7
   */
  @Deprecated
  public ObjectName[] manageBridgeServers()
    throws AdminException, MalformedObjectNameException {
    return manageCacheServers();
  }

  // -------------------------------------------------------------------------
  //   ManagedResource implementation
  // -------------------------------------------------------------------------
  
  /** The name of the MBean that will manage this resource */
  private String mbeanName;

  /** The ModelMBean that is configured to manage this resource */
  private ModelMBean modelMBean;
  
	public String getMBeanName() {
		return this.mbeanName;
	}
  
	public ModelMBean getModelMBean() {
		return this.modelMBean;
	}
	public void setModelMBean(ModelMBean modelMBean) {
		this.modelMBean = modelMBean;
	}
  
  public ObjectName getObjectName() {
    return this.objectName;
  }

  public ManagedResourceType getManagedResourceType() {
    return ManagedResourceType.SYSTEM_MEMBER_CACHE;
  }

  
  /**
   * Un-registers all the statistics & cache managed resource created for this 
   * member. After un-registering the resource MBean instances, clears 
   * this.memberResources collection.
   * 
   * Creates ConfigurationParameterJmxImpl, StatisticResourceJmxImpl and 
   * SystemMemberCacheJmxImpl. But cleans up only StatisticResourceJmxImpl and 
   * SystemMemberCacheJmxImpl which are of type ManagedResource.  
   */
  public void cleanupResource() {
    synchronized (this.managedRegionResourcesMap) {
      Collection<SystemMemberRegionJmxImpl> values = managedRegionResourcesMap.values();
      
      for (SystemMemberRegionJmxImpl systemMemberRegionJmxImpl : values) {
        MBeanUtil.unregisterMBean(systemMemberRegionJmxImpl);
      }
      
      this.managedRegionResourcesMap.clear();
    }
    
    synchronized (this.managedCacheServerResourcesMap) {
      Collection<SystemMemberBridgeServerJmxImpl> values = managedCacheServerResourcesMap.values();
      
      for (SystemMemberBridgeServerJmxImpl SystemMemberBridgeServerJmxImpl : values) {
        MBeanUtil.unregisterMBean(SystemMemberBridgeServerJmxImpl);
      }
      
      this.managedCacheServerResourcesMap.clear();
    }
  }

  /**
   * Cleans up managed resources created for the region that was (created and)
   * destroyed in a cache represented by this Managed Resource.
   * 
   * @param regionPath
   *          path of the region that got destroyed
   * @return a managed resource related to this region path
   */
  public ManagedResource cleanupRegionResources(String regionPath) {
    ManagedResource cleaned = null;
    
    synchronized (this.managedRegionResourcesMap) {
      Set<Entry<String, SystemMemberRegionJmxImpl>> entries = managedRegionResourcesMap.entrySet();
      for (Iterator<Entry<String, SystemMemberRegionJmxImpl>> it = entries.iterator(); it.hasNext();) {
        Entry<String, SystemMemberRegionJmxImpl> entry = it.next();
        SystemMemberRegionJmxImpl managedResource = entry.getValue();
        ObjectName                objName         = managedResource.getObjectName();
        
        String pathProp = objName.getKeyProperty("path");
        if (pathProp != null && pathProp.equals(regionPath)) {
          cleaned = managedResource;
          it.remove();
          
          break;
        }
      }
    }

    return cleaned;
  }
  
  /**
   * Checks equality of the given object with <code>this</code> based on the
   * type (Class) and the MBean Name returned by <code>getMBeanName()</code>
   * methods.
   * 
   * @param obj
   *          object to check equality with
   * @return true if the given object is if the same type and its MBean Name is
   *         same as <code>this</code> object's MBean Name, false otherwise
   */
  @Override
  public boolean equals(Object obj) {
    if ( !(obj instanceof SystemMemberCacheJmxImpl) ) {
      return false;
    }
    
    SystemMemberCacheJmxImpl other = (SystemMemberCacheJmxImpl) obj;

    return this.getMBeanName().equals(other.getMBeanName());
  }
  
  /**
   * Returns hash code for <code>this</code> object which is based on the MBean 
   * Name generated. 
   * 
   * @return hash code for <code>this</code> object
   */
  @Override
  public int hashCode() {
    return this.getMBeanName().hashCode();
  }
}

