package com.gemstone.gemfire.internal.tools.gfsh.util;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.admin.AdminDistributedSystemFactory;
import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.admin.CacheVm;
import com.gemstone.gemfire.admin.DistributedSystemConfig;
import com.gemstone.gemfire.admin.SystemMember;
import com.gemstone.gemfire.admin.SystemMemberCache;
import com.gemstone.gemfire.admin.SystemMemberRegion;
import com.gemstone.gemfire.admin.internal.AdminDistributedSystemImpl;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.MirrorType;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.util.BridgeClient;
import com.gemstone.gemfire.cache.util.BridgeLoader;
import com.gemstone.gemfire.cache.util.BridgeWriter;
import com.gemstone.gemfire.distributed.DistributedSystem;


/**
 * <p>Copyright: Copyright (c) 2005</p>
 * <p>Company: GemStone Systems, Inc.</p>
 * @author Dae Song Park
 * @version 1.0
 */
public class RegionUtil
{
    /**
     * Creates a subregion of the specified parentRegion.
     * @param parentRegion The parent region. If null, it creates a top-level
     *                     region.
     * @param regionName The subregion name
     * @param scope The scope
     * @param dataPolicy The data policy
     * @param listener The cache listener
     * @throws OrderManagerException Thrown if it is unable to create the region.
     */
    public static Region createRegion(Region parentRegion, String regionName, Scope scope,
    		DataPolicy dataPolicy, CacheListener listener)
                               throws CacheException
    {
        return createRegion(parentRegion, regionName, scope, dataPolicy, listener, null, null);
    }

    public static Region createRegion(Region parentRegion, String regionName, Scope scope,
                                      DataPolicy dataPolicy, CacheListener listener,
                                      File[] persistenceDirs, EvictionAttributes evictionAttributes)
                               throws CacheException
    {
    	return createRegion(parentRegion, regionName, scope, dataPolicy, listener, persistenceDirs, evictionAttributes, null);
    }
    
    public static Region createRegion(Region parentRegion, String regionName, Scope scope,
            DataPolicy dataPolicy, CacheListener listener,
            File[] persistenceDirs, EvictionAttributes evictionAttributes, String endpoints)
     throws CacheException
	{
    	Region region = parentRegion.getSubregion(regionName);
    	if (region != null) {
    		return region;
    	}
        AttributesFactory factory = new AttributesFactory();

        factory.setScope(scope);
        factory.setDataPolicy(dataPolicy);
        if (listener != null) {
        	factory.addCacheListener(listener);
        }

        if (persistenceDirs != null) {
            factory.setDiskDirs(persistenceDirs);
        }

        factory.setEvictionAttributes(evictionAttributes);
        
        if (endpoints != null) {
            BridgeWriter bridgeWriter = new BridgeWriter();
            Properties prop = new Properties();
            prop.setProperty("endpoints", endpoints);
            prop.setProperty("establishCallbackConnection", "true");
            bridgeWriter.init(prop);
            factory.setCacheWriter(bridgeWriter);
            
            BridgeLoader bridgeLoader = new BridgeLoader();
            prop = new Properties();
            prop.setProperty("endpoints", endpoints);
            bridgeLoader.init(prop);
            factory.setCacheLoader(bridgeLoader);
        }

        RegionAttributes attr = factory.create();

        return parentRegion.createSubregion(regionName, attr);
	}

    public static Region createVMRegion(Cache cache, String regionName, Scope scope,
                                        DataPolicy dataPolicy, CacheListener listener)
                                 throws CacheException
    {
        AttributesFactory factory = new AttributesFactory();

        factory.setScope(scope);
        factory.setDataPolicy(dataPolicy);
        if (listener != null) {
        	factory.addCacheListener(listener);
        }

        RegionAttributes attr = factory.create();

        return cache.createRegion(regionName, attr);
    }
    
    /**
     * Returns the parent region of the specified path.
     * @param regionPath The region path.
     * @return the parent region of the specified path.
     */
    public static Region getParentRegion(String regionPath)
    {
    	if (regionPath == null) {
    		return null;
    	}
    	
    	Cache cache = CacheFactory.getAnyInstance();
    	String split[];
    	if (regionPath.startsWith("/")) {
    		split = regionPath.substring(1).split("/");
    	} else {
    		split = regionPath.split("/");
    	}
    	if (split.length == 0) {
    		return null;
    	}
    	Region region = cache.getRegion(split[0]);
    	if (region != null) {
    		int i;
	    	for (i = 1; i < split.length; i++) {
	    		Region subregion = region.getSubregion(split[i]);
	    		if (subregion == null) {
	    			break;
	    		}
	    		region = subregion;
	    	}
	    	if (i != split.length - 1) {
	    		region = null;
	    	}
    	}
    	return region;
    }
    
    /**
     * Returns the last region
     * @param regionPath
     * @return the last region
     */
    public static Region getLastRegionInPath(String regionPath)
    {
    	if (regionPath == null) {
    		return null;
    	}
    	
    	Cache cache = CacheFactory.getAnyInstance();
    	String split[] = regionPath.split("/");
    	if (split.length == 0) {
    		return null;
    	}
    	
    	Region region = cache.getRegion(split[0]);
    	if (region != null) {
	    	for (int i = 1; i < split.length; i++) {
	    		Region subregion = region.getSubregion(split[i]);
	    		if (subregion == null) {
	    			break;
	    		}
	    		region = subregion;
	    	}
    	}
    	return region;
    }
    
    /**
     * Returns the specified region. It creates one if it does not exist.
     * @param regionPath
     * @param scope
     * @param mirrorType
     * @param endpoints
     * @return Returns the specified region. It creates one if it does not exist.
     * @deprecated Use #getRegion(String regionPath, Scope scope, DataPolicy dataPolicy, String endpoints) instead
     */
    public static Region getRegion(String regionPath, Scope scope, MirrorType mirrorType, String endpoints)
    {
    	DataPolicy dataPolicy;
    	if (mirrorType == MirrorType.KEYS || mirrorType == MirrorType.KEYS_VALUES) {
    		dataPolicy = DataPolicy.REPLICATE;
    	} else if (mirrorType == MirrorType.NONE) {
    		dataPolicy = DataPolicy.NORMAL;
    	} else {
    		dataPolicy = DataPolicy.DEFAULT;
    	}
    	return getRegion(regionPath, scope, dataPolicy, endpoints);
    }
    
    // nk94960
    public static Region getRegion(String regionPath, Scope scope, DataPolicy dataPolicy, String endpoints)
      throws CacheException
      {
        return getRegion(regionPath, scope, dataPolicy, endpoints, false, null);
      }

    //nk94960
    public static Region getRegion(String regionPath, Scope scope, DataPolicy dataPolicy, String endpoints, boolean enableBridgeConflation)
      throws CacheException
      {
        return getRegion(regionPath, scope, dataPolicy, endpoints, enableBridgeConflation, null);

      }
    
    /**
     * Returns the region specified. It creates a new region if it does not
     * exist.
     * 
     * The following system properties are used to create BridgeLoader and
     * BridgeWriter for the region if endpoints is specified (i.e., if enpoints
     * is not null.) 
     * <p>
     * <li>readTimeout (optional: default 10000)</li>
	 * <li>allowableServerTimeouts (optional: default 7 timeouts)</li> 
	 * <li>allowableServerTimeoutPeriod (optional: default: 10000 milliseconds)</li> 
	 * <li>retryAttempts (optional: default 5)</li> 
	 * <li>retryInterval (optional: default 10000)</li>
	 * <li>LBPolicy (optional: default "Sticky")</li>
	 * <li>connectionsPerServer (optional: default 2)</li>
	 * <li>socketBufferSize (optional: default 32768)</li>
	 * <p>
     * 
     * @param regionPath
     * @param scope
     * @param dataPolicy
     * @param endpoints
     * @param enableBridgeConflation
     * @return the region specified
     * @throws CacheException
     */
    public static Region getRegion(String regionPath, Scope scope, DataPolicy dataPolicy, String endpoints, boolean enableBridgeConflation, String diskDir) //nk94960
    throws CacheException
    {
      Cache cache = null;
      Region region = null;

      try {
	cache = CacheFactory.getAnyInstance();
	region = cache.getRegion(regionPath);

	if (region != null) {
	  return region;
	}
      } catch (CacheClosedException ex) {
	Properties props = new Properties();
	DistributedSystem system = DistributedSystem.connect(props);
	cache = CacheFactory.create(system);
      }

      // Parse region path
      StringTokenizer st = new StringTokenizer(regionPath, "/");
      String regionName;
      int index = 0;
      int count = st.countTokens();
      AttributesFactory factory = new AttributesFactory();
      factory.setDataPolicy(DataPolicy.NORMAL);

      while (st.hasMoreTokens()) {
	regionName = st.nextToken();
	index++;
            
	if (index == count) {
	  factory.setDataPolicy(dataPolicy);

	  if (endpoints != null) {
	    String establishCallbackConnection = System.getProperty("establishCallbackConnection", "true");
	    Long readTimeout = Long.getLong("readTimeout", 10000);
	    Integer allowableServerTimeouts = Integer.getInteger("allowableServerTimeouts", 7);
	    Long allowableServerTimeoutPeriod  = Long.getLong("allowableServerTimeoutPeriod ", 10000);
	    Integer retryAttempts = Integer.getInteger("retryAttempts", 5);
	    Long retryInterval = Long.getLong("retryInterval", 10000);
	    String lbPolicy = System.getProperty("LBPolicy", "Sticky");
	    Integer connectionsPerServer = Integer.getInteger("connectionsPerServer", 2);
	    Integer socketBufferSize  = Integer.getInteger("socketBufferSize ", 32768);
	    BridgeClient bridgeClient = new BridgeClient();
	    Properties prop = new Properties();
	    prop.setProperty("endpoints", endpoints);
	    prop.setProperty("establishCallbackConnection", establishCallbackConnection);
	    prop.setProperty("readTimeout", readTimeout.toString());
	    prop.setProperty("allowableServerTimeouts", allowableServerTimeouts.toString());
	    prop.setProperty("allowableServerTimeoutPeriod ", allowableServerTimeoutPeriod.toString());
	    prop.setProperty("retryAttempts", retryAttempts.toString());
	    prop.setProperty("retryInterval", retryInterval.toString());
	    prop.setProperty("LBPolicy", lbPolicy);
	    prop.setProperty("connectionsPerServer", connectionsPerServer.toString());
	    prop.setProperty("socketBufferSize", socketBufferSize.toString());
	    bridgeClient.init(prop);
	    factory.setCacheLoader(bridgeClient);
	  } else {
	    factory.setEnableBridgeConflation(enableBridgeConflation);
	    factory.setEnableAsyncConflation(enableBridgeConflation);
	  }
	}

	if (index == 1) {
	  region = cache.getRegion(regionName);

	  if (region == null) {
	    factory.setScope(scope);
	    //nk94960
	    if(diskDir != null){
	      File[] dirs = new File[1];
	      dirs[0] = new File(diskDir);
					   
	      factory.setDiskDirs(dirs);
	    }
	    region = cache.createRegion(regionName, factory.create());
	  }
	} else {
	  Region subregion = region.getSubregion(regionName);
	  if (subregion == null) {
	    factory.setScope(scope);
	    //nk94960
	    if(diskDir != null){
	      File[] dirs = new File[1];
	      dirs[0] = new File(diskDir);
					   
	      factory.setDiskDirs(dirs);
	    }
	    subregion = region.createSubregion(regionName, factory.create());
	  }

	  region = subregion;
	}
      }

      return region;
    }

    public static Region getRegion(String regionPath, DataPolicy dataPolicy)
                            throws CacheException
    {
        return getRegion(regionPath, Scope.DISTRIBUTED_NO_ACK, dataPolicy, null);
    }

    public static Region getRegion(String regionPath) throws CacheException
    {
        return getRegion(regionPath, Scope.DISTRIBUTED_NO_ACK, DataPolicy.NORMAL, null);
    }
    
    public static Region getRegion(String regionPath, Scope scope, DataPolicy dataPolicy, Pool pool,
			boolean enableBridgeConflation) throws CacheException
	{
		Cache cache = null;
		Region region = null;

		try {
			cache = CacheFactory.getAnyInstance();
			region = cache.getRegion(regionPath);

			if (region != null) {
				return region;
			}
		} catch (CacheClosedException ex) {
			Properties props = new Properties();
			DistributedSystem system = DistributedSystem.connect(props);
			cache = CacheFactory.create(system);
		}

		// Parse region path
		StringTokenizer st = new StringTokenizer(regionPath, "/");
		String regionName;
		int index = 0;
		int count = st.countTokens();
		AttributesFactory factory = new AttributesFactory();
		factory.setDataPolicy(DataPolicy.NORMAL);

		while (st.hasMoreTokens()) {
			regionName = st.nextToken();
			index++;

			if (index == count) {
				factory.setDataPolicy(dataPolicy);
				factory.setPoolName(pool.getName());
			}

			if (index == 1) {
				region = cache.getRegion(regionName);

				if (region == null) {
					factory.setScope(scope);
					region = cache.createRegion(regionName, factory.create());
				}
			} else {
				Region subregion = region.getSubregion(regionName);
				if (subregion == null) {
					factory.setScope(scope);
					subregion = region.createSubregion(regionName, factory.create());
				}

				region = subregion;
			}
		}

		return region;
	}

    /**
     * Returns a sorted list of all region full paths found in the specified
     * distributed system.
     * @param ds The distributed system to search.
     * @return Returns a sorted list of all region paths defined in the 
     *         distributed system.  
     */
    public static List getAllRegionPathListInDistributedSystem(DistributedSystem ds, boolean recursive)
    {
    	List list = new ArrayList();
    	try {
    		// members 
    		AdminDistributedSystem adminSystem = getAdminDistributedSystemConnected(ds);
            SystemMember[] members = adminSystem.getSystemMemberApplications();
            for (int i = 0; i < members.length; i++) {
                SystemMemberCache cache = members[i].getCache();
                
                if (cache != null) {
                	list = getRegionPaths(cache, list, recursive);
                }
            }
            
            // cache servers
            CacheVm[] vms = adminSystem.getCacheVms();
            for (int i = 0; i < vms.length; i++) {
            	SystemMemberCache cache = vms[i].getCache();
            	if (cache != null) {
                	list = getRegionPaths(cache, list, recursive);
                }
			}
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        Collections.sort(list);
        return list;
    }
    
    /**
     * Returns a sorted String array of all region full paths found in the specified
     * distributed system.
     * @param ds The distributed system to search.
     * @return Returns a String array of all region paths defined in the 
     *         distributed system.  
     */
    public static String[] getAllRegionPathsInDistributedSystem(DistributedSystem ds, boolean recursive)
    {
    	List list = getAllRegionPathListInDistributedSystem(ds, recursive);
        return (String[])list.toArray(new String[0]);
    }
    
    public static List getRegionPathList(SystemMemberCache cache, boolean recursive)
			throws Exception
	{
    	return getRegionPaths(cache, new ArrayList(), recursive);
	}
    
    public static String[] getRegionPaths(SystemMemberCache cache, boolean recursive)
			throws Exception
	{
    	List list = getRegionPathList(cache, recursive);
    	return (String[])list.toArray(new String[0]);
	}
    
    private static List getRegionPaths(SystemMemberCache cache, List list, boolean recursive)
    					throws Exception
	{
		// get a list of all root regions
		Set regions = cache.getRootRegionNames();
		Iterator itor = regions.iterator();
		
		while (itor.hasNext()) {
			String regionPath = "/" + itor.next().toString();
			
			SystemMemberRegion systemMemberRegion = cache.getRegion(regionPath);
			if (list.contains(regionPath) == false) {
				list.add(regionPath);
			}
			if (recursive && systemMemberRegion.getSubregionCount() > 0) {
				getRegionPaths(systemMemberRegion, cache, list);
			}
		}
		return list;
	}
    
    private static List getRegionPaths(SystemMemberRegion systemMemberRegion, SystemMemberCache cache, List list, boolean recursive) throws Exception
	{
		// get a list of all root regions
		Set regionNames = systemMemberRegion.getSubregionNames();
		list.addAll(regionNames);

		Iterator itor = regionNames.iterator();
		while (itor.hasNext()) {
			String regionPath = (String)itor.next();
			if (list.contains(regionPath) == false) {
				list.add(regionPath);
			}
			SystemMemberRegion subregion = cache.getRegion(regionPath);
			if (recursive && subregion != null && subregion.getSubregionCount() > 0) {
				list = getRegionPaths(subregion, cache, list, recursive);
			}
		}
		return list;
	}
    
    private static List getRegionPaths(SystemMemberRegion systemMemberRegion, SystemMemberCache cache, List list)
	     				throws Exception
	{
		// get a list of all subregions
		Set subregions = systemMemberRegion.getSubregionFullPaths();
		Iterator itor = subregions.iterator();
		
		while (itor.hasNext()) {
			String regionPath = itor.next().toString();
			if (list.contains(regionPath) == false) {
				list.add(regionPath);
			}
			
			SystemMemberRegion subregion = cache.getRegion(regionPath);
			
			if (subregion != null && subregion.getSubregionCount() > 0) {
				getRegionPaths(subregion, cache, list);
			}
		}
		return list;
	}
    
    public static List getAllRegionPathList(Cache cache)
    {
    	return getAllRegionPathList(cache, true);
    }
    
    /**
     * Returns a sorted list of all region full paths found in the specified
     * cache.
     * @param cache The cache to search.
     * @return Returns a sorted list of all region paths defined in the 
     *         distributed system.  
     */
    public static List getAllRegionPathList(Cache cache, boolean recursive)
    {
    	ArrayList list = new ArrayList();
    	if (cache == null) {
    		return list;
    	}
    	
    	// get a list of all root regions
		Set regions = cache.rootRegions();
		Iterator itor = regions.iterator();

		while (itor.hasNext()) {
			String regionPath = ((Region)itor.next()).getFullPath();

			Region region = cache.getRegion(regionPath);
			list.add(regionPath);
			Set subregionSet = region.subregions(true);
			if (recursive) {
				for (Iterator subIter = subregionSet.iterator(); subIter.hasNext(); ){
					list.add(((Region)subIter.next()).getFullPath());
				}
			}
		}
		Collections.sort(list);
		return list;
    }
    
    public static String[] getAllRegionPaths(Cache cache)
    {
    	return getAllRegionPaths(cache, true);
    }
    
    /**
     * Returns a sorted String array of all region full paths found in the specified
     * cache.
     * @param cache The cache to search.
     * @return Returns a sorted String array of all region paths defined in the 
     *         distributed system.  
     */
    public static String[] getAllRegionPaths(Cache cache, boolean recursive)
    {
    	List list = getAllRegionPathList(cache, recursive);
        return (String[])list.toArray(new String[0]);
    }
    
    public static List getAllRegionPathList(Region region, boolean recursive)
    {
    	List list = new ArrayList();
    	if (region == null) {
    		return list;
    	}
    	
    	Set subregionSet = region.subregions(true);
		if (recursive) {
			for (Iterator subIter = subregionSet.iterator(); subIter.hasNext(); ){
				list.add(((Region)subIter.next()).getFullPath());
			}
		}
		
        Collections.sort(list);
        return list;
    }
    
    public static String[] getAllRegionPaths(Region region, boolean recursive)
    {
    	List list = getAllRegionPathList(region, recursive);
        return (String[])list.toArray(new String[0]);
    }
 
    public static List getAllRegionPathListInDistributedSystem(Region region, boolean recursive)
    {
    	DistributedSystem ds = region.getCache().getDistributedSystem();
    	String regionPath = region.getFullPath();
    	List list = new ArrayList();
    	try {
    		AdminDistributedSystem adminSystem = getAdminDistributedSystemConnected(ds);
            SystemMember[] members = adminSystem.getSystemMemberApplications();
            
            for (int i = 0; i < members.length; i++) {
                SystemMemberCache cache = members[i].getCache();

                if (cache != null) {
                	SystemMemberRegion sregion = cache.getRegion(regionPath);
                	list = getRegionPaths(sregion, cache, list, recursive);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        Collections.sort(list);
        return list;
    }
    
    /*
     * Returns a sorted String array of all region full paths found in the specified
     * distributed system.
     * @param ds The distributed system to search.
     * @return Returns a String array of all region paths defined in the 
     *         distributed system.  
     *///FIXME: Java Docs needs serious fixing.
    public static String[] getAllRegionPathsInDistributedSystem(Region region, boolean recursive)
    {
    	List list = getAllRegionPathListInDistributedSystem(region, recursive);
        return (String[])list.toArray(new String[0]);
    }
    
    public static AdminDistributedSystem getAdminDistributedSystemConnected(DistributedSystem ds) throws AdminException
    {
    	AdminDistributedSystem adminSystem = AdminDistributedSystemImpl.getConnectedInstance();
		if (adminSystem == null) {
            DistributedSystemConfig config = AdminDistributedSystemFactory.defineDistributedSystem(ds, null);
            adminSystem = AdminDistributedSystemFactory.getDistributedSystem(config);
		}
        
        try {
        	if (adminSystem.isConnected() == false) {
        		adminSystem.connect();
        	}
        } catch (Exception ex) {
        	// ignore
        }
        return adminSystem;
    }
    

    /**
     * Creates the entire regions found in the distributed system.
     * @param ds
     * @param scope
     */
    public static void createAllRegionsInDistributedSystem(DistributedSystem ds, Scope scope)
    {
        try {
            AdminDistributedSystem adminSystem = getAdminDistributedSystemConnected(ds);
            SystemMember[] members = adminSystem.getSystemMemberApplications();

            for (int i = 0; i < members.length; i++) {
                SystemMemberCache cache = members[i].getCache();

                if (cache != null) {
                    createCache(cache, scope);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void createAllRegionsInCacheServers(DistributedSystem ds, Scope scope)
    {
        try {
        	AdminDistributedSystem adminSystem = getAdminDistributedSystemConnected(ds);
            CacheVm[] cacheVms = adminSystem.getCacheVms();

            for (int i = 0; i < cacheVms.length; i++) {
                SystemMemberCache cache = cacheVms[i].getCache();

                if (cache != null) {
                    createCache(cache, scope);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createCache(SystemMemberCache cache, Scope scope)
                      throws Exception
    {
        // get a list of all root regions
        Set regions = cache.getRootRegionNames();
        Iterator itor = regions.iterator();
        Scope regionScope;

        while (itor.hasNext()) {
            String regionName = itor.next().toString();

            SystemMemberRegion systemMemberRegion = cache.getRegion(regionName);

            if (scope == null) {
                regionScope = systemMemberRegion.getScope();
            } else {
                regionScope = scope;
            }

            Region region = getRegion(systemMemberRegion.getFullPath(), regionScope, DataPolicy.NORMAL, null);

            if (systemMemberRegion.getSubregionCount() > 0) {
                createRegion(region, systemMemberRegion, cache, scope);
            }
        }
    }

    private static void createRegion(Region region, SystemMemberRegion systemMemberRegion,
                              SystemMemberCache cache, Scope scope)
                       throws Exception
    {
        // get a list of all subregions
        Set subregions = systemMemberRegion.getSubregionFullPaths();
        Iterator itor = subregions.iterator();

        while (itor.hasNext()) {
            String regionName = itor.next().toString();

            SystemMemberRegion subregion = cache.getRegion(regionName);
            region = getRegion(systemMemberRegion.getFullPath(), scope, DataPolicy.NORMAL, null);

            if (subregion.getSubregionCount() > 0) {
                createRegion(region, subregion, cache, scope);
            }
        }
    }
    
    /**
     * Locally clears the specified region. 
     * @param region The region to be cleared.
     * @throws CacheException Thrown if it encounters a cache error.
     */
    public final static void clearLocalRegion(Region region) throws CacheException
    {
    	if (region == null) {
    		return;
    	}
        for (Iterator iterator = region.keySet().iterator(); iterator.hasNext();) {
            try {
            	region.localDestroy(iterator.next());
            } catch (EntryNotFoundException ex) {
            	// ignore all exceptions, especially, EntryNotFoundException
            }
        }
    }
    
    /**
     * Clears the distributed region.
     * @param region The region to be cleared.
     * @throws CacheException
     */    
    public final static void clearRegion(Region region) throws CacheException
	{	
    	if (region == null) {
    		return;
    	}
    	
    	region.clear();
	}

    public static Region<?,?> getLocalRegion(String regionPath){
      Region<?,?> region = null;
      try{
        Cache cache = CacheFactory.getAnyInstance();
        region = cache.getRegion(regionPath);
      }catch(CacheClosedException cce){
        region = null;
      }
      return region;
    }

}
