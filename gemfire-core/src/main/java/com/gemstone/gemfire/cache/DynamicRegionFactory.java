/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.ServerRegionProxy;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.DynamicRegionAttributes;
import com.gemstone.gemfire.internal.cache.DynamicRegionFactoryImpl;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EnumListenerEvent;
import com.gemstone.gemfire.internal.cache.EvictionAttributesImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InitialImageOperation;
import com.gemstone.gemfire.internal.cache.InternalRegionArguments;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.cache.RegionEventImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.security.GemFireSecurityException;


/**
 * DynamicRegionFactory provides a distributed region creation service. 
 * Any other member of the GemFire DistributedSystem that has created 
 * an instance of this class will automatically instantiate regions created
 * through the factory from anywhere else in the DistributedSystem.
 * <p><p>
 * Instructions for Use:<br>
 * <ul>
 * <li> If your application is a client in a client/server installation,
 * either specify the pool name in the
 * {@link DynamicRegionFactory.Config} you'll use to create a
 * DynamicRegionFactory <i>or</i> specify it
 * in a dynamic-region-factory element in your cache.xml.
 *
 * <li> Before you've created a GemFire Cache in your application, add a
 * line of code as follows:<br>
 * <pre><code>  DynamicRegionFactory factory = DynamicRegionFactory.get();
 *  factory.open(config);</code></pre>
 * <pre><code>  DynamicRegionFactory myFactoryHandle = DynamicRegionFactory.get().open(config);</code></pre>
 * or just use a dynamic-region-factory element in the cache.xml.
 *
 * <li> Create the GemFire Cache.  During cache creation, the list of dynamic Regions will either be discovered
 * by recovering
 * their names from disk (see {@link DynamicRegionFactory.Config#persistBackup}) or from other members of the
 * distributed system.
 * These dynamic Regions will be created before Cache creation completes.
 *
 * <li> Thereafter, when you want to create dynamic distributed Regions,
 * create them using the {@link #createDynamicRegion}.  Regions created with the factory will
 * inherit their RegionAttributes from their parent Region, though you can override
 * callbacks when you configure the factory.
 *
 * <p>All other instances of GemFire across the distributed system that
 * instantiate and open a DynamicRegionFactory will also get the dynamic distributed Regions.
 * 
 * <li>Non-dynamic parent Regions should be declared in cache.xml so that they can be created before
 * the dynamic Region factory goes active and starts creating Regions.  You will have cache creation
 * problems if this isn't done.
 * 
 * <li>A DynamicRegionListener can be registered before open is called and before cache creation
 * so that the listener will be called if dynamic Regions are created during cache creation.
 * 
 * </ul>
 * <p>Saving the factory on disk:  
 * If {@link DynamicRegionFactory.Config#persistBackup} is configured for the factory, dynamic Region information
 * is written to disk for recovery.
 * By default the current directory is used for this information. The {@link DynamicRegionFactory.Config#diskDir}
 * can be used to change this default.
 * <p>
 * Registering interest in cache server information: The {@link DynamicRegionFactory.Config#registerInterest}
 * setting determines whether clients will register interest in server keys or not.  You will generally want
 * this to be turned on so that clients will see updates made to servers.  In server processes, DynamicRegionFactory
 * forces use of NotifyBySubscription.
 * </ul>
 * <p>
 * Notes:
 * <ul>
 * <li>DynamicRegionFactories in non-client VMs must not be configured with a pool.
 * <li>If {@link #open()} is called before cache creation and the cache.xml has a dynamic-region-factory
 * element then the cache.xml will override the open call's configuration.
 * 
 * <li>Since the RegionAttributes of a dynamically created Region are copied
 * from the parent Region, any callbacks, ({@link CacheListener},
 * {@link CacheWriter}, and {@link CacheLoader}
 * are shared by the parent and all its dynamic children
 * so make sure the callback is thread-safe and that its
 * {@link CacheCallback#close} implementation does not stop it from functioning.
 * However the products LRUAlgorithm instances will
 * be cloned so that each dynamic Region has its own callback.
 * 
 * <li>The root Region name "DynamicRegions" is reserved. The factory creates a root Region of
 * that name and uses it to keep track of what dynamic Regions exist. Applications should
 * not directly access this Region; instead use the methods on this factory.
 * </ul>
 * @since 4.3
 * @author Gideon Low
 * @author Darrel Schneider
 * @author Bruce Schuchardt
 *
 */
@SuppressWarnings("deprecation")
public abstract class DynamicRegionFactory  {

  public static final String dynamicRegionListName = "__DynamicRegions";
  private Region dynamicRegionList = null;
  /**
   * This controls the delay introduced to try and avoid any race conditions
   * between propagation of newly created Dynamic Regions
   * and the Entries put into them.
   */
  private static final long regionCreateSleepMillis = Long.getLong("DynamicRegionFactory.msDelay", 250).longValue();
  private static DynamicRegionFactory singleInstance = new DynamicRegionFactoryImpl ( );
  GemFireCacheImpl c = null;
  Config config = null;

  /** The region listeners registered on this DynamicRegionFactory */
  private static volatile List regionListeners = Collections.EMPTY_LIST;
  private static final Object regionListenerLock = new Object();

  /**
   * Opens the DynamicRegionFactory with default settings.
   */
  public void open() {
    open(new Config());
  }

  /**
   * Opens the factory with the given settings.
   * This should be sent to the factory before creating a cache.  The cache
   * will otherwise open a factory with default settings.
   * This does not need to be sent if the cache.xml declares the use of dynamic regions.
   * @param conf the configuration for this factory.
   */
  public void open(Config conf) {
    this.config = new Config(conf);
  }
  /**
   * Closes the dynamic region factory, disabling any further creation or
   * destruction of dynamic regions in this cache.
   */
  protected void _close() {
    this.config = null;
    this.c = null;
  }
  /**
   * Returns true if dynamic region factory is open; false if closed.
   */
  public boolean isOpen() {
    return getConfig() != null;
  }
  /**
   * Returns true if this factory is open and can produce dynamic regions.
   * Factories are only active after their cache has been created.
   */
  public boolean isActive() {
    return isOpen() && this.c != null;
  }
  /**
   * Returns true if dynamic region factory is closed.
   */
  public boolean isClosed() {
    return !isOpen();
  }

  /**
   * Returns the configuration for this factory.
   * Returns null if the factory is closed;
   */
  public Config getConfig() {
    if (this.config == null)
      return null;
    else
      return new Config(this.config);
  }

  public static boolean regionIsDynamicRegionList(String regionPath) {
    return regionPath != null && regionPath.equals('/' + dynamicRegionListName);
  }

  /**
   * The method is for internal use only. It is called implicitly during cache creation.
   * @param theCache The GemFire <code>Cache</code>
   * @throws CacheException
   */

  protected void _internalInit ( GemFireCacheImpl theCache ) throws CacheException
  {

    if (isClosed()) {
      // DynamicRegions are not enabled in this vm. Just return.
      return;
    }
    /**
     * This method is called internally during cache initialization at the correct time.
     * Initialize the factory with a GemFire Cache.  We create the metadata Region which holds all our
     * dynamically created regions.
     */
    try {
      this.c = theCache;
      this.dynamicRegionList = theCache.getRegion(dynamicRegionListName);
      final boolean isClient = this.config.getPoolName()!=null;
      if (this.dynamicRegionList == null) {
        InternalRegionArguments ira = new InternalRegionArguments()
        .setDestroyLockFlag(true)
        .setSnapshotInputStream(null)
        .setImageTarget(null);
        AttributesFactory af = new AttributesFactory ();       
        if (this.config.getPersistBackup()) {
          af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
          af.setDiskWriteAttributes(new DiskWriteAttributesFactory().create());
          if (this.config.getDiskDir() != null) {
            af.setDiskDirs(new File[]{this.config.getDiskDir()});
          }
        }

        if (isClient) {
          af.setScope(Scope.LOCAL);
          af.setDataPolicy(DataPolicy.NORMAL); //MirrorType(MirrorType.NONE);
          af.setStatisticsEnabled(true);
          String cpName = this.config.getPoolName();
          if (cpName != null) {
            Pool cp = PoolManager.find(cpName);
            if(cp==null)   {
              throw new IllegalStateException("Invalid pool name specified. This pool is not registered with the cache: " + cpName);
            } else {
              if (!cp.getSubscriptionEnabled()) {
                throw new IllegalStateException("The client pool of a DynamicRegionFactory must be configured with queue-enabled set to true.");
              }
              af.setPoolName(cpName);
            }
          }
          ira.setInternalMetaRegion(new LocalMetaRegion(af.create(), ira));
        } else {
          af.setScope(Scope.DISTRIBUTED_ACK);
          if (!this.config.getPersistBackup()) {  // if persistBackup, the data policy has already been set
	    af.setDataPolicy(DataPolicy.REPLICATE); //setMirrorType(MirrorType.KEYS_VALUES);
	  }

          for (GatewaySender gs : c.getGatewaySenders()) {
            if (!gs.isParallel())
              af.addGatewaySenderId(gs.getId());
          }
          ira.setInternalMetaRegion(new DistributedMetaRegion(af.create())); // bug fix 35432
        }
    
        try { 
          dynamicRegionList = theCache.createVMRegion(dynamicRegionListName,  af.create(), ira);
        }
        catch (IOException e) {
          // only if loading snapshot, not here
          InternalGemFireError assErr = new InternalGemFireError(LocalizedStrings.DynamicRegionFactory_UNEXPECTED_EXCEPTION.toLocalizedString());
          assErr.initCause(e);
          throw assErr;
        }
        catch (ClassNotFoundException e) {
          // only if loading snapshot, not here
          InternalGemFireError assErr = new InternalGemFireError(LocalizedStrings.DynamicRegionFactory_UNEXPECTED_EXCEPTION.toLocalizedString());
          assErr.initCause(e);
          throw assErr;
        }
        if (isClient) {
          dynamicRegionList.registerInterest("ALL_KEYS");
        }
        if (theCache.getLoggerI18n().fineEnabled()) {
          theCache.getLoggerI18n().fine("Created dynamic region: " + dynamicRegionList);
        }
      } else {
        if (theCache.getLoggerI18n().fineEnabled()) {
          theCache.getLoggerI18n().fine("Retrieved dynamic region: " + dynamicRegionList);
        }
      }

      createDefinedDynamicRegions ( );

    } catch ( CacheException e ) {
      //
      theCache.getLoggerI18n().warning(LocalizedStrings.DynamicRegionFactory_ERROR_INITIALIZING_DYNAMICREGIONFACTORY, e);
      throw e;
    }
  }

  /**
   *  This creates Dynamic Regions that already exist in other publishing processes
   *
   */
  private void createDefinedDynamicRegions ( ) throws CacheException {
    // TODO: perhaps add some logic here to avoid the possiblity of synchronization issues . . . .
    Set s = dynamicRegionList.entrySet( false );

    Iterator i = s.iterator();
    TreeMap sorted = new TreeMap();

    // sort by region name before creating (bug 35528)
    while ( i.hasNext() ) {
      Region.Entry e = (Region.Entry)i.next();
      DynamicRegionAttributes dda = (DynamicRegionAttributes)e.getValue();
      sorted.put(dda.rootRegionName + "/" + dda.name, dda);
    }
    i = sorted.values().iterator();
    
    while ( i.hasNext() ) {
      DynamicRegionAttributes dda = (DynamicRegionAttributes)i.next();

      doBeforeRegionCreated ( dda.rootRegionName, dda.name, null );
      Region region = createDynamicRegionImpl ( dda.rootRegionName, dda.name, false );
      doAfterRegionCreated ( region, false, false, null );

    }

  }

  /**
   * Returns the <code>DynamicRegionFactory</code> singleton instance.
   * @return the <code>DynamicRegionFactory</code> singleton instance
   */
  public static DynamicRegionFactory get() {
    return singleInstance;
  }

  /**
   * Registers a <code>DynamicRegionListener</code> for callbacks.
   * @param listener The <code>DynamicRegionListener</code> to be registered
   */
  public void registerDynamicRegionListener(DynamicRegionListener listener) {
    synchronized (regionListenerLock) {
      List oldListeners = regionListeners;
      if (!oldListeners.contains(listener)) {
        List newListeners = new ArrayList(oldListeners);
        newListeners.add(listener);
        regionListeners = newListeners;
      }
    }
  }

  /**
   * Unregisters a <code>DynamicRegionListener</code> for callbacks.
   * @param listener The <code>DynamicRegionListener</code> to be unregistered
   */
  public void unregisterDynamicRegionListener(DynamicRegionListener listener) {
    synchronized (regionListenerLock) {
      List oldListeners = regionListeners;
      if (oldListeners.contains(listener)) {
        List newListeners = new ArrayList(oldListeners);
        if (newListeners.remove(listener)) {
          regionListeners = newListeners;
        }
      }
    }
  }

  private void doBeforeRegionCreated( String parentRegion, String regionName, DistributedMember mbr ) {
    for ( Iterator i = regionListeners.iterator(); i.hasNext(); ) {
      DynamicRegionListener listener = ( DynamicRegionListener ) i.next();
      try {
        listener.beforeRegionCreate( parentRegion, regionName );
      } 
      catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error.  We're poisoned
        // now, so don't let this thread continue.
        throw err;
      }
      catch (Throwable t) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above).  However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        this.c.getLoggerI18n().warning(LocalizedStrings.DynamicRegionFactory_DYNAMICREGIONLISTENER__0__THREW_EXCEPTION_ON_BEFOREREGIONCREATED, listener, t); 
      }
    }
  }

  private void doAfterRegionCreated( Region region, boolean distributed, boolean isOriginRemote, DistributedMember mbr ) {
    RegionEvent event = new RegionEventImpl(region, Operation.REGION_CREATE, null, isOriginRemote, getMember(mbr));
    for ( Iterator i = regionListeners.iterator(); i.hasNext(); ) {
      DynamicRegionListener listener = ( DynamicRegionListener ) i.next();
      try {
        listener.afterRegionCreate( event /*region*/ );
      } 
      catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error.  We're poisoned
        // now, so don't let this thread continue.
        throw err;
      }
      catch (Throwable t) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above).  However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        this.c.getLoggerI18n().warning(LocalizedStrings.DynamicRegionFactory_DYNAMICREGIONLISTENER__0__THREW_EXCEPTION_ON_AFTERREGIONCREATED, listener, t);
      }
    }
  }

  private void doBeforeRegionDestroyed( Region region, boolean distributed, boolean isOriginRemote, boolean expiration, DistributedMember mbr ) {
    final Operation op;
    if (!distributed && !isOriginRemote) {
      op = expiration? Operation.REGION_EXPIRE_LOCAL_DESTROY : Operation.REGION_LOCAL_DESTROY;
    }
    else {
       op = expiration? Operation.REGION_EXPIRE_DESTROY : Operation.REGION_DESTROY;
    }
    RegionEvent event = new RegionEventImpl(region, op, null, isOriginRemote, getMember(mbr));
    for ( Iterator i = regionListeners.iterator(); i.hasNext(); ) {
      DynamicRegionListener listener = ( DynamicRegionListener ) i.next();
      try {
        listener.beforeRegionDestroy( event /*fullRegionName*/ );
      }
      catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error.  We're poisoned
        // now, so don't let this thread continue.
        throw err;
      }
      catch (Throwable t) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above).  However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        this.c.getLoggerI18n().warning(LocalizedStrings.DynamicRegionFactory_DYNAMICREGIONLISTENER__0__THREW_EXCEPTION_ON_BEFOREREGIONDESTROYED, listener, t);
      }
    }
  }

  private void doAfterRegionDestroyed( Region region, boolean distributed, boolean isOriginRemote, boolean expiration, DistributedMember mbr ) {
    final Operation op;
    if (!distributed && !isOriginRemote) {
      op = expiration? Operation.REGION_EXPIRE_LOCAL_DESTROY : Operation.REGION_LOCAL_DESTROY;
    }
    else {
       op = expiration? Operation.REGION_EXPIRE_DESTROY : Operation.REGION_DESTROY;
    }
    RegionEvent event = new RegionEventImpl(region, op, null, isOriginRemote, getMember(mbr));
    for ( Iterator i = regionListeners.iterator(); i.hasNext(); ) {
      DynamicRegionListener listener = ( DynamicRegionListener ) i.next();
      try {
        listener.afterRegionDestroy( event /*fullRegionName*/ );
      } 
      catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error.  We're poisoned
        // now, so don't let this thread continue.
        throw err;
      }
      catch (Throwable t) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above).  However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        this.c.getLoggerI18n().warning(LocalizedStrings.DynamicRegionFactory_DYNAMICREGIONLISTENER__0__THREW_EXCEPTION_ON_AFTERREGIONDESTROYED, listener, t);
      }
    }
  }
  
  /** return the argument, or if null the DistributedMember id of this vm */
  private DistributedMember getMember(DistributedMember mbr) {
    if (mbr == null) {
      return InternalDistributedSystem.getAnyInstance().getDistributedMember();
    }
    else {
     return null;
    }
  }
    

  /**
   * Creates the dynamic Region in the local cache and distributes the
   * creation to other caches.
   * 
   * @param parentRegionName the new region is created as a subregion of the region having this path
   * @param regionName the name of the new subregion
   * @return the <code>Region</code> created
   * @throws CacheException
   */
  public Region createDynamicRegion ( String parentRegionName, String regionName ) throws CacheException {
    if (isClosed()) {
      throw new IllegalStateException("Dynamic region factory is closed");
    }
    doBeforeRegionCreated ( parentRegionName, regionName, null );
    Region region = createDynamicRegionImpl ( parentRegionName, regionName, true );
    doAfterRegionCreated ( region, false, false, null );
    return region;
  }

  /**
   * Destroys the dynamic Region in the local cache and distributes the
   * destruction to other caches.
   * @param fullRegionName The full path of the <code>Region</code> to be
   * dynamically destroyed
   * @throws CacheException
   * @throws RegionDestroyedException if the dynamic region was never created
   * or has already been destroyed
   */
  public void destroyDynamicRegion ( String fullRegionName ) throws CacheException {    
    if (!dynamicRegionList.containsKey(fullRegionName)) {
      throw new RegionDestroyedException(LocalizedStrings.DynamicRegionFactory_DYNAMIC_REGION_0_HAS_NOT_BEEN_CREATED.toLocalizedString(fullRegionName), fullRegionName);
    }
    if (isClosed()) {
      throw new IllegalStateException("Dynamic region factory is closed");
    }

    // Retrieve the region to destroy
    Region region = c.getRegion( fullRegionName );
    if (region != null) {
      DistributedMember mbr = getMember(null);
      doBeforeRegionDestroyed ( region, false, false, false, mbr );
      // Locally destroy the region. Let the dynamicRegionList handle distributing
      // the destroy.
      region.localDestroyRegion();
      destroyDynamicRegionImpl(fullRegionName);
      doAfterRegionDestroyed ( region, false, false, false, mbr );
    } else {
      // make sure meta region is cleaned up locally and remotely
      destroyDynamicRegionImpl(fullRegionName);
    }
  }

  private Region createDynamicRegionImpl ( String parentRegionName, String newRegionName, boolean addEntry )
  throws CacheException {

    Region parentRegion = c.getRegion ( parentRegionName );
    Region newRegion = null;

    if ( parentRegion == null ) {
      String errMsg = LocalizedStrings.DynamicRegionFactory_ERROR__COULD_NOT_FIND_A_REGION_NAMED___0_.toLocalizedString(parentRegionName);
      RegionDestroyedException e = new RegionDestroyedException(errMsg, parentRegionName);
      c.getLoggerI18n().warning(LocalizedStrings.DynamicRegionFactory_ERROR__COULD_NOT_FIND_A_REGION_NAMED___0_, parentRegionName, e);
      throw e;
    }

    // Create RegionAttributes by inheriting from the parent
    RegionAttributes rra = parentRegion.getAttributes();
    RegionAttributes newRegionAttributes = null;

    AttributesFactory af = new AttributesFactory( rra );
    {
      EvictionAttributes ev = rra.getEvictionAttributes();
      if (ev != null && ev.getAlgorithm().isLRU()) {
        EvictionAttributes rev = new EvictionAttributesImpl( (EvictionAttributesImpl)ev );
        af.setEvictionAttributes( rev );
      }
    }

    // for internal testing, until partitioned regions support subclasses or
    // DynamicRegion implementation is redone to not inherit attrs from parent
    // regions [bruce]
    if (newRegionName.endsWith("_PRTEST_")) {
      af.setPartitionAttributes((new PartitionAttributesFactory()).create());
    }
    
    newRegionAttributes = af.create();

    try {
      newRegion = parentRegion.createSubregion( newRegionName, newRegionAttributes );
      c.getLoggerI18n().fine("Created dynamic region " + newRegion);
    } catch (RegionExistsException ex) {
      // a race condition exists that can cause this so just fine log it
      c.getLoggerI18n().fine("DynamicRegion " + newRegionName + " in parent " + parentRegionName + " already existed");
      newRegion = ex.getRegion();
//    } catch ( CacheException e ) {
//      c.getLoggerI18n().warning ( "Error creating new Dynamic Region '" + newRegionName, e );
//      throw e;
    }

    if (addEntry) {
      DynamicRegionAttributes dra = new DynamicRegionAttributes ( );
      dra.name = newRegionName;
      dra.rootRegionName = parentRegion.getFullPath();
      if (c.getLoggerI18n().fineEnabled()) {
        c.getLoggerI18n().fine ("Putting entry into dynamic region list at key: " + newRegion.getFullPath());
      }
      dynamicRegionList.put ( newRegion.getFullPath(), dra );
    }

    if (config.getRegisterInterest()) {
      ServerRegionProxy proxy = ((LocalRegion)newRegion).getServerProxy();
      if (proxy != null) {
        if (((Pool)proxy.getPool()).getSubscriptionEnabled()) {
          try {
            newRegion.registerInterest("ALL_KEYS");
          }
          catch (GemFireSecurityException ex) {
            // Ignore security exceptions here
            c.getSecurityLoggerI18n().warning(
              LocalizedStrings.DynamicRegionFactory_EXCEPTION_WHEN_REGISTERING_INTEREST_FOR_ALL_KEYS_IN_DYNAMIC_REGION_0_1,  
              new Object[] {newRegion.getFullPath(), ex});
          }
        }
      }
    }
    
    if (regionCreateSleepMillis > 0) {
      try {
        Thread.sleep( regionCreateSleepMillis );
      } catch ( InterruptedException e ) {
        Thread.currentThread().interrupt();
      }
    }

    if (c.getLoggerI18n().fineEnabled()) {
      c.getLoggerI18n().fine ( "Created Dynamic Region " + newRegion.getFullPath() );
    }
    return newRegion;
  }

  private void destroyDynamicRegionImpl(String fullRegionName)
  throws CacheException {
    // Destroy the entry in the dynamicRegionList
    try {
      if (c.getLoggerI18n().fineEnabled()) {
        c.getLoggerI18n().fine ("Destroying entry from dynamic region list at key: " + fullRegionName);
      }
      dynamicRegionList.destroy ( fullRegionName );
    } catch (CacheException e) {
      c.getLoggerI18n().warning(LocalizedStrings.DynamicRegionFactory_ERROR_DESTROYING_DYNAMIC_REGION__0, fullRegionName, e);
      throw e;
    }

    if (c.getLoggerI18n().fineEnabled()) {
      c.getLoggerI18n().fine ( "Destroyed Dynamic Region " + fullRegionName );
    }
  }

  /**
   * Configuration for dynamic region factory.
   * The default attributes are:
   * <ul>
   * <li>diskDir: <code>null</code>
   * <li>poolName: <code>null</code>
   * <li>persistBackup: <code>true</code>
   * <li>registerInterest: <code>true</code>
   * </ul>
   * @since 4.3
   */
  public static class Config  {
    private static final boolean DISABLE_REGISTER_INTEREST = Boolean.getBoolean("DynamicRegionFactory.disableRegisterInterest");
    private static final boolean DISABLE_PERSIST_BACKUP = Boolean.getBoolean("DynamicRegionFactory.disablePersistence");

    /** Causes the factory to be persisted on disk.  See {@link #diskDir}  */
    public final boolean persistBackup;
    /** The directory where the factory's {@link #persistBackup} files are placed */
    public final File diskDir;
    /** Causes regions created by the factory to register interest in all keys in a corresponding server cache region */
    public final boolean registerInterest;
    
    /**
     * The ${link Pool} to be used by a client factory to communicate with 
     * the server-side factory.
     */
    public final String poolName;

    /**
     * Creates a configuration with the default attributes.
     */
    public Config() {
      this(null, null, !DISABLE_PERSIST_BACKUP, !DISABLE_REGISTER_INTEREST);
    }

    /**
     * Creates a configuration with defaults and the given diskDir and poolName.
     */
    public Config(File diskDir, String poolName) {
      this(diskDir, poolName, !DISABLE_PERSIST_BACKUP, !DISABLE_REGISTER_INTEREST);
    }
    /**
     * Creates a configuration with the given attributes
     */
    public Config(
      File diskDir,
      String poolName,
      boolean persistBackup,
      boolean registerInterest)
    {
      this.registerInterest = registerInterest;
      this.persistBackup = persistBackup;
      this.diskDir = diskDir;
      this.poolName = poolName;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((diskDir == null) ? 0 : diskDir.hashCode());
      result = prime * result + (persistBackup ? 1231 : 1237);
      result = prime * result + ((poolName == null) ? 0 : poolName.hashCode());
      result = prime * result + (registerInterest ? 1231 : 1237);
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Config other = (Config) obj;
      if (diskDir == null) {
        if (other.diskDir != null)
          return false;
      } else if (!diskDir.equals(other.diskDir))
        return false;
      if (persistBackup != other.persistBackup)
        return false;
      if (poolName == null) {
        if (other.poolName != null)
          return false;
      } else if (!poolName.equals(other.poolName))
        return false;
      if (registerInterest != other.registerInterest)
        return false;
      return true;
    }

    /**
     * Returns true if the factory is persisted to disk; false if not.
     */
    public boolean getPersistBackup() {
      return this.persistBackup;
    }
    
    /**
     * Returns true if the region will register interest in all keys of a corresponding
     * server cache region
     */
    public boolean getRegisterInterest() {
      return this.registerInterest;
    }
    
    /**
     * Returns the disk directory that the dynamic region factory data
     * will be written to.
     * Returns null if no directory has been specified.
     * The diskDir is only used if <code>persistBackup</code> is true.
     */
    public File getDiskDir() {
      return this.diskDir;
    }
    
    
    /**
     * Returns the name of the {@link Pool} associated with the dynamic region factory.
     * Returns null if there is no connection pool for dynamic regions.
     */
    public String getPoolName() {
      return this.poolName;
    }
    
    /** create a new Config with settings from another one */
    Config(Config conf) {
      this.diskDir = conf.diskDir;
      this.persistBackup = conf.persistBackup;
      this.registerInterest = conf.registerInterest;
      this.poolName = conf.poolName;
    }
  }
  
  protected void buildDynamicRegion(EntryEvent event) {
    if (!DynamicRegionFactory.this.isOpen())
      return;
    
    // Ignore the callback if it originated in this process (because the region
    // will already have been created) and the event is not a client event
    if ( !event.isOriginRemote() && !event.isBridgeEvent() ) return;
    //
    DynamicRegionAttributes dra = (DynamicRegionAttributes)event.getNewValue();
    String parentRegionName = dra.rootRegionName;
    String newRegionName = dra.name;

    try {
      doBeforeRegionCreated ( parentRegionName, newRegionName, event.getDistributedMember() );
      Region region = createDynamicRegionImpl ( parentRegionName, newRegionName, false);
      doAfterRegionCreated ( region, true, true, event.getDistributedMember() );
    } catch ( Exception e ) {
      c.getLoggerI18n().warning(LocalizedStrings.DynamicRegionFactory_ERROR_ATTEMPTING_TO_LOCALLY_CREATE_DYNAMIC_REGION__0, newRegionName, e);
    }
  }
  
  protected void razeDynamicRegion(EntryEvent event) {
    if (!DynamicRegionFactory.this.isOpen())
      return;

    // Because CacheClientUpdater calls localDestroy we need to allow
    // "local" events. If this is a true local then c.getRegion will return
    // null and this code will do nothing.
    // When bug 35644 fixed the following "if" can be uncommented.
//     // Ignore the callback if it originated in this process (because the region
//     // will already have been destroyed)
//     if ( !event.isOriginRemote() && !(event instanceof BridgeEntryEventImpl)) return;

    String fullRegionName = ( String ) event.getKey();
    Region drRegion = c.getRegion( fullRegionName );
    if (drRegion != null) {
      try {
        doBeforeRegionDestroyed ( drRegion, true, event.getOperation().isDistributed(), event.getOperation().isExpiration(), event.getDistributedMember() );
        drRegion.localDestroyRegion();
        doAfterRegionDestroyed ( drRegion, true, event.getOperation().isDistributed(), event.getOperation().isExpiration(), event.getDistributedMember() );
      } catch ( Exception e ) {
        c.getLoggerI18n().warning(LocalizedStrings.DynamicRegionFactory_ERROR_ATTEMPTING_TO_LOCALLY_DESTROY_DYNAMIC_REGION__0, fullRegionName, e);
      }
    }
  }
  
//  private class DRListener implements CacheListener {
//    public void afterCreate(EntryEvent arg0) {
//      buildDynamicRegion(arg0);
//    }
//
//    public void afterDestroy(EntryEvent arg0) {
//      razeDynamicRegion(arg0);
//    }
//
//    public void afterInvalidate(EntryEvent arg0) {
//      // Stub, nothing to do.
//    }
//
//    public void afterRegionDestroy(RegionEvent arg0) {
//      // Stub, nothing to do.
//    }
//
//    public void afterRegionInvalidate(RegionEvent arg0) {
//      // Stub, nothing to do.
//    }
//
//    public void afterUpdate(EntryEvent arg0) {
//      // Stub, nothing to do.
//    }
//
//    public void close() {
//      // Stub, nothing to do.
//    }
//  }
  
  // Introduced to keep symmetry with DistributedMetaRegion and potentially provide improved control of
  // the meta data
  private class LocalMetaRegion extends LocalRegion  {
    protected LocalMetaRegion(RegionAttributes attrs, InternalRegionArguments ira) {
      super(dynamicRegionListName, attrs, null, DynamicRegionFactory.this.c, ira);
      Assert.assertTrue(attrs.getScope().isLocal());
    }
    
    // This is an internal uses only region
    @Override
    protected boolean isSecret()
    {
      return true;
    }

    @Override
    protected boolean isCopyOnRead() {
      return false;
    }

//    //@override event tracker not needed for this type of region
//    void initEventTracker() {
//    }

    // while internal, its contents should be communicated with bridge clients 
    @Override
    protected boolean shouldNotifyBridgeClients()
    {
      return getCache().getCacheServers().size() > 0;
    }

    // Over-ride the super behavior to perform the destruction of the dynamic region
    @Override
    public void invokeDestroyCallbacks(EnumListenerEvent eventType, EntryEventImpl event, boolean callDispatchEventsCallback, boolean notifyGateways )
    {
      Assert.assertTrue(eventType.equals(EnumListenerEvent.AFTER_DESTROY));
      // Notify bridge clients (if this is a BridgeServer)
      event.setEventType(eventType);
      notifyBridgeClients(event);
      // Notify GatewayHub (if this region participates in a Gateway)
      if (notifyGateways) {
        notifyGatewaySender(eventType, event);
      }
      // Destroy the dynamic region after the clients and gateways have been notified
      razeDynamicRegion(event);
    }

    // Over-ride the super behavior to perform the creation of the dynamic region
    @Override
    protected long basicPutPart2(EntryEventImpl event, RegionEntry entry,
        boolean isInitialized, long lastModified,
        boolean clearConflict)  {
    
    boolean isCreate = event.getOperation().isCreate();
    boolean set = false;
    
    if (isCreate && !event.callbacksInvoked()) {
      // don't notify clients until all peers have created the region so that
      // their register-interest operations will be sure to find data
      event.callbacksInvoked(true);
      set = true;
    }
    
    long result = super.basicPutPart2(event, entry, isInitialized, lastModified, clearConflict);
    
    if (set) {
      event.callbacksInvoked(false);
    }
    
    if (isCreate) {
      buildDynamicRegion(event);
    }
    return result;
  }
  
    // The dynamic-region meta-region needs to tell clients about the event
    // after all servers have created the region so that register-interest
    // will work correctly
    @Override
    public void basicPutPart3(EntryEventImpl event, RegionEntry entry,
        boolean isInitialized, long lastModified, boolean invokeCallbacks,
        boolean ifNew, boolean ifOld, Object expectedOldValue,
        boolean requireOldValue) {
      
      super.basicPutPart3(event, entry, isInitialized, lastModified, invokeCallbacks, ifNew, ifOld, expectedOldValue, requireOldValue);
      
      // this code is copied from LocalRegion.basicPutPart2
      invokeCallbacks &= !entry.isTombstone(); // put() is creating a tombstone
      if (invokeCallbacks) {
        boolean doCallback = false;
        if (isInitialized) {
          if (event.isGenerateCallbacks()) {
            doCallback = true;
          }
        }
        if (doCallback) {
          notifyGatewaySender(event.getOperation().isUpdate()? EnumListenerEvent.AFTER_UPDATE
                                               : EnumListenerEvent.AFTER_CREATE, event);
          // Notify listeners
          if (!event.isBulkOpInProgress()) {
            try {
              entry.dispatchListenerEvents(event);
            }
            catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              stopper.checkCancelInProgress(null);
            }
          }
        }
      }
    }   
  }

  // Part of the fix for bug 35432, which required a change to the 
  // distribution and notification order on the BridgeServer
  private class DistributedMetaRegion extends DistributedRegion  {
    protected DistributedMetaRegion(RegionAttributes attrs) {
      super(dynamicRegionListName, attrs, null, DynamicRegionFactory.this.c, new InternalRegionArguments());
    }
    // This is an internal uses only region
    @Override
    protected boolean isSecret()
    {
      return true;
    }
    
//    //@override event tracker not needed for this type of region
//    void initEventTracker() {
//    }

    @Override
    protected boolean isCopyOnRead() {
      return false;
    }

    // while internal, its contents should be communicated with bridge clients 
    @Override
    final public boolean shouldNotifyBridgeClients()
    {
      return getCache().getCacheServers().size() > 0;
    }    
   
    // Over-ride the super behavior to perform the destruction of the dynamic region
    // 
    @Override
    public void invokeDestroyCallbacks(EnumListenerEvent eventType, EntryEventImpl event, boolean callDispatchEventsCallback, boolean notifyGateways)
    {
      Assert.assertTrue(eventType.equals(EnumListenerEvent.AFTER_DESTROY));
      // Notify bridge clients (if this is a BridgeServer)
      event.setEventType(eventType);
      notifyBridgeClients(event);
      // Notify GatewayHub (if this region participates in a Gateway)
      if (notifyGateways) {
        notifyGatewaySender(eventType, event);
      }
      // Destroy the dynamic region after the clients and gateways have been notified
      razeDynamicRegion(event);
    }

    @Override
    protected long basicPutPart2(EntryEventImpl event, RegionEntry entry,
        boolean isInitialized, long lastModified,
        boolean clearConflict)
    {
      boolean isCreate = event.getOperation().isCreate();
      boolean set = false;
      if (isCreate && !event.callbacksInvoked()) {
        // don't notify clients until all peers have created the region so that
        // their register-interest operations will be sure to find data
        event.callbacksInvoked(true);
        set = true;
      }
      
      long result = super.basicPutPart2(event, entry, isInitialized, lastModified, clearConflict);
      
      if (set) {
        event.callbacksInvoked(false);
      }
      
      if (isCreate) {
        try {
          InitialImageOperation.setInhibitStateFlush(true); // fix for bug 36175
          buildDynamicRegion(event);
        }
        finally {
          InitialImageOperation.setInhibitStateFlush(false);
        }
      }
      return result;
    }
    
    // The dynamic-region meta-region needs to tell clients about the event
    // after all servers have created the region so that register-interest
    // will work correctly
    @Override
    public void basicPutPart3(EntryEventImpl event, RegionEntry entry,
        boolean isInitialized, long lastModified, boolean invokeCallbacks,
        boolean ifNew, boolean ifOld, Object expectedOldValue,
        boolean requireOldValue) {

      super.basicPutPart3(event, entry, isInitialized, lastModified, invokeCallbacks, ifNew, ifOld, expectedOldValue, requireOldValue);
      
      // this code is copied from LocalRegion.basicPutPart2
      invokeCallbacks &= !entry.isTombstone(); // put() is creating a tombstone
      if (invokeCallbacks) {
        boolean doCallback = false;
        if (isInitialized) {
          if (event.isGenerateCallbacks()) {
            doCallback = true;
          }
        }
        if (doCallback) {
          notifyGatewaySender(event.getOperation().isUpdate()? EnumListenerEvent.AFTER_UPDATE
                                               : EnumListenerEvent.AFTER_CREATE, event);
          // Notify listeners
          if (!event.isBulkOpInProgress()) {
            try {
              entry.dispatchListenerEvents(event);
            }
            catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              stopper.checkCancelInProgress(null);
            }
          }
        }
      }
    }

  }

}
