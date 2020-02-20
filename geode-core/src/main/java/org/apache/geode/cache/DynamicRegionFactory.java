/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.cache;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.ServerRegionProxy;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.DynamicRegionAttributes;
import org.apache.geode.internal.cache.DynamicRegionFactoryImpl;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.EvictionAttributesImpl;
import org.apache.geode.internal.cache.InitialImageOperation;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.InternalRegionFactory;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.RegionEventImpl;
import org.apache.geode.security.GemFireSecurityException;

/**
 * DynamicRegionFactory provides a distributed region creation service. Any other member of the
 * GemFire DistributedSystem that has created an instance of this class will automatically
 * instantiate regions created through the factory from anywhere else in the DistributedSystem.
 * <p>
 * <p>
 * Instructions for Use:<br>
 * <ul>
 * <li>If your application is a client in a client/server installation, either specify the pool name
 * in the {@link DynamicRegionFactory.Config} you'll use to create a DynamicRegionFactory <i>or</i>
 * specify it in a dynamic-region-factory element in your cache.xml.
 *
 * <li>Before you've created a GemFire Cache in your application, add a line of code as follows:<br>
 *
 * <pre>
 * {
 *   DynamicRegionFactory factory = DynamicRegionFactory.get();
 *   factory.open(config);
 * }
 * </pre>
 *
 * <pre>
 * {
 *   DynamicRegionFactory myFactoryHandle = DynamicRegionFactory.get().open(config);
 * }
 * </pre>
 *
 * or just use a dynamic-region-factory element in the cache.xml.
 *
 * <li>Create the GemFire Cache. During cache creation, the list of dynamic Regions will either be
 * discovered by recovering their names from disk (see
 * {@link DynamicRegionFactory.Config#persistBackup}) or from other members of the distributed
 * system. These dynamic Regions will be created before Cache creation completes.
 *
 * <li>Thereafter, when you want to create dynamic distributed Regions, create them using the
 * {@link #createDynamicRegion}. Regions created with the factory will inherit their
 * RegionAttributes from their parent Region, though you can override callbacks when you configure
 * the factory.
 *
 * <p>
 * All other instances of GemFire across the distributed system that instantiate and open a
 * DynamicRegionFactory will also get the dynamic distributed Regions.
 *
 * <li>Non-dynamic parent Regions should be declared in cache.xml so that they can be created before
 * the dynamic Region factory goes active and starts creating Regions. You will have cache creation
 * problems if this isn't done.
 *
 * <li>A DynamicRegionListener can be registered before open is called and before cache creation so
 * that the listener will be called if dynamic Regions are created during cache creation.
 *
 * </ul>
 * <p>
 * Saving the factory on disk: If {@link DynamicRegionFactory.Config#persistBackup} is configured
 * for the factory, dynamic Region information is written to disk for recovery. By default the
 * current directory is used for this information. The {@link DynamicRegionFactory.Config#diskDir}
 * can be used to change this default.
 * <p>
 * Registering interest in cache server information: The
 * {@link DynamicRegionFactory.Config#registerInterest} setting determines whether clients will
 * register interest in server keys or not. You will generally want this to be turned on so that
 * clients will see updates made to servers. In server processes, DynamicRegionFactory forces use of
 * NotifyBySubscription.
 * </ul>
 * <p>
 * Notes:
 * <ul>
 * <li>DynamicRegionFactories in non-client VMs must not be configured with a pool.
 * <li>If {@link #open()} is called before cache creation and the cache.xml has a
 * dynamic-region-factory element then the cache.xml will override the open call's configuration.
 *
 * <li>Since the RegionAttributes of a dynamically created Region are copied from the parent Region,
 * any callbacks, ({@link CacheListener}, {@link CacheWriter}, and {@link CacheLoader} are shared by
 * the parent and all its dynamic children so make sure the callback is thread-safe and that its
 * {@link CacheCallback#close} implementation does not stop it from functioning. However the
 * products EvictionAlgorithm instances will be cloned so that each dynamic Region has its own
 * callback.
 *
 * <li>The root Region name "DynamicRegions" is reserved. The factory creates a root Region of that
 * name and uses it to keep track of what dynamic Regions exist. Applications should not directly
 * access this Region; instead use the methods on this factory.
 * </ul>
 *
 * @since GemFire 4.3
 * @deprecated This class is deprecated. Use {@link FunctionService} to create regions on other
 *             members instead.
 */
@SuppressWarnings("deprecation")
@Deprecated
public abstract class DynamicRegionFactory {

  public static final String DYNAMIC_REGION_LIST_NAME = "__DynamicRegions";

  private Region dynamicRegionList = null;

  /**
   * This controls the delay introduced to try and avoid any race conditions between propagation of
   * newly created Dynamic Regions and the Entries put into them.
   */
  private static final long regionCreateSleepMillis =
      Long.getLong("DynamicRegionFactory.msDelay", 250);

  @MakeNotStatic
  private static final DynamicRegionFactory singleInstance = new DynamicRegionFactoryImpl();

  InternalCache cache = null;

  private Config config = null;

  /** The region listeners registered on this DynamicRegionFactory */
  @MakeNotStatic
  private static volatile List regionListeners = Collections.emptyList();

  private static final Object regionListenerLock = new Object();

  /**
   * Opens the DynamicRegionFactory with default settings.
   */
  public void open() {
    open(new Config());
  }

  /**
   * Opens the factory with the given settings. This should be sent to the factory before creating a
   * cache. The cache will otherwise open a factory with default settings. This does not need to be
   * sent if the cache.xml declares the use of dynamic regions.
   *
   * @param conf the configuration for this factory.
   */
  public void open(Config conf) {
    this.config = new Config(conf);
  }

  /**
   * Closes the dynamic region factory, disabling any further creation or destruction of dynamic
   * regions in this cache.
   */
  protected void doClose() {
    this.config = null;
    this.cache = null;
  }

  /**
   * Returns true if dynamic region factory is open; false if closed.
   */
  public boolean isOpen() {
    return getConfig() != null;
  }

  /**
   * Returns true if this factory is open and can produce dynamic regions. Factories are only active
   * after their cache has been created.
   */
  public boolean isActive() {
    return isOpen() && this.cache != null;
  }

  /**
   * Returns true if dynamic region factory is closed.
   */
  public boolean isClosed() {
    return !isOpen();
  }

  /**
   * Returns the configuration for this factory. Returns null if the factory is closed;
   */
  public Config getConfig() {
    if (this.config == null)
      return null;
    else
      return new Config(this.config);
  }

  public static boolean regionIsDynamicRegionList(String regionPath) {
    return regionPath != null && regionPath.equals('/' + DYNAMIC_REGION_LIST_NAME);
  }

  /**
   * The method is for internal use only. It is called implicitly during cache creation.
   * <p>
   * This method is called internally during cache initialization at the correct time. Initialize
   * the factory with a GemFire Cache. We create the metadata Region which holds all our dynamically
   * created regions.
   *
   * @param theCache The GemFire {@code Cache}
   */
  protected void doInternalInit(InternalCache theCache) throws CacheException {
    if (isClosed()) {
      // DynamicRegions are not enabled in this vm. Just return.
      return;
    }

    try {
      this.cache = theCache;
      this.dynamicRegionList = theCache.getRegion(DYNAMIC_REGION_LIST_NAME);
      final boolean isClient = this.config.getPoolName() != null;
      if (this.dynamicRegionList == null) {
        InternalRegionFactory factory = cache.createInternalRegionFactory();
        factory.setDestroyLockFlag(true).setInternalRegion(true).setSnapshotInputStream(null)
            .setImageTarget(null);
        if (this.config.getPersistBackup()) {
          factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
          factory.setDiskWriteAttributes(new DiskWriteAttributesFactory().create());
          if (this.config.getDiskDir() != null) {
            factory.setDiskDirs(new File[] {this.config.getDiskDir()});
          }
        }

        if (isClient) {
          factory.setScope(Scope.LOCAL);
          factory.setDataPolicy(DataPolicy.NORMAL);
          factory.setStatisticsEnabled(true);
          String cpName = this.config.getPoolName();
          if (cpName != null) {
            Pool cp = PoolManager.find(cpName);
            if (cp == null) {
              throw new IllegalStateException(
                  "Invalid pool name specified. This pool is not registered with the cache: "
                      + cpName);
            } else {
              if (!cp.getSubscriptionEnabled()) {
                throw new IllegalStateException(
                    "The client pool of a DynamicRegionFactory must be configured with queue-enabled set to true.");
              }
              factory.setPoolName(cpName);
            }
          }
          factory.setInternalMetaRegion(new LocalMetaRegion(factory.getCreateAttributes(),
              factory.getInternalRegionArguments()));
        } else {
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setValueConstraint(DynamicRegionAttributes.class);

          if (!this.config.getPersistBackup()) { // if persistBackup, the data policy has already
                                                 // been set
            factory.setDataPolicy(DataPolicy.REPLICATE);
          }

          for (GatewaySender gs : this.cache.getGatewaySenders()) {
            if (!gs.isParallel())
              factory.addGatewaySenderId(gs.getId());
          }
          factory.setInternalMetaRegion(new DistributedMetaRegion(factory.getCreateAttributes()));
        }
        this.dynamicRegionList = factory.create(DYNAMIC_REGION_LIST_NAME);
        if (isClient) {
          this.dynamicRegionList.registerInterest("ALL_KEYS");
        }
        if (theCache.getLogger().fineEnabled()) {
          theCache.getLogger().fine("Created dynamic region: " + this.dynamicRegionList);
        }
      } else {
        if (theCache.getLogger().fineEnabled()) {
          theCache.getLogger().fine("Retrieved dynamic region: " + this.dynamicRegionList);
        }
      }

      createDefinedDynamicRegions();

    } catch (CacheException e) {
      theCache.getLogger().warning(
          "Error initializing DynamicRegionFactory", e);
      throw e;
    }
  }

  /**
   * This creates Dynamic Regions that already exist in other publishing processes
   */
  private void createDefinedDynamicRegions() throws CacheException {
    // TODO: perhaps add some logic here to avoid the possibility of synchronization issues
    Set set = this.dynamicRegionList.entrySet(false);

    Iterator iterator = set.iterator();
    SortedMap sorted = new TreeMap();

    // sort by region name before creating (bug 35528)
    while (iterator.hasNext()) {
      Region.Entry e = (Region.Entry) iterator.next();
      DynamicRegionAttributes dda = (DynamicRegionAttributes) e.getValue();
      sorted.put(dda.rootRegionName + '/' + dda.name, dda);
    }
    iterator = sorted.values().iterator();

    while (iterator.hasNext()) {
      DynamicRegionAttributes dda = (DynamicRegionAttributes) iterator.next();

      doBeforeRegionCreated(dda.rootRegionName, dda.name);
      Region region = createDynamicRegionImpl(dda.rootRegionName, dda.name, false);
      doAfterRegionCreated(region, false, false, null);
    }
  }

  /**
   * Returns the {@code DynamicRegionFactory} singleton instance.
   *
   * @return the {@code DynamicRegionFactory} singleton instance
   */
  public static DynamicRegionFactory get() {
    return singleInstance;
  }

  /**
   * Registers a {@code DynamicRegionListener} for callbacks.
   *
   * @param listener The {@code DynamicRegionListener} to be registered
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
   * Unregisters a {@code DynamicRegionListener} for callbacks.
   *
   * @param listener The {@code DynamicRegionListener} to be unregistered
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

  private void doBeforeRegionCreated(String parentRegion, String regionName) {
    for (Object regionListener : regionListeners) {
      DynamicRegionListener listener = (DynamicRegionListener) regionListener;
      try {
        listener.beforeRegionCreate(parentRegion, regionName);
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Throwable t) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        this.cache.getLogger().warning(
            String.format("DynamicRegionListener %s threw exception on beforeRegionCreated",
                listener),
            t);
      }
    }
  }

  private void doAfterRegionCreated(Region region, boolean distributed, boolean isOriginRemote,
      DistributedMember mbr) {
    RegionEvent event =
        new RegionEventImpl(region, Operation.REGION_CREATE, null, isOriginRemote, getMember(mbr));
    for (Object regionListener : regionListeners) {
      DynamicRegionListener listener = (DynamicRegionListener) regionListener;
      try {
        listener.afterRegionCreate(event /* region */);
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Throwable t) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        this.cache.getLogger().warning(
            String.format("DynamicRegionListener %s threw exception on afterRegionCreated",
                listener),
            t);
      }
    }
  }

  private void doBeforeRegionDestroyed(Region region, boolean distributed, boolean isOriginRemote,
      boolean expiration, DistributedMember mbr) {
    final Operation op;
    if (!distributed && !isOriginRemote) {
      op = expiration ? Operation.REGION_EXPIRE_LOCAL_DESTROY : Operation.REGION_LOCAL_DESTROY;
    } else {
      op = expiration ? Operation.REGION_EXPIRE_DESTROY : Operation.REGION_DESTROY;
    }
    RegionEvent event = new RegionEventImpl(region, op, null, isOriginRemote, getMember(mbr));
    for (Object regionListener : regionListeners) {
      DynamicRegionListener listener = (DynamicRegionListener) regionListener;
      try {
        listener.beforeRegionDestroy(event /* fullRegionName */);
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Throwable t) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        this.cache.getLogger().warning(
            String.format("DynamicRegionListener %s threw exception on beforeRegionDestroyed",
                listener),
            t);
      }
    }
  }

  private void doAfterRegionDestroyed(Region region, boolean distributed, boolean isOriginRemote,
      boolean expiration, DistributedMember mbr) {
    final Operation op;
    if (!distributed && !isOriginRemote) {
      op = expiration ? Operation.REGION_EXPIRE_LOCAL_DESTROY : Operation.REGION_LOCAL_DESTROY;
    } else {
      op = expiration ? Operation.REGION_EXPIRE_DESTROY : Operation.REGION_DESTROY;
    }
    RegionEvent event = new RegionEventImpl(region, op, null, isOriginRemote, getMember(mbr));
    for (Object regionListener : regionListeners) {
      DynamicRegionListener listener = (DynamicRegionListener) regionListener;
      try {
        listener.afterRegionDestroy(event /* fullRegionName */);
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Throwable t) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        this.cache.getLogger().warning(
            String.format("DynamicRegionListener %s threw exception on afterRegionDestroyed",
                listener),
            t);
      }
    }
  }

  /** return the argument, or if null the DistributedMember id of this vm */
  private DistributedMember getMember(DistributedMember mbr) {
    if (mbr == null) {
      return this.cache.getInternalDistributedSystem().getDistributedMember();
    } else {
      return null;
    }
  }

  /**
   * Creates the dynamic Region in the local cache and distributes the creation to other caches.
   *
   * @param parentRegionName the new region is created as a subregion of the region having this path
   * @param regionName the name of the new subregion
   * @return the {@code Region} created
   */
  public Region createDynamicRegion(String parentRegionName, String regionName)
      throws CacheException {
    if (isClosed()) {
      throw new IllegalStateException("Dynamic region factory is closed");
    }
    doBeforeRegionCreated(parentRegionName, regionName);
    Region region = createDynamicRegionImpl(parentRegionName, regionName, true);
    doAfterRegionCreated(region, false, false, null);
    return region;
  }

  /**
   * Destroys the dynamic Region in the local cache and distributes the destruction to other caches.
   *
   * @param fullRegionName The full path of the {@code Region} to be dynamically destroyed
   * @throws RegionDestroyedException if the dynamic region was never created or has already been
   *         destroyed
   */
  public void destroyDynamicRegion(String fullRegionName) throws CacheException {
    if (!this.dynamicRegionList.containsKey(fullRegionName)) {
      throw new RegionDestroyedException(
          String.format("Dynamic region %s has not been created.",
              fullRegionName),
          fullRegionName);
    }
    if (isClosed()) {
      throw new IllegalStateException("Dynamic region factory is closed");
    }

    // Retrieve the region to destroy
    Region region = this.cache.getRegion(fullRegionName);
    if (region != null) {
      DistributedMember mbr = getMember(null);
      doBeforeRegionDestroyed(region, false, false, false, mbr);
      // Locally destroy the region. Let the dynamicRegionList handle distributing
      // the destroy.
      region.localDestroyRegion();
      destroyDynamicRegionImpl(fullRegionName);
      doAfterRegionDestroyed(region, false, false, false, mbr);
    } else {
      // make sure meta region is cleaned up locally and remotely
      destroyDynamicRegionImpl(fullRegionName);
    }
  }

  private Region createDynamicRegionImpl(String parentRegionName, String newRegionName,
      boolean addEntry) throws CacheException {

    Region parentRegion = this.cache.getRegion(parentRegionName);

    if (parentRegion == null) {
      String errMsg =
          String.format("Error -- Could not find a region named: '%s'",
              parentRegionName);
      RegionDestroyedException e = new RegionDestroyedException(errMsg, parentRegionName);
      this.cache.getLogger().warning(
          String.format("Error -- Could not find a region named: '%s'",
              parentRegionName),
          e);
      throw e;
    }

    // Create RegionAttributes by inheriting from the parent
    RegionAttributes rra = parentRegion.getAttributes();

    AttributesFactory af = new AttributesFactory(rra);
    EvictionAttributes ev = rra.getEvictionAttributes();
    if (ev != null && ev.getAlgorithm().isLRU()) {
      EvictionAttributes rev = new EvictionAttributesImpl(ev);
      af.setEvictionAttributes(rev);
    }

    // for internal testing, until partitioned regions support subclasses or
    // DynamicRegion implementation is redone to not inherit attrs from parent
    // regions
    if (newRegionName.endsWith("_PRTEST_")) {
      af.setPartitionAttributes(new PartitionAttributesFactory().create());
    }

    RegionAttributes newRegionAttributes = af.create();

    Region newRegion;
    try {
      newRegion = parentRegion.createSubregion(newRegionName, newRegionAttributes);
      this.cache.getLogger().fine("Created dynamic region " + newRegion);
    } catch (RegionExistsException ex) {
      // a race condition exists that can cause this so just fine log it
      this.cache.getLogger().fine(
          "DynamicRegion " + newRegionName + " in parent " + parentRegionName + " already existed");
      newRegion = ex.getRegion();
    }

    if (addEntry) {
      DynamicRegionAttributes dra = new DynamicRegionAttributes();
      dra.name = newRegionName;
      dra.rootRegionName = parentRegion.getFullPath();
      if (this.cache.getLogger().fineEnabled()) {
        this.cache.getLogger()
            .fine("Putting entry into dynamic region list at key: " + newRegion.getFullPath());
      }
      this.dynamicRegionList.put(newRegion.getFullPath(), dra);
    }

    if (this.config.getRegisterInterest()) {
      ServerRegionProxy proxy = ((InternalRegion) newRegion).getServerProxy();
      if (proxy != null) {
        if (((Pool) proxy.getPool()).getSubscriptionEnabled()) {
          try {
            newRegion.registerInterest("ALL_KEYS");
          } catch (GemFireSecurityException ex) {
            // Ignore security exceptions here
            this.cache.getSecurityLogger().warning(
                String.format(
                    "Exception when registering interest for all keys in dynamic region [%s]. %s",
                    new Object[] {newRegion.getFullPath(), ex}));
          }
        }
      }
    }

    if (regionCreateSleepMillis > 0) {
      try {
        Thread.sleep(regionCreateSleepMillis);
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
      }
    }

    if (this.cache.getLogger().fineEnabled()) {
      this.cache.getLogger().fine("Created Dynamic Region " + newRegion.getFullPath());
    }
    return newRegion;
  }

  private void destroyDynamicRegionImpl(String fullRegionName) throws CacheException {
    // Destroy the entry in the dynamicRegionList
    try {
      if (this.cache.getLogger().fineEnabled()) {
        this.cache.getLogger()
            .fine("Destroying entry from dynamic region list at key: " + fullRegionName);
      }
      this.dynamicRegionList.destroy(fullRegionName);
    } catch (CacheException e) {
      this.cache.getLogger().warning(
          String.format("Error destroying Dynamic Region '%s'", fullRegionName),
          e);
      throw e;
    }

    if (this.cache.getLogger().fineEnabled()) {
      this.cache.getLogger().fine("Destroyed Dynamic Region " + fullRegionName);
    }
  }

  /**
   * Configuration for dynamic region factory. The default attributes are:
   * <ul>
   * <li>diskDir: {@code null}
   * <li>poolName: {@code null}
   * <li>persistBackup: {@code true}
   * <li>registerInterest: {@code true}
   * </ul>
   *
   * @since GemFire 4.3
   */
  public static class Config {
    private static final boolean DISABLE_REGISTER_INTEREST =
        Boolean.getBoolean("DynamicRegionFactory.disableRegisterInterest");
    private static final boolean DISABLE_PERSIST_BACKUP =
        Boolean.getBoolean("DynamicRegionFactory.disablePersistence");

    /** Causes the factory to be persisted on disk. See {@link #diskDir} */
    public final boolean persistBackup;
    /** The directory where the factory's {@link #persistBackup} files are placed */
    public final File diskDir;
    /**
     * Causes regions created by the factory to register interest in all keys in a corresponding
     * server cache region
     */
    public final boolean registerInterest;

    /**
     * The ${link Pool} to be used by a client factory to communicate with the server-side factory.
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
    public Config(File diskDir, String poolName, boolean persistBackup, boolean registerInterest) {
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
     * Returns true if the region will register interest in all keys of a corresponding server cache
     * region
     */
    public boolean getRegisterInterest() {
      return this.registerInterest;
    }

    /**
     * Returns the disk directory that the dynamic region factory data will be written to. Returns
     * null if no directory has been specified. The diskDir is only used if {@code persistBackup} is
     * true.
     */
    public File getDiskDir() {
      return this.diskDir;
    }


    /**
     * Returns the name of the {@link Pool} associated with the dynamic region factory. Returns null
     * if there is no connection pool for dynamic regions.
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
    if (!event.isOriginRemote() && !event.hasClientOrigin())
      return;
    //
    DynamicRegionAttributes dra = (DynamicRegionAttributes) event.getNewValue();
    String parentRegionName = dra.rootRegionName;
    String newRegionName = dra.name;

    try {
      doBeforeRegionCreated(parentRegionName, newRegionName);
      Region region = createDynamicRegionImpl(parentRegionName, newRegionName, false);
      doAfterRegionCreated(region, true, true, event.getDistributedMember());
    } catch (Exception e) {
      cache.getLogger().warning(
          String.format("Error attempting to locally create Dynamic Region: %s",
              newRegionName),
          e);
    }
  }

  protected void razeDynamicRegion(EntryEvent event) {
    if (!DynamicRegionFactory.this.isOpen())
      return;

    // Because CacheClientUpdater calls localDestroy we need to allow
    // "local" events. If this is a true local then c.getRegion will return
    // null and this code will do nothing.
    // When bug 35644 fixed the following "if" can be uncommented.

    // Ignore the callback if it originated in this process (because the region
    // will already have been destroyed)

    String fullRegionName = (String) event.getKey();
    Region drRegion = cache.getRegion(fullRegionName);
    if (drRegion != null) {
      try {
        doBeforeRegionDestroyed(drRegion, true, event.getOperation().isDistributed(),
            event.getOperation().isExpiration(), event.getDistributedMember());
        drRegion.localDestroyRegion();
        doAfterRegionDestroyed(drRegion, true, event.getOperation().isDistributed(),
            event.getOperation().isExpiration(), event.getDistributedMember());
      } catch (Exception e) {
        cache.getLogger().warning(
            String.format("Error attempting to locally destroy Dynamic Region: %s",
                fullRegionName),
            e);
      }
    }
  }

  // Introduced to keep symmetry with DistributedMetaRegion and potentially provide improved control
  // of
  // the meta data
  private class LocalMetaRegion extends LocalRegion {
    protected LocalMetaRegion(RegionAttributes attrs, InternalRegionArguments ira) {
      super(DYNAMIC_REGION_LIST_NAME, attrs, null, DynamicRegionFactory.this.cache, ira,
          DynamicRegionFactory.this.cache.getStatisticsClock());
      Assert.assertTrue(attrs.getScope().isLocal());
    }

    // This is an internal uses only region
    @Override
    public boolean isSecret() {
      return true;
    }

    @Override
    public boolean isCopyOnRead() {
      return false;
    }

    // //@override event tracker not needed for this type of region
    // void initEventTracker() {
    // }

    // while internal, its contents should be communicated with bridge clients
    @Override
    public boolean shouldNotifyBridgeClients() {
      return getCache().getCacheServers().size() > 0;
    }

    // Over-ride the super behavior to perform the destruction of the dynamic region
    @Override
    public void invokeDestroyCallbacks(EnumListenerEvent eventType, EntryEventImpl event,
        boolean callDispatchEventsCallback, boolean notifyGateways) {
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
    public long basicPutPart2(EntryEventImpl event, RegionEntry entry, boolean isInitialized,
        long lastModified, boolean clearConflict) {

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
    public void basicPutPart3(EntryEventImpl event, RegionEntry entry, boolean isInitialized,
        long lastModified, boolean invokeCallbacks, boolean ifNew, boolean ifOld,
        Object expectedOldValue, boolean requireOldValue) {

      super.basicPutPart3(event, entry, isInitialized, lastModified, invokeCallbacks, ifNew, ifOld,
          expectedOldValue, requireOldValue);

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
          notifyGatewaySender(event.getOperation().isUpdate() ? EnumListenerEvent.AFTER_UPDATE
              : EnumListenerEvent.AFTER_CREATE, event);
          // Notify listeners
          if (!event.isBulkOpInProgress()) {
            try {
              entry.dispatchListenerEvents(event);
            } catch (InterruptedException ignore) {
              Thread.currentThread().interrupt();
              getCancelCriterion().checkCancelInProgress(null);
            }
          }
        }
      }
    }
  }

  // Part of the fix for bug 35432, which required a change to the
  // distribution and notification order on the BridgeServer
  private class DistributedMetaRegion extends DistributedRegion {
    protected DistributedMetaRegion(RegionAttributes attrs) {
      super(DYNAMIC_REGION_LIST_NAME, attrs, null, DynamicRegionFactory.this.cache,
          new InternalRegionArguments(), DynamicRegionFactory.this.cache.getStatisticsClock());
    }

    // This is an internal uses only region
    @Override
    public boolean isSecret() {
      return true;
    }

    // //@override event tracker not needed for this type of region
    // void initEventTracker() {
    // }

    @Override
    public boolean isCopyOnRead() {
      return false;
    }

    // while internal, its contents should be communicated with bridge clients
    @Override
    public boolean shouldNotifyBridgeClients() {
      return getCache().getCacheServers().size() > 0;
    }

    // Over-ride the super behavior to perform the destruction of the dynamic region
    //
    @Override
    public void invokeDestroyCallbacks(EnumListenerEvent eventType, EntryEventImpl event,
        boolean callDispatchEventsCallback, boolean notifyGateways) {
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
    public long basicPutPart2(EntryEventImpl event, RegionEntry entry, boolean isInitialized,
        long lastModified, boolean clearConflict) {
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
        } finally {
          InitialImageOperation.setInhibitStateFlush(false);
        }
      }
      return result;
    }

    // The dynamic-region meta-region needs to tell clients about the event
    // after all servers have created the region so that register-interest
    // will work correctly
    @Override
    public void basicPutPart3(EntryEventImpl event, RegionEntry entry, boolean isInitialized,
        long lastModified, boolean invokeCallbacks, boolean ifNew, boolean ifOld,
        Object expectedOldValue, boolean requireOldValue) {

      super.basicPutPart3(event, entry, isInitialized, lastModified, invokeCallbacks, ifNew, ifOld,
          expectedOldValue, requireOldValue);

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
          notifyGatewaySender(event.getOperation().isUpdate() ? EnumListenerEvent.AFTER_UPDATE
              : EnumListenerEvent.AFTER_CREATE, event);
          // Notify listeners
          if (!event.isBulkOpInProgress()) {
            try {
              entry.dispatchListenerEvents(event);
            } catch (InterruptedException ignore) {
              Thread.currentThread().interrupt();
              getCancelCriterion().checkCancelInProgress(null);
            }
          }
        }
      }
    }

  }

}
