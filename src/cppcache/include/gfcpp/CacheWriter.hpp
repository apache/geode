#ifndef __GEMFIRE_CACHEWRITER_H__
#define __GEMFIRE_CACHEWRITER_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"

/**
 * @file
 *
 */

namespace gemfire {

class EntryEvent;
class RegionEvent;
/**
 * @class CacheWriter CacheWriter.hpp
 * An application plug-in that can be installed on the region.
 * Defines methods that are called BEFORE entry modification.
 * A distributed region will typically have a single cache writer.
 * If the application is designed such that all or most updates to
 * a region occurs on a node, it is desirable to have the cache writer
 * for the region to be installed at that node.
 *
 * Cache writer invocations are initiated by the node where the entry or
 * region modification occurs. If there is a local cache writer on the node
 * where the change occurs, that is invoked. If there is no local cache writer
 * , the system knows which of the nodes that have the region defined have a
 * cache writer defined.
 *
 * Note that cache writer callbacks are synchronous callbacks and have the
 * ability
 * to veto the cache update. Since cache writer invocations require
 * communications
 * over the network, (especially if they are not colocated on the nodes where
 * the
 * change occurs) the use of cache writers presents a performance penalty.
 *
 * A user-defined object defined in the {@link RegionAttributes} that is
 * called synchronously before a region or entry in the cache is modified.
 *
 * The typical use for a <code>CacheWriter</code> is to update a database.
 * Application writers should implement these methods to execute
 * application-specific behavior before the cache is modified.
 *
 * <p>Before the region is updated via a put, create, or destroy operation,
 * GemFire will call a <code>CacheWriter</code> that is installed anywhere in
 * any
 * participating cache for that region, preferring a local
 * <code>CacheWriter</code>
 * if there is one. Usually there will be only one <code>CacheWriter</code> in
 * the distributed system. If there are multiple <code>CacheWriter</code>s
 * available in the distributed system, the GemFire
 * implementation always prefers one that is stored locally, or else picks one
 * arbitrarily. In any case, only one <code>CacheWriter</code> will be invoked.
 *
 * <p>The <code>CacheWriter</code> is capable of aborting the update to the
 * cache by throwing
 * a <code>CacheWriterException</code>. This exception or any runtime exception
 * thrown by the <code>CacheWriter</code> will abort the operation, and the
 * exception will be propagated to the initiator of the operation, regardless
 * of whether the initiator is in the same process as the
 * <code>CacheWriter</code>.
 *
 * @see AttributesFactory::setCacheWriter
 * @see RegionAttributes::getCacheWriter
 * @see AttributesMutator::setCacheWriter
 */
class CPPCACHE_EXPORT CacheWriter : public SharedBase {
 public:
  /**
   * Called before an entry is updated. The entry update is initiated by a
   * <code>put</code>
   * or a <code>get</code> that causes the loader to update an existing entry.
   * The entry previously existed in the cache where the operation was
   * initiated, although the old value may have been NULLPTR. The entry being
   * updated may or may not exist in the local cache where the CacheWriter is
   * installed.
   *
   * @param event EntryEvent denotes the event object associated with updating
   * the entry
   *
   *
   * @see Region::put
   * @see Region::get
   */
  virtual bool beforeUpdate(const EntryEvent& event);

  /** Called before an entry is created. Entry creation is initiated by a
   * <code>create</code>, a <code>put</code>, or a <code>get</code>.
   * The <code>CacheWriter</code> can determine whether this value comes from a
   * <code>get</code> or not from {@link EntryEvent::isLoad}. The entry being
   * created may already exist in the local cache where this
   * <code>CacheWriter</code>
   * is installed, but it does not yet exist in the cache where the operation
   * was initiated.
   * @param event EntryEvent denotes the event object associated with creating
   * the entry
   *
   *
   * @see Region::create
   * @see Region::put
   * @see Region::get
   */
  virtual bool beforeCreate(const EntryEvent& event);

  /**
   * Called before an entry is destroyed. The entry being destroyed may or may
   * not exist in the local cache where the CacheWriter is installed. This
   * method
   * is <em>not</em> called as a result of expiration or {@link
   * Region::localDestroyRegion}.
   *
   * @param event EntryEvent denotes the event object associated with destroying
   * the entry
   *
   * @see Region::destroy
   */
  virtual bool beforeDestroy(const EntryEvent& event);

  /*@brief called before this region is cleared
   * @param event EntryEvent denotes the event object associated with clearing
   * the region
   *
   * @see Region::clear
   */
  virtual bool beforeRegionClear(const RegionEvent& event);

  /*@brief called before this region is destroyed
   * @param event EntryEvent denotes the event object associated with destroying
   * the region
   *
   * @see Region::destroyRegion
   */
  virtual bool beforeRegionDestroy(const RegionEvent& event);

  /** Called when the region containing this callback is destroyed, when
     * the cache is closed.
     *
     * <p>Implementations should clean up any external
     * resources, such as database connections. Any runtime exceptions this
   * method
     * throws will be logged.
     *
     * <p>It is possible for this method to be called multiple times on a single
     * callback instance, so implementations must be tolerant of this.
     *
     * @see Cache::close
     * @see Region::destroyRegion
   */
  virtual void close(const RegionPtr& rp);

  virtual ~CacheWriter();

 protected:
  CacheWriter();

 private:
  // never implemented.
  CacheWriter(const CacheWriter& other);
  void operator=(const CacheWriter& other);
};

}  // namespace gemfire

#endif
