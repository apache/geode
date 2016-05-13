/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
  * @file    EventTest.hpp
  * @since   1.0
  * @version 1.0
  * @see
  *
  */

// ----------------------------------------------------------------------------

#ifndef __EVENT_TEST_HPP__
#define __EVENT_TEST_HPP__

// ----------------------------------------------------------------------------

#include "fwklib/FrameworkTest.hpp"

#include <vector>

namespace gemfire {
namespace testframework {
  
typedef std::vector<RegionPtr> RegionVector;
  
// ----------------------------------------------------------------------------

class ETCacheListener : public CacheListener
{
public:
  ETCacheListener( const FrameworkTest * test ) : m_test( test ), m_bb( "ETCacheListener" ) {} ;

  virtual void afterCreate( const EntryEvent& event );

  virtual void afterUpdate( const EntryEvent& event );

  virtual void afterInvalidate( const EntryEvent& event );

  virtual void afterDestroy( const EntryEvent& event );

  virtual void afterRegionInvalidate( const RegionEvent& event );

  virtual void afterRegionDestroy( const RegionEvent& event );

  virtual void close( const RegionPtr& region );

private:
  /** @brief needed for BB access */
  const FrameworkTest * m_test;
  std::string m_bb;
};

// ----------------------------------------------------------------------------

class ETCacheWriter : public CacheWriter
{
public:
  ETCacheWriter( const FrameworkTest * test ) : m_test( test ), m_bb( "ETCacheWriter" ) {} ;

  virtual bool beforeUpdate( const EntryEvent& event );

  virtual bool beforeCreate( const EntryEvent& event );

  virtual bool beforeDestroy( const EntryEvent& event );

  virtual bool beforeRegionDestroy( const RegionEvent& event );

  virtual void close( const RegionPtr& region );

private:
  /** @brief needed for BB access */
  const FrameworkTest * m_test;
  std::string m_bb;
};

// ----------------------------------------------------------------------------

class ETCacheLoader : public CacheLoader
{
public:
  ETCacheLoader( const FrameworkTest * test ) : m_test( test ), m_bb( "ETCacheLoader" ) {} ;

  CacheablePtr load(const RegionPtr& rp, const CacheableKeyPtr& key,
                    const UserDataPtr& aCallbackArgument);

  virtual void close( const RegionPtr& region );

private:
  /** @brief needed for BB access */
  const FrameworkTest * m_test;
  std::string m_bb;
};

// ----------------------------------------------------------------------------

class EventTest : public FrameworkTest
{
public:
// ----------------------------------------------------------------------------

EventTest( const char * initArgs ) :
  FrameworkTest( initArgs ),
  m_sRegionName( "EventRegion" ),
  m_bb( "EventBB" ),
  m_numKeys( 0 ),
  m_failCount() {}

  virtual ~EventTest() {
    m_keysVec.clear();
  }

public: // FwkClientService
//  void onRegisterMembers(void);

public:
  int32_t doCreateRootRegion();
  int32_t doEventOperations();
  int32_t doIterate();
  int32_t doMemoryMeasurement();
  int32_t verifyKeyCount();
  int32_t addEntry();
  int32_t addOrDestroyEntry();
  int32_t validateCacheContent();
  int32_t validateRegionContent();
  int32_t doCreateObject();
  int32_t doIterateOnEntry();
  int32_t doNetSearch();
  int32_t doGets();
  int32_t feedEntries();
  int32_t doBasicTest();
  void doTwinkleRegion();
  int32_t doEntryEvents();
  void  entryEvent( const char* opcode );

  void checkTest( const char * taskId );

  void createRootRegion( const char * regionName = NULL );

  bool TestNetSearch(RegionPtr& regionPtr,CacheableStringPtr& pszKey,
     CacheableBytesPtr& pszValue);

  bool TestEntryPropagation(RegionPtr& regionPtr,
     CacheableStringPtr& szKey, CacheableBytesPtr& szValue);

  void addObject( RegionPtr& regionPtr, 
    const char* pszKey = NULL, const char* pszValue = NULL);
  void invalidateObject(RegionPtr& regionPtr, bool bIsLocalInvalidate);
  void destroyObject(RegionPtr& regionPtr, bool bIsLocalDestroy);
  void updateObject(RegionPtr& regionPtr);
  void readObject(RegionPtr& regionPtr);
  void netsearchObject( RegionPtr& randomRegion );
  

  void addRegion();

  /** @brief Invalidate a random region.
    *  @param isLocalInvalidate true if the invalidate should be a
    *   local invalidate, false otherwise.
    *  @retval The total number of regions invalidated (counting subregions
    *   of the random region invalidated)
    */
  void invalidateRegion(RegionPtr& regionPtr, bool bIsLocalInvalidate);

  /** @brief Destroy a random region.
    *  @param isLocalDestroy true if the destroy should be a local destroy,
    *    false otherwise.
    *  @retval The total number of regions destroyed (counting subregions of
    *   the random region destroyed)
    */
  void destroyRegion(RegionPtr& regionPtr, bool bIsLocalDestroy);

  /** @brief Return a currently existing random region.
    * Assumes the root region exists.
    * @param bAllowRootRegion true if this call can return the root region,
    *  false otherwise.
    * @retval A random region, or null of none available. Null can only be
    *  returned if bAllowRootRegion is false.
    */
  RegionPtr getRandomRegion(bool bAllowRootRegion);

  /** @brief Called when the test gets a RegionDestroyedException. Sometimes
    *  the test expects this exception, sometimes not. Check for error
    *  scenarios and throw an error if the test should not get the
    *  RegionDestroyedException.
    *  @param regionPtr - the region that supposedly was destroyed and
    *  triggered the RegionDestroyedException
    *  @param anException - the exception that was thrown.
    */
  void handleExpectedException( Exception & e );

  /** @brief Verify that the given key in the given region is invalid (
    *  has a null value).
    *  If not, then throw an error. Checking is done locally, without
    *  invoking a cache loader or doing a net search.
    *
    *  @param regionPtr The region containing key
    *  @param key The key that should have a null value
    */
  void verifyObjectInvalidated(RegionPtr& regionPtr, CacheableKeyPtr& key);

  /** @brief Verify that the given key in the given region is destroyed
    *  (has no key/value).  If not, then throw an error. Checking is done
    *  locally, without invoking a cache loader or doing a net search.
    *
    *  @param regionPtr The region contains the destroyed key
    *  @param key The destroyed key
    */
  void verifyObjectDestroyed(RegionPtr& regionPtr, CacheableKeyPtr& key);

  /** @brief Used for end task validation, iterate the keys/values in
    *   the given region, checking that the key/value match according
    *   to the test strategy.
    *
    * @param aRegion - the region to iterate.
    * @param bAllowZeroKeys - If the number of keys in the region is 0,
    *   then allow it if this is true, otherwise log an error to the
    *   return string.
    * @param bAllowZeroNonNullValues - If the number of non-null values in
    *   the region is 0, then allow it if this is true, otherwise log an
    *   error to the return string.
    * @param  iKeysInRegion returns the number of keys in aRegion
    * @param  iNoneNullValuesInRegion return hhe number of non-null values
    *   in aRegion
    * @param  sError return a String containg a description of any errors
    *   detected, or "" if none.
    */
  void  iterateRegion(
    RegionPtr aRegion, bool bAllowZeroKeys, bool bAllowZeroNonNullValues,
    uint32_t& ulKeysInRegion, uint32_t& ulNoneNullValuesInRegion,
    std::string& sError);

  int getSubRegionCount(const RegionPtr& regionPtr);
  int getAllRegionCount();
  int getRootRegionCount();
  CacheableKeyPtr getKey(RegionPtr& regionPtr, bool bInvalidOK);

  void removeRegion(RegionPtr &region);
  
  int32_t getRegionCount() {
    VectorOfRegion rootRegionVector;
    m_cache->rootRegions(rootRegionVector);
    return rootRegionVector.size();
  }
  
  int32_t getFailCount() { return m_failCount->value(); }
  void clearFailCount() { m_failCount->zero(); }
  void incFailCount() { m_failCount->add( 1 ); }

private:
  int32_t percentDifferent();
  void doEntryTest(const char* opcode);	
  void doRegionTest(const char* opcode, int iMaxRegions);
  const std::string getNextRegionName(RegionPtr& regionPtr);
  void measureMemory(std::string, double & vs, double & rs);
  CacheableKeyPtr findKeyNotInCache( RegionPtr& regionPtr );

  std::string             m_sRegionName;
  std::string m_bb;
  
  /** @brief abort, exit, done ops downcounter before execution */
  int m_iPremptCounter;

  VectorOfCacheableKey m_keysVec;
  int32_t m_numKeys;
  ACE_TSS<perf::Counter> m_failCount;
};

} // namespace testframework
} // namespace gemfire
// ----------------------------------------------------------------------------

#endif // __EVENT_TEST_HPP__

