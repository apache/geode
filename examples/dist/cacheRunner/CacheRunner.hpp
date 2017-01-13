/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
 * @file CacheRunner.hpp
 * @since   1.0
 * @version 1.0
 * @see
*/

#ifndef __CACHE_RUNNER_HPP__
#define __CACHE_RUNNER_HPP__

#define _GF_SAFE_DELETE(PTR) if (PTR){ delete PTR; PTR = 0; }
#define _GF_SAFE_DELETE_ARRAY(PTR) if (PTR){ delete []PTR; PTR = 0; }

#ifndef NULL
#define NULL 0
#endif

#include "CommandReader.hpp"

#include <string>

using namespace gemfire;

// ----------------------------------------------------------------------------

// forward declare
class CacheRunner;

// make a smart pointer for CacheRunner
typedef SharedPtr< CacheRunner > CacheRunnerPtr;

//typedef ACE_Hash_Map_Manager_Ex<CacheableKeyPtr, MapEntryPtr, ::ACE_Hash<CacheableKeyPtr>, ::ACE_Equal_To<CacheableKeyPtr>, ::ACE_Null_Mutex> CacheableKeyHashMap;

// ----------------------------------------------------------------------------

/**
 * @class CacheRunner
 *
 * @brief This class is a command-line application that allows the user to
 * exercise GemFire's eXtreme cache API}. The example allows the user
 * to specify a <A HREF="{@docRoot}/../cacheRunner/cache.xml">cache.xml</A>
 * file that specifies a parent region with certain properties and
 * then allows the user to exercise the cache API
 *
 * 
 * @since 3.0
 */

class CacheRunner : public SharedBase
{
public:

  CacheRunner( ) {}
  ~CacheRunner( ) {}

  /** @brief create an instance of CacheRunner.
    * @throw OutOfMemoryException if not enough memory for the object creation.
    */
  inline static CacheRunnerPtr create_Runner( )
      throw(OutOfMemoryException)
    {
      CacheRunnerPtr rptr;
      rptr = NULLPTR;
      CacheRunner* rp = new CacheRunner( );
      if (!rp)
      {
        throw OutOfMemoryException("ReferenceCountedFactory::create_Runner: out of memory");
      }
      rptr = rp;
      return rptr;
  }

  /**
   * @brief Prints information on how this program should be used.
   */
  void showHelp( );
  void showHelpForClientType( );

  /**
    * @brief Initializes the <code>Cache</code> for this example program.
    * Uses the {@link TestCacheListener}, {@link TestCacheLoader},
    * {@link TestCacheWriter}, and {@link TestCapacityController}.
    */
  void initialize( );

  void setXmlFile( std::string cacheXmlFileName );
  /**
   * @brief Prints number of regions are created in the cache.
   */
  void cacheInfo();

  /**
    * @brief Prompts the user for input and executes the command accordingly.
    */
  void go( );


private:
  /**
    * @brief Connect to distributed system and set properties
    * @retval returns true if success, false if failed
    */
  bool connectDistributedSystem( const char* pszCacheXmlFileName = NULL);

  /**
    * @brief Disconnect to distributed system and set properties
    * @retval returns true if success, false if failed
    */
  void disconnectDistributedSystem( );

  /**
    * @brief Prints out information about the current region
    *
    * @see Region#getStatistics
    */
  void status(CommandReader& commandReader) throw ( Exception );

  /**
    * @brief Creates a new subregion of the current region
    *
    * @see Region#createSubregion
    */
  void mkrgn(CommandReader& commandReader) throw ( Exception );

  /**
    * @brief Prints out information about the region entries that are
    * currently locked.
    */
  void showlocks();

  /**
    * @brief Locks the current region or an entry in the current region based
    * on the given <code>command</code>.
    *
    * @see Region#getRegionDistributedLock
    * @see Region#getDistributedLock
    */
  void chrgn(CommandReader& commandReader) throw ( Exception );

  /**
    * @brief Invalidates (either local or distributed) a region or region
    * entry depending on the contents of <code>command</code>.
    *
    * @see Region#invalidateRegion
    * @see Region#invalidate
    */
  void inv(CommandReader& commandReader) throw ( Exception );

  /**
    * @brief Resets the current region attributes
    */
  void reset() throw ( Exception );

  /**
    * @brief Prints out the current region attributes
    *
    * @see Region#getAttributes
    */
  void attr(CommandReader& commandReader) throw ( Exception );

  /**
    * @brief Gets a cached object from the current region and prints out its
    * <code>String</code> value.
    *
    * @see Region#get
    */
  void get(CommandReader& commandReader) throw ( Exception );

  void run(CommandReader& commandReader) throw ( Exception );

  void putAll(CommandReader& commandReader) throw ( Exception );

  /**
    * @brief Register keys in current region
    *
    * @see Region#registerKeys
    */
  void registerKeys(CommandReader& commandReader) throw ( Exception );

  /**
    * @brief unregister keys in current region
    *
    * @see Region#unregisterKeys
    */
  void unregisterKeys(CommandReader& commandReader) throw ( Exception );

  /**
    * @brief Register regular expression in current region
    *
    * @see Region#registerRegex
    */
  void registerRegex(CommandReader& commandReader) throw ( Exception );

  /**
    * @brief unregister regular expression in current region
    *
    * @see Region#unregisterRegex
    */
  void unregisterRegex(CommandReader& commandReader) throw ( Exception );

  /**
    * @brief Creates a new entry in the current region
    *
    * @see Region#create
    */

  void create(CommandReader& commandReader) throw ( Exception );

  /**
    * @brief Puts an entry into the current region
    *
    * @see Region#put
    */
  void put(CommandReader& commandReader) throw ( Exception );

  /**
    * @brief Destroys (local or distributed) a region or entry in the current
    * region.
    *
    * @see Region#destroyRegion
    */
  void des(CommandReader& commandReader) throw ( Exception );

  /**
    * @brief Lists the contents of the current region.
    *
    * @see Region#entries
    */
  void ls(CommandReader& commandReader) throw ( Exception );

  /**
    * @brief Prints the key entry pair.
    */

  void  printEntry(CacheableKeyPtr& sKey, CacheablePtr& valueBytes);

  /**
    * @brief prints the region attributes.
    */

  void printAttribute(RegionAttributesPtr& attr);

  /**
    * @brief Sets an expiration attribute of the current region
    *
    * @see Region#getAttributesMutator
    */
  void setExpirationAttr(CommandReader& commandReader) throw ( Exception );

  /**
    * @brief Sets a region attribute of the current region
    *
    * @see #setExpirationAttr
    * @see Region#getAttributesMutator
    */
  void setRgnAttr(CommandReader& commandReader) throw ( Exception );

  /**
    * @brief Specifies the <code>cache.xml</code> file to use when creating
    * the <code>Cache</code>.  If the <code>Cache</code> has already
    * been open, then the existing one is closed.
    *
    * @see CacheFactory#create
    */
  void load(CommandReader& commandReader) throw ( Exception );

  /**
    * @brief Opens the <code>Cache</code> and sets the current region to the
    * _GS_CACHE_RUNNER_REGION region.
    *
    * @see Cache#getRegion
    */
  void open(CommandReader& commandReader) throw ( Exception );

  /**
    *  @brief Creates <code>ExpirationAttributes</code> from an expiration time
    *  and the name of an expiration action.
    */
  ExpirationAttributes* parseExpAction( int iExpTime, std::string sActionName);

  void exec(CommandReader& commandReader) throw ( Exception );
  void query(CommandReader& commandReader) throw ( Exception );
  void existsValue(CommandReader& commandReader) throw ( Exception );
  void selectValue(CommandReader& commandReader) throw ( Exception );

  void printStructSet(CacheablePtr field, StructPtr ssptr, int32_t& fields);
  void printResultset(SerializablePtr field);

private:
  /** @brief Cache <code>Region</code> currently reviewed by this example  */
  RegionPtr m_currRegionPtr;

  /** @brief The cache used in the example */
  CachePtr m_cachePtr;

  /** @brief the attributes of the current region */
  RegionAttributesPtr m_currRegionAttributesPtr;

  /** @brief This example's connection to the distributed system */
  DistributedSystemPtr m_distributedSystemPtr;

  /** @brief The cache.xml file used to declaratively configure the cache */
  std::string m_sCacheXmlFileName;

};

// ----------------------------------------------------------------------------

#endif // __CACHE_RUNNER_HPP__

