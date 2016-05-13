#ifndef __GEMFIRE_CACHEATTRIBUTESFACTORY_H__
#define __GEMFIRE_CACHEATTRIBUTESFACTORY_H__

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "gfcpp_globals.hpp"
#include "ExceptionTypes.hpp"
#include "CacheAttributes.hpp"


/**
 * @file
 */


namespace gemfire
{

  /**
   * Creates instances of <code>CacheAttributes</code>.
   *
   * @see CacheAttributes
   */
  class CPPCACHE_EXPORT CacheAttributesFactory
  {
    /**
     * @brief public methods
     */
    public:

      /**
       * @brief constructor
       * Creates a new instance of CacheAttributesFactory ready to create a
       *       <code>CacheAttributes</code> with default settings.
       */
      CacheAttributesFactory();

      /**
       *@brief destructor
       */
      virtual ~CacheAttributesFactory();

      // ATTRIBUTES

      /**
       * Sets the redundancy level for this native client. Notification
       * queues are attempted to be maintained on n servers ( if possible )
       * where n = redundancy level.
       * @deprecated since 3.5, use {@link PoolFactory#setSubscriptionRedundancy} instead.
       */
      void setRedundancyLevel(int redundancyLevel);

      /**
       * Sets the end points for this native client.
       * @deprecated since 3.5, use {@link PoolFactory#addServer} or {@link PoolFactory#addLocator}instead.
       */
      void setEndpoints(const char* endPoints);

      // FACTORY METHOD

      /**
       * Creates a <code>CacheAttributes</code> with the current settings.
       * @return the newly created <code>CacheAttributes</code>
       * @throws IllegalStateException if the current settings violate the
       * <a href="compatibility.html">compatibility rules</a>
       */
      CacheAttributesPtr createCacheAttributes();


    private:

      CacheAttributesPtr m_cacheAttributes;

      // Never implemented
      CacheAttributesFactory(const CacheAttributesPtr& cacheAttributes);
  };

} //namespace gemfire


#endif //ifndef __GEMFIRE_CACHEATTRIBUTESFACTORY_H__
