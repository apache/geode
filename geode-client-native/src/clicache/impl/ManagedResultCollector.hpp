/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once
#include "../gf_defs.hpp"
#include <vcclr.h>
#include <cppcache/ResultCollector.hpp>
#include "../IResultCollector.hpp"

namespace gemfire {

  /// <summary>
  /// Wraps the managed <see cref="GemStone.GemFire.Cache.IResultCollector" />
  /// object and implements the native <c>gemfire::ResultCollector</c> interface.
  /// </summary>
  class ManagedResultCollector
    : public ResultCollector
  {
  public:

    /// <summary>
    /// Constructor to initialize with the provided managed object.
    /// </summary>
    /// <param name="managedptr">
    /// The managed object.
    /// </param>
    inline ManagedResultCollector(
      GemStone::GemFire::Cache::IResultCollector^ managedptr )
      : m_managedptr( managedptr ) { }

    /// <summary>
    /// Static function to create a <c>ManagedResultCollector</c> using given
    /// managed assembly path and given factory function.
    /// </summary>
    /// <param name="assemblyPath">
    /// The path of the managed assembly that contains the <c>ICacheListener</c>
    /// factory function.
    /// </param>
    /// <param name="factoryFunctionName">
    /// The name of the factory function of the managed class for creating
    /// an object that implements <c>IResultCollector</c>.
    /// This should be a static function of the format
    /// {Namespace}.{Class Name}.{Method Name}.
    /// </param>
    /// <exception cref="IllegalArgumentException">
    /// If the managed library cannot be loaded or the factory function fails.
    /// </exception>
    static ResultCollector* create( const char* assemblyPath,
      const char* factoryFunctionName );

    /// <summary>
    /// Destructor -- does nothing.
    /// </summary>
    virtual ~ManagedResultCollector( ) { }

        CacheableVectorPtr getResult(uint32_t timeout =  DEFAULT_QUERY_RESPONSE_TIMEOUT);
        void addResult(CacheablePtr& result);
        void endResults();
        void clearResults();
    /// <summary>
    /// Returns the wrapped managed object reference.
    /// </summary>
    inline GemStone::GemFire::Cache::IResultCollector^ ptr( ) const
    {   
      return m_managedptr;
    }


  private:


    /// <summary>
    /// Using gcroot to hold the managed delegate pointer (since it cannot be stored directly).
    /// Note: not using auto_gcroot since it will result in 'Dispose' of the ICacheListener
    /// to be called which is not what is desired when this object is destroyed. Normally this
    /// managed object may be created by the user and will be handled automatically by the GC.
    /// </summary>
    gcroot<GemStone::GemFire::Cache::IResultCollector^> m_managedptr;
    //GemStone::GemFire::Cache::IResultCollector^ m_managedptr;
  };

}
