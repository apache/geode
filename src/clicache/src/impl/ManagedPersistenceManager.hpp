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
#include <gfcpp/PersistenceManager.hpp>
#include "PersistenceManagerProxy.hpp"

namespace gemfire {

  /// <summary>
  /// Wraps the managed <see cref="GemStone.GemFire.Cache.IPersistenceManager" />
  /// object and implements the native <c>gemfire::PersistenceManager</c> interface.
  /// </summary>
  class ManagedPersistenceManagerGeneric : public gemfire::PersistenceManager
  {
  public:

    inline ManagedPersistenceManagerGeneric(Object^ userptr ) : m_userptr(userptr) { }

    static gemfire::PersistenceManager* create( const char* assemblyPath,
      const char* factoryFunctionName );

    virtual ~ManagedPersistenceManagerGeneric( ) { }

   
    virtual void write(const CacheableKeyPtr&  key, const CacheablePtr&  value, void *& PersistenceInfo);
    virtual bool writeAll();
    virtual void init(const RegionPtr& region, PropertiesPtr& diskProperties);
    virtual CacheablePtr read(const CacheableKeyPtr& key, void *& PersistenceInfo);
    virtual bool readAll();
    virtual void destroy(const CacheableKeyPtr& key, void *& PersistenceInfo);
    virtual void close();

    inline void setptr(GemStone::GemFire::Cache::Generic::IPersistenceManagerProxy^ managedptr)
    {
      m_managedptr = managedptr;
    }

    inline Object^ userptr( ) const
    {
      return m_userptr;
    }

  private:


     /// <summary>
    /// Using gcroot to hold the managed delegate pointer (since it cannot be stored directly).
    /// Note: not using auto_gcroot since it will result in 'Dispose' of the IPersistenceManager
    /// to be called which is not what is desired when this object is destroyed. Normally this
    /// managed object may be created by the user and will be handled automatically by the GC.
    /// </summary>
    gcroot<GemStone::GemFire::Cache::Generic::IPersistenceManagerProxy^> m_managedptr;

    gcroot<Object^> m_userptr;

    // Disable the copy and assignment constructors
    ManagedPersistenceManagerGeneric( const ManagedPersistenceManagerGeneric& );
    ManagedPersistenceManagerGeneric& operator = ( const ManagedPersistenceManagerGeneric& );
  };

}
