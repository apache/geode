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
#include <cppcache/Properties.hpp>
#include "../PropertiesM.hpp"


namespace gemfire
{

  /// <summary>
  /// Wraps the managed <see cref="GemStone.GemFire.Cache.PropertyVisitor" />
  /// delegate and implements the native <c>gemfire::Properties::Visitor</c> interface.
  /// </summary>
  class ManagedVisitor
    : public gemfire::Properties::Visitor
  {
  public:

    /// <summary>
    /// Create a <c>gemfire::Properties::Visitor</c> from the given managed
    /// <c>PropertyVisitor</c> delegate.
    /// </summary>
    inline ManagedVisitor(
      GemStone::GemFire::Cache::PropertyVisitor^ visitorFunc )
      : m_managedptr( visitorFunc ) { }

    /// <summary>
    /// Invokes the managed <c>PropertyVisitor</c> delegate for the given
    /// <c>Property</c> key and value.
    /// </summary>
    virtual void visit( CacheableKeyPtr& key, CacheablePtr& value );

    /// <summary>
    /// Destructor -- does nothing.
    /// </summary>
    virtual ~ManagedVisitor( ) { }


  private:

    // Using gcroot to hold the managed delegate pointer (since it cannot be stored directly).
    // Note: not using auto_gcroot since it will result in 'Dispose' of the PropertyVisitor
    // to be called which is not what is desired when this object is destroyed. Normally this
    // managed object may be created by the user and will be handled automatically by the GC.
    gcroot<GemStone::GemFire::Cache::PropertyVisitor^> m_managedptr;

    // Disable the copy and assignment constructors
    ManagedVisitor( );
    ManagedVisitor( const ManagedVisitor& );
  };

}
