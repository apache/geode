/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "../gf_includes.hpp"
#include "ManagedVisitor.hpp"
#include "SafeConvert.hpp"

using namespace System;

namespace gemfire
{

  void ManagedVisitor::visit( CacheableKeyPtr& key, CacheablePtr& value )
  {
    try {
      GemStone::GemFire::Cache::ICacheableKey^ mg_key(
        GemStone::GemFire::Cache::SafeUMKeyConvert( key.ptr( ) ) );
      GemStone::GemFire::Cache::IGFSerializable^ mg_value(
        GemStone::GemFire::Cache::SafeUMSerializableConvert( value.ptr( ) ) );

      m_managedptr->Invoke( mg_key, mg_value );
    }
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
  }

}
