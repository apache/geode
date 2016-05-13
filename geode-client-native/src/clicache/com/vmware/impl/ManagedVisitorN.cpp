/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "../../../../gf_includesN.hpp"
#include "ManagedVisitorN.hpp"
#include "SafeConvertN.hpp"
#include "../ExceptionTypesMN.hpp"


using namespace System;

namespace gemfire
{

  void ManagedVisitorGeneric::visit( CacheableKeyPtr& key, CacheablePtr& value )
  {
    try {
      GemStone::GemFire::Cache::ICacheableKey^ mg_key(
        GemStone::GemFire::Cache::SafeUMKeyConvert( key.ptr( ) ) );
      GemStone::GemFire::Cache::IGFSerializable^ mg_value(
        GemStone::GemFire::Cache::SafeUMSerializableConvert( value.ptr( ) ) );

      m_visitor->Invoke( mg_key, mg_value );
    }
    catch (GemStone::GemFire::Cache::Generic::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::Generic::GemFireException::ThrowNative(ex);
    }
  }

}
