/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "../../../../gf_includes.hpp"
#include "ManagedVisitor.hpp"
#include "SafeConvert.hpp"
#include "../ExceptionTypes.hpp"


using namespace System;

namespace gemfire
{

  void ManagedVisitorGeneric::visit( CacheableKeyPtr& key, CacheablePtr& value )
  {
    using namespace GemStone::GemFire::Cache::Generic;
    try {
      ICacheableKey^ mg_key(SafeGenericUMKeyConvert<ICacheableKey^>(key.ptr()));
      IGFSerializable^ mg_value(SafeUMSerializableConvertGeneric(value.ptr()));

      m_visitor->Invoke( mg_key, (GemStone::GemFire::Cache::Generic::IGFSerializable^)mg_value );
    }
    catch (GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemFireException::ThrowNative(ex);
    }
  }

}
