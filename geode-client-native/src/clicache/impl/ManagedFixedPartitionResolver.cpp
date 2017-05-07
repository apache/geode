/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "../gf_includes.hpp"
#include "ManagedFixedPartitionResolver.hpp"
#include "../IFixedPartitionResolver.hpp"
#include "../RegionM.hpp"
#include "../EntryEventM.hpp"
#include "SafeConvert.hpp"

using namespace System;
using namespace System::Reflection;

namespace gemfire
{
  CacheableKeyPtr ManagedFixedPartitionResolver::getRoutingObject(const EntryEvent& key)
  {    
    try {
      GemStone::GemFire::Cache::EntryEvent mevent( &key );
      return gemfire::CacheablePtr(
        GemStone::GemFire::Cache::SafeMSerializableConvert(
        m_managedptr->GetRoutingObject( %mevent )));
    }  
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
    return NULLPTR;
  }

  const char* ManagedFixedPartitionResolver::getName() 
  {    
    try {
      GemStone::GemFire::ManagedString mg_exStr( m_managedptr->GetName()->ToString() );
      return mg_exStr.CharPtr;
    }
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
    return NULL;
  }

  const char* ManagedFixedPartitionResolver::getPartitionName(const EntryEvent& opDetails,
      CacheableHashSetPtr targetPartitions) 
  {    
    try {
      GemStone::GemFire::Cache::EntryEvent mevent( &opDetails );
      GemStone::GemFire::Cache::CacheableHashSet^ ret = static_cast<GemStone::GemFire::Cache::CacheableHashSet^>
        ( GemStone::GemFire::Cache::SafeUMSerializableConvert(targetPartitions.ptr()));
      GemStone::GemFire::ManagedString mg_exStr( m_managedptr->GetPartitionName(%mevent, ret)->ToString() );
      return mg_exStr.CharPtr;
    }  
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
    return NULL;
  }

}
