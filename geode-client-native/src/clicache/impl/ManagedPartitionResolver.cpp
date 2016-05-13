/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "../gf_includes.hpp"
#include "ManagedPartitionResolver.hpp"
#include "../IPartitionResolver.hpp"
#include "ManagedFixedPartitionResolver.hpp"
#include "../IFixedPartitionResolver.hpp"
#include "../RegionM.hpp"
#include "../EntryEventM.hpp"
#include "SafeConvert.hpp"

using namespace System;
using namespace System::Reflection;

namespace gemfire
{

  PartitionResolver* ManagedPartitionResolver::create( const char* assemblyPath,
    const char* factoryFunctionName )
  {
    try
    {
      String^ mg_assemblyPath =
        GemStone::GemFire::ManagedString::Get( assemblyPath );
      String^ mg_factoryFunctionName =
        GemStone::GemFire::ManagedString::Get( factoryFunctionName );
      String^ mg_typeName = nullptr;
      int32_t dotIndx = -1;

      if (mg_factoryFunctionName == nullptr ||
        ( dotIndx = mg_factoryFunctionName->LastIndexOf( '.' ) ) < 0 )
      {
        std::string ex_str = "ManagedPartitionResolver: Factory function name '";
        ex_str += factoryFunctionName;
        ex_str += "' does not contain type name";
        throw IllegalArgumentException( ex_str.c_str( ) );
      }

      mg_typeName = mg_factoryFunctionName->Substring( 0, dotIndx );
      mg_factoryFunctionName = mg_factoryFunctionName->Substring( dotIndx + 1 );

      Assembly^ assmb = nullptr;
      try
      {
        assmb = Assembly::Load( mg_assemblyPath );
      }
      catch (System::Exception^)
      {
        assmb = nullptr;
      }
      if (assmb == nullptr)
      {
        std::string ex_str = "ManagedPartitionResolver: Could not load assembly: ";
        ex_str += assemblyPath;
        throw IllegalArgumentException( ex_str.c_str( ) );
      }
      Object^ typeInst = assmb->CreateInstance( mg_typeName, true );
      if (typeInst != nullptr)
      {
        MethodInfo^ mInfo = typeInst->GetType( )->GetMethod( mg_factoryFunctionName,
          BindingFlags::Public | BindingFlags::Static | BindingFlags::IgnoreCase );
        if (mInfo != nullptr)
        {
          GemStone::GemFire::Cache::IPartitionResolver^ managedptr = nullptr;
          GemStone::GemFire::Cache::IFixedPartitionResolver^ managedFixedPtr = nullptr;
          try
          {
            managedptr = dynamic_cast<GemStone::GemFire::Cache::IPartitionResolver^>(
              mInfo->Invoke( typeInst, nullptr ) );
          }
          catch (System::Exception^)
          {
            managedptr = nullptr;
          }
          if (managedptr == nullptr)
          {
            std::string ex_str = "ManagedPartitionResolver: Could not create "
              "object on invoking factory function [";
            ex_str += factoryFunctionName;
            ex_str += "] in assembly: ";
            ex_str += assemblyPath;
            throw IllegalArgumentException( ex_str.c_str( ) );
          }
          try {
            managedFixedPtr = 
              safe_cast<GemStone::GemFire::Cache::IFixedPartitionResolver^>(managedptr);
          }
          catch (System::Exception^) {            
            managedFixedPtr = nullptr;
          }
          if (managedFixedPtr == nullptr) {            
            return new ManagedPartitionResolver( managedptr );
          }
          else {            
            return new ManagedFixedPartitionResolver( managedFixedPtr );
          }
        }
        else
        {
          std::string ex_str = "ManagedPartitionResolver: Could not load "
            "function with name [";
          ex_str += factoryFunctionName;
          ex_str += "] in assembly: ";
          ex_str += assemblyPath;
          throw IllegalArgumentException( ex_str.c_str( ) );
        }
      }
      else
      {
        GemStone::GemFire::ManagedString typeName( mg_typeName );
        std::string ex_str = "ManagedPartitionResolver: Could not load type [";
        ex_str += typeName.CharPtr;
        ex_str += "] in assembly: ";
        ex_str += assemblyPath;
        throw IllegalArgumentException( ex_str.c_str( ) );
      }
    }
    catch (const gemfire::Exception&)
    {
      throw;
    }
    catch (System::Exception^ ex)
    {
      GemStone::GemFire::ManagedString mg_exStr( ex->ToString( ) );
      std::string ex_str = "ManagedPartitionResolver: Got an exception while "
        "loading managed library: ";
      ex_str += mg_exStr.CharPtr;
      throw IllegalArgumentException( ex_str.c_str( ) );
    }
    return NULL;
  }

  CacheableKeyPtr ManagedPartitionResolver::getRoutingObject(const EntryEvent& key)
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

  const char* ManagedPartitionResolver::getName() 
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

}
