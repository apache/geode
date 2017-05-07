/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "../gf_includes.hpp"
#include "SafeConvert.hpp"
#include "ManagedResultCollector.hpp"
#include "../IResultCollector.hpp"
#include "../IGFSerializable.hpp"
#include "ManagedString.hpp"


using namespace System;
using namespace System::Reflection;
using namespace GemStone::GemFire::Cache;

namespace gemfire
{

  ResultCollector* ManagedResultCollector::create( const char* assemblyPath,
    const char* factoryFunctionName )
  {
    try
    {
      String^ mg_assemblyPath =
        GemStone::GemFire::ManagedString::Get( assemblyPath );
      String^ mg_factoryFunctionName =
        GemStone::GemFire::ManagedString::Get( factoryFunctionName );
      String^ mg_typeName = nullptr;
      Int32 dotIndx = -1;

      if (mg_factoryFunctionName == nullptr ||
        ( dotIndx = mg_factoryFunctionName->LastIndexOf( '.' ) ) < 0 )
      {
        std::string ex_str = "ManagedResultCollector: Factory function name '";
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
        std::string ex_str = "ManagedResultCollector: Could not load assembly: ";
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
          GemStone::GemFire::Cache::IResultCollector^ managedptr = nullptr;
          try
          {
            managedptr = dynamic_cast<GemStone::GemFire::Cache::IResultCollector^>(
              mInfo->Invoke( typeInst, nullptr ) );
          }
          catch (System::Exception^)
          {
            managedptr = nullptr;
          }
          if (managedptr == nullptr)
          {
            std::string ex_str = "ManagedResultCollector: Could not create "
              "object on invoking factory function [";
            ex_str += factoryFunctionName;
            ex_str += "] in assembly: ";
            ex_str += assemblyPath;
            throw IllegalArgumentException( ex_str.c_str( ) );
          }
          return new ManagedResultCollector( managedptr );
        }
        else
        {
          std::string ex_str = "ManagedResultCollector: Could not load "
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
        std::string ex_str = "ManagedResultCollector: Could not load type [";
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
      std::string ex_str = "ManagedResultCollector: Got an exception while "
        "loading managed library: ";
      ex_str += mg_exStr.CharPtr;
      throw IllegalArgumentException( ex_str.c_str( ) );
    }
    return NULL;
  }

  void ManagedResultCollector::addResult( CacheablePtr& result )
  {
    try {
      IGFSerializable^ rs = SafeUMSerializableConvert( result.ptr( ) );
      m_managedptr->AddResult( rs );
    }
    catch ( GemStone::GemFire::Cache::GemFireException^ ex ) {
      ex->ThrowNative( );
    }
    catch ( System::Exception^ ex ) {
      GemStone::GemFire::ManagedString mg_exStr( ex->ToString( ) );
      std::string ex_str = "ManagedResultCollector: Got an exception in"
        "addResult: ";
      ex_str += mg_exStr.CharPtr;
      throw IllegalArgumentException( ex_str.c_str( ) );
    }
  }

  CacheableVectorPtr ManagedResultCollector::getResult(uint32_t timeout)
  {
    try {
       array<IGFSerializable^>^ rs = m_managedptr->GetResult(timeout);
       gemfire::CacheableVectorPtr rsptr = gemfire::CacheableVector::create();
       for( int index = 0; index < rs->Length; index++ )
       {
         rsptr->push_back(gemfire::CacheablePtr(SafeMSerializableConvert( rs[ index])));
       }
       return rsptr;
    }
    catch ( GemStone::GemFire::Cache::GemFireException^ ex ) {
      ex->ThrowNative( );
    }
    catch ( System::Exception^ ex ) {
      GemStone::GemFire::ManagedString mg_exStr( ex->ToString( ) );
      std::string ex_str = "ManagedResultCollector: Got an exception in"
        "getResult: ";
      ex_str += mg_exStr.CharPtr;
      throw IllegalArgumentException( ex_str.c_str( ) );
    }
    return NULLPTR;
  }
  void ManagedResultCollector::endResults()
  {
    try {
       m_managedptr->EndResults();
    }
    catch ( GemStone::GemFire::Cache::GemFireException^ ex ) {
      ex->ThrowNative( );
    }
    catch ( System::Exception^ ex ) {
      GemStone::GemFire::ManagedString mg_exStr( ex->ToString( ) );
      std::string ex_str = "ManagedResultCollector: Got an exception in"
        "endResults: ";
      ex_str += mg_exStr.CharPtr;
      throw IllegalArgumentException( ex_str.c_str( ) );
    }
  }
  void ManagedResultCollector::clearResults()
  {
    try {
       m_managedptr->ClearResults();
    }
    catch ( GemStone::GemFire::Cache::GemFireException^ ex ) {
      ex->ThrowNative( );
    }
    catch ( System::Exception^ ex ) {
      GemStone::GemFire::ManagedString mg_exStr( ex->ToString( ) );
      std::string ex_str = "ManagedResultCollector: Got an exception in"
        "clearResults: ";
      ex_str += mg_exStr.CharPtr;
      throw IllegalArgumentException( ex_str.c_str( ) );
    }
  }
}
