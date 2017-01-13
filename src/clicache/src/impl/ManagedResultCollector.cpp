/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "../../gf_includes.hpp"
//#include "../../../gf_includes.hpp"
#include "ManagedResultCollector.hpp"
//#include "../../../IGFSerializable.hpp"


//#include "../IGFSerializable.hpp"
#include "ManagedString.hpp"
#include "SafeConvert.hpp"
#include "../ExceptionTypes.hpp"
#include <string>

using namespace System;
using namespace System::Text;
using namespace System::Reflection;


namespace gemfire
{

  gemfire::ResultCollector* ManagedResultCollectorGeneric::create( const char* assemblyPath,
    const char* factoryFunctionName )
  {
    try
    {
      String^ mg_assemblyPath =
        GemStone::GemFire::Cache::Generic::ManagedString::Get( assemblyPath );
      String^ mg_factoryFunctionName =
        GemStone::GemFire::Cache::Generic::ManagedString::Get( factoryFunctionName );
      String^ mg_typeName = nullptr;
      Int32 dotIndx = -1;

      if (mg_factoryFunctionName == nullptr ||
        ( dotIndx = mg_factoryFunctionName->LastIndexOf( '.' ) ) < 0 )
      {
        std::string ex_str = "ManagedResultCollector: Factory function name '";
        ex_str += factoryFunctionName;
        ex_str += "' does not contain type name";
        throw gemfire::IllegalArgumentException( ex_str.c_str( ) );
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
        throw gemfire::IllegalArgumentException( ex_str.c_str( ) );
      }
      Object^ typeInst = assmb->CreateInstance( mg_typeName, true );
      if (typeInst != nullptr)
      {
        MethodInfo^ mInfo = typeInst->GetType( )->GetMethod( mg_factoryFunctionName,
          BindingFlags::Public | BindingFlags::Static | BindingFlags::IgnoreCase );
        if (mInfo != nullptr)
        {
          //GemStone::GemFire::Cache::Generic::ResultCollector<Object^>^ managedptr = nullptr;
          Object^ userptr = nullptr;
          try
          {
            throw gemfire::UnsupportedOperationException( "Not supported" );
            /*managedptr = dynamic_cast<GemStone::GemFire::Cache::Generic::ResultCollector<Object^>^>(
              mInfo->Invoke( typeInst, nullptr ) );*/
            userptr = mInfo->Invoke( typeInst, nullptr );
          }
          catch (System::Exception^ ex)
          {
            GemStone::GemFire::Cache::Generic::Log::Debug("{0}: {1}", ex->GetType()->Name, ex->Message);
            userptr = nullptr;
          }
          if (userptr == nullptr)
          {
            std::string ex_str = "ManagedResultCollector: Could not create "
              "object on invoking factory function [";
            ex_str += factoryFunctionName;
            ex_str += "] in assembly: ";
            ex_str += assemblyPath;
            throw gemfire::IllegalArgumentException( ex_str.c_str( ) );
          }
          //TODO::need to pass proper pointer here
          return new ManagedResultCollectorGeneric(/*(GemStone::GemFire::Cache::Generic::ResultCollector<Object^>^) managedptr*/nullptr );
        }
        else
        {
          std::string ex_str = "ManagedResultCollector: Could not load "
            "function with name [";
          ex_str += factoryFunctionName;
          ex_str += "] in assembly: ";
          ex_str += assemblyPath;
          throw gemfire::IllegalArgumentException( ex_str.c_str( ) );
        }
      }
      else
      {
        GemStone::GemFire::Cache::Generic::ManagedString typeName( mg_typeName );
        std::string ex_str = "ManagedResultCollector: Could not load type [";
        ex_str += typeName.CharPtr;
        ex_str += "] in assembly: ";
        ex_str += assemblyPath;
        throw gemfire::IllegalArgumentException( ex_str.c_str( ) );
      }
    }
    catch (const gemfire::Exception&)
    {
      throw;
    }
    catch (System::Exception^ ex)
    {
      GemStone::GemFire::Cache::Generic::ManagedString mg_exStr( ex->ToString( ) );
      std::string ex_str = "ManagedResultCollector: Got an exception while "
        "loading managed library: ";
      ex_str += mg_exStr.CharPtr;
      throw gemfire::IllegalArgumentException( ex_str.c_str( ) );
    }
    return NULL;
  }

  void ManagedResultCollectorGeneric::addResult(CacheablePtr& result )
  {
    try {
      //GemStone::GemFire::Cache::IGFSerializable^ res = SafeUMSerializableConvertGeneric(result.ptr());
      Object^ rs = GemStone::GemFire::Cache::Generic::Serializable::GetManagedValueGeneric<Object^>(result);
      m_managedptr->AddResult( rs );
      //m_managedptr->AddResult( SafeUMSerializableConvert( result.ptr( ) ) );
    }
    catch ( GemStone::GemFire::Cache::Generic::GemFireException^ ex ) {
      ex->ThrowNative( );
    }
    catch ( System::Exception^ ex ) {
      GemStone::GemFire::Cache::Generic::ManagedString mg_exStr( ex->ToString( ) );
      std::string ex_str = "ManagedResultCollector: Got an exception in"
        "addResult: ";
      ex_str += mg_exStr.CharPtr;
      throw gemfire::IllegalArgumentException( ex_str.c_str( ) );
    }
  }

  CacheableVectorPtr ManagedResultCollectorGeneric::getResult(uint32_t timeout)
  {
    try {
       //array<IGFSerializable^>^ rs = m_managedptr->GetResult(timeout);
       //gemfire::CacheableVectorPtr rsptr = gemfire::CacheableVector::create();
       //for( int index = 0; index < rs->Length; index++ )
       //{
       //  //gemfire::CacheablePtr valueptr(GemStone::GemFire::Cache::Generic::Serializable::GetUnmanagedValueGeneric<IGFSerializable^>(rs[ index]));
       //  gemfire::CacheablePtr valueptr (SafeMSerializableConvert(rs[ index]));
       //  rsptr->push_back(valueptr);
       //}
       //return rsptr;
      throw gemfire::IllegalStateException( "This should not be get callled.");
    }
    catch ( GemStone::GemFire::Cache::Generic::GemFireException^ ex ) {
      ex->ThrowNative( );
    }
    catch ( System::Exception^ ex ) {
      GemStone::GemFire::Cache::Generic::ManagedString mg_exStr( ex->ToString( ) );
      std::string ex_str = "ManagedResultCollector: Got an exception in"
        "getResult: ";
      ex_str += mg_exStr.CharPtr;
      throw gemfire::IllegalArgumentException( ex_str.c_str( ) );
    }
    return NULLPTR;
  }
  void ManagedResultCollectorGeneric::endResults()
  {
    try {
       m_managedptr->EndResults();
    }
    catch ( GemStone::GemFire::Cache::Generic::GemFireException^ ex ) {
      ex->ThrowNative( );
    }
    catch ( System::Exception^ ex ) {
      GemStone::GemFire::Cache::Generic::ManagedString mg_exStr( ex->ToString( ) );
      std::string ex_str = "ManagedResultCollector: Got an exception in"
        "endResults: ";
      ex_str += mg_exStr.CharPtr;
      throw gemfire::IllegalArgumentException( ex_str.c_str( ) );
    }
  }
  void ManagedResultCollectorGeneric::clearResults()
  {
    try {
       m_managedptr->ClearResults(/*false*/);
    }
    catch ( GemStone::GemFire::Cache::Generic::GemFireException^ ex ) {
      ex->ThrowNative( );
    }
    catch ( System::Exception^ ex ) {
      GemStone::GemFire::Cache::Generic::ManagedString mg_exStr( ex->ToString( ) );
      std::string ex_str = "ManagedResultCollector: Got an exception in"
        "clearResults: ";
      ex_str += mg_exStr.CharPtr;
      throw gemfire::IllegalArgumentException( ex_str.c_str( ) );
    }
  }
}
