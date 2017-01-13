/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*=========================================================================
*/

#include "ManagedCqStatusListener.hpp"
#include "../ICqStatusListener.hpp"
#include "../CqEvent.hpp"

#include "ManagedString.hpp"
#include "../ExceptionTypes.hpp"
#include "SafeConvert.hpp"
#include <string>

using namespace System;
using namespace System::Text;
using namespace System::Reflection;


namespace gemfire
{

  gemfire::CqListener* ManagedCqStatusListenerGeneric::create( const char* assemblyPath,
    const char* factoryFunctionName )
  {
    try
    {
      String^ mg_assemblyPath =
        GemStone::GemFire::Cache::Generic::ManagedString::Get(assemblyPath);
      String^ mg_factoryFunctionName =
        GemStone::GemFire::Cache::Generic::ManagedString::Get(factoryFunctionName);
      String^ mg_typeName = nullptr;
      int32_t dotIndx = -1;

      if (mg_factoryFunctionName == nullptr ||
        ( dotIndx = mg_factoryFunctionName->LastIndexOf( '.' ) ) < 0 )
      {
        std::string ex_str = "ManagedCqStatusListenerGeneric: Factory function name '";
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
        std::string ex_str = "ManagedCqStatusListenerGeneric: Could not load assembly: ";
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
          GemStone::GemFire::Cache::Generic::ICqStatusListener<Object^, Object^>^ managedptr = nullptr;
          try
          {
            managedptr = dynamic_cast<GemStone::GemFire::Cache::Generic::ICqStatusListener<Object^, Object^>^>(
              mInfo->Invoke( typeInst, nullptr ) );
          }
          catch (System::Exception^)
          {
            managedptr = nullptr;
          }
          if (managedptr == nullptr)
          {
            std::string ex_str = "ManagedCqStatusListenerGeneric: Could not create "
              "object on invoking factory function [";
            ex_str += factoryFunctionName;
            ex_str += "] in assembly: ";
            ex_str += assemblyPath;
            throw IllegalArgumentException( ex_str.c_str( ) );
          }
          return new ManagedCqStatusListenerGeneric( (GemStone::GemFire::Cache::Generic::ICqListener<Object^, Object^>^)managedptr );
        }
        else
        {
          std::string ex_str = "ManagedCqStatusListenerGeneric: Could not load "
            "function with name [";
          ex_str += factoryFunctionName;
          ex_str += "] in assembly: ";
          ex_str += assemblyPath;
          throw IllegalArgumentException( ex_str.c_str( ) );
        }
      }
      else
      {
        GemStone::GemFire::Cache::Generic::ManagedString typeName(mg_typeName);
        std::string ex_str = "ManagedCqStatusListenerGeneric: Could not load type [";
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
      GemStone::GemFire::Cache::Generic::ManagedString mg_exStr(ex->ToString());
      std::string ex_str = "ManagedCqStatusListenerGeneric: Got an exception while "
        "loading managed library: ";
      ex_str += mg_exStr.CharPtr;
      throw IllegalArgumentException( ex_str.c_str( ) );
    }
    return NULL;
  }

  void ManagedCqStatusListenerGeneric::onEvent( const CqEvent& ev )
  {
    try {

      GemStone::GemFire::Cache::Generic::CqEvent<Object^, Object^> mevent( &ev );
      m_managedptr->OnEvent( %mevent );
    }
    catch ( GemStone::GemFire::Cache::Generic::GemFireException^ ex ) {
      ex->ThrowNative( );
    }
    catch ( System::Exception^ ex ) {
      GemStone::GemFire::Cache::Generic::ManagedString mg_exStr(ex->ToString());
      std::string ex_str = "ManagedCqStatusListenerGeneric: Got an exception in"
        "onEvent: ";
      ex_str += mg_exStr.CharPtr;
      throw IllegalArgumentException( ex_str.c_str( ) );
    }
  }

  void ManagedCqStatusListenerGeneric::onError( const CqEvent& ev )
  {
    GemStone::GemFire::Cache::Generic::CqEvent<Object^, Object^> mevent( &ev );
    m_managedptr->OnError( %mevent );
  }

  void ManagedCqStatusListenerGeneric::close()
  {
    try {
      m_managedptr->Close();
    }
    catch ( GemStone::GemFire::Cache::Generic::GemFireException^ ex ) {
      ex->ThrowNative( );
    }
    catch ( System::Exception^ ex ) {
      GemStone::GemFire::Cache::Generic::ManagedString mg_exStr(ex->ToString());
      std::string ex_str = "ManagedCqStatusListenerGeneric: Got an exception in"
        "close: ";
      ex_str += mg_exStr.CharPtr;
      throw IllegalArgumentException( ex_str.c_str( ) );
    }
  }

  void ManagedCqStatusListenerGeneric::onCqDisconnected()
  {
    try {
      m_managedptr->OnCqDisconnected();
    }
    catch ( GemStone::GemFire::Cache::Generic::GemFireException^ ex ) {
      ex->ThrowNative( );
    }
    catch ( System::Exception^ ex ) {
      GemStone::GemFire::Cache::Generic::ManagedString mg_exStr(ex->ToString());
      std::string ex_str = "ManagedCqStatusListenerGeneric: Got an exception in"
        "onCqDisconnected: ";
      ex_str += mg_exStr.CharPtr;
      throw IllegalArgumentException( ex_str.c_str( ) );
    }
  }

  void ManagedCqStatusListenerGeneric::onCqConnected()
  {
    try {
      m_managedptr->OnCqConnected();
    }
    catch ( GemStone::GemFire::Cache::Generic::GemFireException^ ex ) {
      ex->ThrowNative( );
    }
    catch ( System::Exception^ ex ) {
      GemStone::GemFire::Cache::Generic::ManagedString mg_exStr(ex->ToString());
      std::string ex_str = "ManagedCqStatusListenerGeneric: Got an exception in"
        "OnCqConnected: ";
      ex_str += mg_exStr.CharPtr;
      throw IllegalArgumentException( ex_str.c_str( ) );
    }
  }

}
