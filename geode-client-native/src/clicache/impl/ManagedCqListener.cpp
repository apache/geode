/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "../gf_includes.hpp"
#include "ManagedCqListener.hpp"
#include "../ICqListener.hpp"
#include "../CqEventM.hpp"
#include "ManagedString.hpp"
#include "../ExceptionTypesM.hpp"


using namespace System;
using namespace System::Reflection;

namespace gemfire
{

  CqListener* ManagedCqListener::create( const char* assemblyPath,
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
        std::string ex_str = "ManagedCqListener: Factory function name '";
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
        std::string ex_str = "ManagedCqListener: Could not load assembly: ";
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
          GemStone::GemFire::Cache::ICqListener^ managedptr = nullptr;
          try
          {
            managedptr = dynamic_cast<GemStone::GemFire::Cache::ICqListener^>(
              mInfo->Invoke( typeInst, nullptr ) );
          }
          catch (System::Exception^)
          {
            managedptr = nullptr;
          }
          if (managedptr == nullptr)
          {
            std::string ex_str = "ManagedCqListener: Could not create "
              "object on invoking factory function [";
            ex_str += factoryFunctionName;
            ex_str += "] in assembly: ";
            ex_str += assemblyPath;
            throw IllegalArgumentException( ex_str.c_str( ) );
          }
          return new ManagedCqListener( managedptr );
        }
        else
        {
          std::string ex_str = "ManagedCqListener: Could not load "
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
        std::string ex_str = "ManagedCqListener: Could not load type [";
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
      std::string ex_str = "ManagedCqListener: Got an exception while "
        "loading managed library: ";
      ex_str += mg_exStr.CharPtr;
      throw IllegalArgumentException( ex_str.c_str( ) );
    }
    return NULL;
  }

  void ManagedCqListener::onEvent( const CqEvent& ev )
  {
    try {
      GemStone::GemFire::Cache::CqEvent mevent( &ev );
      m_managedptr->OnEvent( %mevent );
    }
    catch ( GemStone::GemFire::Cache::GemFireException^ ex ) {
      ex->ThrowNative( );
    }
    catch ( System::Exception^ ex ) {
      GemStone::GemFire::ManagedString mg_exStr( ex->ToString( ) );
      std::string ex_str = "ManagedCqListener: Got an exception in"
        "onEvent: ";
      ex_str += mg_exStr.CharPtr;
      throw IllegalArgumentException( ex_str.c_str( ) );
    }
  }

  void ManagedCqListener::onError( const CqEvent& ev )
  {
    GemStone::GemFire::Cache::CqEvent mevent( &ev );
    m_managedptr->OnError( %mevent );
  }

  void ManagedCqListener::close()
  {
    try {
      m_managedptr->Close();
    }
    catch ( GemStone::GemFire::Cache::GemFireException^ ex ) {
      ex->ThrowNative( );
    }
    catch ( System::Exception^ ex ) {
      GemStone::GemFire::ManagedString mg_exStr( ex->ToString( ) );
      std::string ex_str = "ManagedCqListener: Got an exception in"
        "close: ";
      ex_str += mg_exStr.CharPtr;
      throw IllegalArgumentException( ex_str.c_str( ) );
    }
  }

}
