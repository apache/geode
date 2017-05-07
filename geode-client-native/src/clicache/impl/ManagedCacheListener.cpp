/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "../gf_includes.hpp"
#include "ManagedCacheListener.hpp"
#include "../ICacheListener.hpp"
#include "../EntryEventM.hpp"
#include "../RegionEventM.hpp"
#include "../RegionM.hpp"


using namespace System;
using namespace System::Reflection;

namespace gemfire
{

  CacheListener* ManagedCacheListener::create( const char* assemblyPath,
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
        std::string ex_str = "ManagedCacheListener: Factory function name '";
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
        std::string ex_str = "ManagedCacheListener: Could not load assembly: ";
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
          GemStone::GemFire::Cache::ICacheListener^ managedptr = nullptr;
          try
          {
            managedptr = dynamic_cast<GemStone::GemFire::Cache::ICacheListener^>(
              mInfo->Invoke( typeInst, nullptr ) );
          }
          catch (System::Exception^)
          {
            managedptr = nullptr;
          }
          if (managedptr == nullptr)
          {
            std::string ex_str = "ManagedCacheListener: Could not create "
              "object on invoking factory function [";
            ex_str += factoryFunctionName;
            ex_str += "] in assembly: ";
            ex_str += assemblyPath;
            throw IllegalArgumentException( ex_str.c_str( ) );
          }
          return new ManagedCacheListener( managedptr );
        }
        else
        {
          std::string ex_str = "ManagedCacheListener: Could not load "
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
        std::string ex_str = "ManagedCacheListener: Could not load type [";
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
      std::string ex_str = "ManagedCacheListener: Got an exception while "
        "loading managed library: ";
      ex_str += mg_exStr.CharPtr;
      throw IllegalArgumentException( ex_str.c_str( ) );
    }
    return NULL;
  }

  void ManagedCacheListener::afterCreate( const EntryEvent& ev )
  {
    try {
      GemStone::GemFire::Cache::EntryEvent mevent( &ev );
      m_managedptr->AfterCreate( %mevent );
    }
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
  }

  void ManagedCacheListener::afterUpdate( const EntryEvent& ev )
  {
    try {
      GemStone::GemFire::Cache::EntryEvent mevent( &ev );
      m_managedptr->AfterUpdate( %mevent );
    }
    catch ( GemStone::GemFire::Cache::GemFireException^ ex ) {
      ex->ThrowNative( );
    }
    catch ( System::Exception^ ex ) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
  }

  void ManagedCacheListener::afterInvalidate( const EntryEvent& ev )
  {
    try {
      GemStone::GemFire::Cache::EntryEvent mevent( &ev );
      m_managedptr->AfterInvalidate( %mevent );
    }
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
  }

  void ManagedCacheListener::afterDestroy( const EntryEvent& ev )
  {
    try {
      GemStone::GemFire::Cache::EntryEvent mevent( &ev );
      m_managedptr->AfterDestroy( %mevent );
    }
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
  }
  void ManagedCacheListener::afterRegionClear( const RegionEvent& ev )
  {
    try {
      GemStone::GemFire::Cache::RegionEvent mevent( &ev );
      m_managedptr->AfterRegionClear( %mevent );
    }
    catch ( GemStone::GemFire::Cache::GemFireException^ ex ) {
      ex->ThrowNative( );
    }
    catch ( System::Exception^ ex ) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
  }

  void ManagedCacheListener::afterRegionInvalidate( const RegionEvent& ev )
  {
    try {
      GemStone::GemFire::Cache::RegionEvent mevent( &ev );
      m_managedptr->AfterRegionInvalidate( %mevent );
    }
    catch ( GemStone::GemFire::Cache::GemFireException^ ex ) {
      ex->ThrowNative( );
    }
    catch ( System::Exception^ ex ) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
  }

  void ManagedCacheListener::afterRegionDestroy( const RegionEvent& ev )
  {
    try {
      GemStone::GemFire::Cache::RegionEvent mevent( &ev );
      m_managedptr->AfterRegionDestroy( %mevent );
    }
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
  }

  void ManagedCacheListener::afterRegionLive( const RegionEvent& ev )
  {
    try {
      GemStone::GemFire::Cache::RegionEvent mevent( &ev );
      m_managedptr->AfterRegionLive( %mevent );
    }
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
  }

  void ManagedCacheListener::close( const RegionPtr& region )
  {
    try {
      GemStone::GemFire::Cache::Region^ mregion =
        GemStone::GemFire::Cache::Region::Create( region.ptr( ) );

      m_managedptr->Close( mregion );
    }
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
  }
  void ManagedCacheListener::afterRegionDisconnected( const RegionPtr& region  )
  {
    try {
      GemStone::GemFire::Cache::Region^ mregion =
        GemStone::GemFire::Cache::Region::Create( region.ptr( ) );
      m_managedptr->AfterRegionDisconnected( mregion );
    }
    catch (GemStone::GemFire::Cache::GemFireException^ ex) {
      ex->ThrowNative();
    }
    catch (System::Exception^ ex) {
      GemStone::GemFire::Cache::GemFireException::ThrowNative(ex);
    }
  }

}
