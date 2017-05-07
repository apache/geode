/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "RegionM.hpp"
#include "CacheM.hpp"
#include "CacheStatisticsM.hpp"
#include "RegionAttributesM.hpp"
#include "AttributesMutatorM.hpp"
#include "RegionEntryM.hpp"
#include "ISelectResults.hpp"
#include "ResultSetM.hpp"
#include "StructSetM.hpp"
#include "impl/AuthenticatedCacheM.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      String^ Region::Name::get( )
      {
        return ManagedString::Get( NativePtr->getName( ) );
      }

      String^ Region::FullPath::get( )
      {
        return ManagedString::Get( NativePtr->getFullPath( ) );
      }

      Region^ Region::ParentRegion::get( )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::RegionPtr& nativeptr( NativePtr->getParentRegion( ) );

          return Region::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      RegionAttributes^ Region::Attributes::get( )
      {
        gemfire::RegionAttributesPtr& nativeptr( NativePtr->getAttributes( ) );

        return RegionAttributes::Create( nativeptr.ptr( ) );
      }

      AttributesMutator^ Region::GetAttributesMutator( )
      {
        gemfire::AttributesMutatorPtr& nativeptr(
          NativePtr->getAttributesMutator( ) );

        return AttributesMutator::Create( nativeptr.ptr( ) );
      }

      CacheStatistics^ Region::Statistics::get( )
      {
        gemfire::CacheStatisticsPtr& nativeptr( NativePtr->getStatistics( ) );

        return CacheStatistics::Create( nativeptr.ptr( ) );
      }

      void Region::InvalidateRegion( IGFSerializable^ callback )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::UserDataPtr callbackptr(
            SafeMSerializableConvert( callback ) );

          NativePtr->invalidateRegion( callbackptr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::LocalInvalidateRegion( IGFSerializable^ callback )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::UserDataPtr callbackptr(
            SafeMSerializableConvert( callback ) );

          NativePtr->localInvalidateRegion( callbackptr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::DestroyRegion( IGFSerializable^ callback )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::UserDataPtr callbackptr(
            SafeMSerializableConvert( callback ) );

          NativePtr->destroyRegion( callbackptr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::LocalDestroyRegion( IGFSerializable^ callback )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::UserDataPtr callbackptr(
            SafeMSerializableConvert( callback ) );

          NativePtr->localDestroyRegion( callbackptr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      Region^ Region::GetSubRegion( String^ path )
      {
        _GF_MG_EXCEPTION_TRY

          ManagedString mg_path( path );

          gemfire::RegionPtr& nativeptr(
            NativePtr->getSubregion( mg_path.CharPtr ) );
          return Region::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      Region^ Region::CreateSubRegion( String^ subRegionName,
        RegionAttributes^ attributes )
      {
        _GF_MG_EXCEPTION_TRY

          ManagedString mg_subregionName( subRegionName );
          gemfire::RegionAttributesPtr p_attrs(
            GetNativePtr<gemfire::RegionAttributes>( attributes ) );

          gemfire::RegionPtr& nativeptr( NativePtr->createSubregion(
            mg_subregionName.CharPtr, p_attrs ) );
          return Region::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      array<Region^>^ Region::SubRegions( bool recursive )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::VectorOfRegion vsr;
          NativePtr->subregions( recursive, vsr );
          array<Region^>^ subRegions =
            gcnew array<Region^>( vsr.size( ) );

          for( int32_t index = 0; index < vsr.size( ); index++ )
          {
            gemfire::RegionPtr& nativeptr( vsr[ index ] );
            subRegions[ index ] = Region::Create( nativeptr.ptr( ) );
          }
          return subRegions;

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      RegionEntry^ Region::GetEntry( GemStone::GemFire::Cache::ICacheableKey^ key )
      {
        //TODO::hitesh
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyptr( SafeMKeyConvert( key ) );
          gemfire::RegionEntryPtr& nativeptr( NativePtr->getEntry( keyptr ) );

          return RegionEntry::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      RegionEntry^ Region::GetEntry( CacheableKey^ key )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyptr(
            (gemfire::CacheableKey*)GetNativePtr2<gemfire::Cacheable>( key ) );
          gemfire::RegionEntryPtr& nativeptr( NativePtr->getEntry( keyptr ) );

          return RegionEntry::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      IGFSerializable^ Region::Get( GemStone::GemFire::Cache::ICacheableKey^ key,
        IGFSerializable^ callback )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyptr( SafeMKeyConvert( key ) );
          gemfire::UserDataPtr callbackptr(
            SafeMSerializableConvert( callback ) );

          gemfire::CacheablePtr& nativeptr(
            NativePtr->get( keyptr, callbackptr ) );
          return SafeUMSerializableConvert( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL
      }
      array<GemStone::GemFire::Cache::ICacheableKey^>^ Region::GetInterestList()
      {
        _GF_MG_EXCEPTION_TRY
          gemfire::VectorOfCacheableKey vc;
          NativePtr->getInterestList( vc );
          array<GemStone::GemFire::Cache::ICacheableKey^>^ keyarr =
            gcnew array<GemStone::GemFire::Cache::ICacheableKey^>( vc.size( ) );
          for( int32_t index = 0; index < vc.size( ); index++ )
          {
            gemfire::CacheableKeyPtr& nativeptr( vc[ index ] );
            keyarr[ index ] = SafeUMKeyConvert( nativeptr.ptr( ) );
          }
          return keyarr;
        _GF_MG_EXCEPTION_CATCH_ALL
      }

      array<String^>^ Region::GetInterestListRegex()
      {
        _GF_MG_EXCEPTION_TRY
          gemfire::VectorOfCacheableString vc;
          NativePtr->getInterestListRegex( vc );
          array<String^>^ strarr =
            gcnew array<String^>( vc.size( ) );
          for( int32_t index = 0; index < vc.size( ); index++ )
          {
              strarr[index] = ManagedString::Get( vc[index]->asChar());
	  }
	  return strarr;
        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::Clear(IGFSerializable^ callback)
      {
        _GF_MG_EXCEPTION_TRY
          gemfire::UserDataPtr callbackptr(
            SafeMSerializableConvert( callback ) );
          NativePtr->clear(callbackptr );
        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::LocalClear(IGFSerializable^ callback)
      {
        _GF_MG_EXCEPTION_TRY
          gemfire::UserDataPtr callbackptr(
            SafeMSerializableConvert( callback ) );
          NativePtr->localClear(callbackptr );
        _GF_MG_EXCEPTION_CATCH_ALL
      }

      Boolean  Region::ContainsKeyOnServer( CacheableKey^ key )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyptr(
            (gemfire::CacheableKey*)GetNativePtr2<gemfire::Cacheable>( key ) );
                   
          return  NativePtr->containsKeyOnServer( keyptr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      IGFSerializable^ Region::Get( CacheableKey^ key,
        IGFSerializable^ callback )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyptr(
            (gemfire::CacheableKey*)GetNativePtr2<gemfire::Cacheable>( key ) );
          gemfire::UserDataPtr callbackptr(
            SafeMSerializableConvert( callback ) );

          gemfire::CacheablePtr& nativeptr(
            NativePtr->get( keyptr, callbackptr ) );
          return SafeUMSerializableConvert( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::Put( GemStone::GemFire::Cache::ICacheableKey^ key, IGFSerializable^ value,
        IGFSerializable^ callback )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyptr( SafeMKeyConvert( key ) );
          gemfire::CacheablePtr valueptr( SafeMSerializableConvert( value ) );
          gemfire::UserDataPtr callbackptr(
            SafeMSerializableConvert( callback ) );

          NativePtr->put( keyptr, valueptr, callbackptr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::Put( CacheableKey^ key, IGFSerializable^ value,
        IGFSerializable^ callback )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyptr(
            (gemfire::CacheableKey*)GetNativePtr2<gemfire::Cacheable>( key ) );
          gemfire::CacheablePtr valueptr( SafeMSerializableConvert( value ) );
          gemfire::UserDataPtr callbackptr(
            SafeMSerializableConvert( callback ) );

          NativePtr->put( keyptr, valueptr, callbackptr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::Put( GemStone::GemFire::Cache::ICacheableKey^ key, Serializable^ value,
        IGFSerializable^ callback )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyptr( SafeMKeyConvert( key ) );
          gemfire::CacheablePtr valueptr(
            GetNativePtr2<gemfire::Cacheable>( value ) );
          gemfire::UserDataPtr callbackptr(
            SafeMSerializableConvert( callback ) );

          NativePtr->put( keyptr, valueptr, callbackptr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::PutAll( CacheableHashMap^ map)
      {
        return PutAll( map, DEFAULT_RESPONSE_TIMEOUT );
      }

      void Region::PutAll( CacheableHashMap^ map, uint32_t timeout)
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::HashMapOfCacheable nativeMap;
          for each ( KeyValuePair<GemStone::GemFire::Cache::ICacheableKey^, IGFSerializable^> keyValPair in map )
          {
            gemfire::CacheableKeyPtr keyPtr( SafeMKeyConvert( keyValPair.Key ) );
            gemfire::SerializablePtr valPtr( SafeMSerializableConvert( keyValPair.Value ) );
            nativeMap.insert( keyPtr, valPtr );
          }
          NativePtr->putAll( nativeMap, timeout );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::Put( CacheableKey^ key, Serializable^ value,
        IGFSerializable^ callback )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyptr(
            (gemfire::CacheableKey*)GetNativePtr2<gemfire::Cacheable>( key ) );
          gemfire::CacheablePtr valueptr(
            GetNativePtr2<gemfire::Cacheable>( value ) );
          gemfire::UserDataPtr callbackptr(
            SafeMSerializableConvert( callback ) );

          NativePtr->put( keyptr, valueptr, callbackptr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::LocalPut(GemStone::GemFire::Cache::ICacheableKey^ key, IGFSerializable^ value,
        IGFSerializable^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyPtr(SafeMKeyConvert(key));
          gemfire::CacheablePtr valuePtr(SafeMSerializableConvert(value));
          gemfire::UserDataPtr callbackArgPtr(
            SafeMSerializableConvert(callbackArg));

          NativePtr->localPut(keyPtr, valuePtr, callbackArgPtr);

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::LocalPut(CacheableKey^ key, IGFSerializable^ value,
        IGFSerializable^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyPtr(
            (gemfire::CacheableKey*)GetNativePtr2<gemfire::Cacheable>(key));
          gemfire::CacheablePtr valuePtr(SafeMSerializableConvert(value));
          gemfire::UserDataPtr callbackArgPtr(
            SafeMSerializableConvert(callbackArg));

          NativePtr->localPut(keyPtr, valuePtr, callbackArgPtr);

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::LocalPut(GemStone::GemFire::Cache::ICacheableKey^ key, Serializable^ value,
        IGFSerializable^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyPtr(SafeMKeyConvert(key));
          gemfire::CacheablePtr valuePtr(
            GetNativePtr2<gemfire::Cacheable>(value));
          gemfire::UserDataPtr callbackArgPtr(
            SafeMSerializableConvert(callbackArg));

          NativePtr->localPut(keyPtr, valuePtr, callbackArgPtr);

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::LocalPut(CacheableKey^ key, Serializable^ value,
        IGFSerializable^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyPtr(
            (gemfire::CacheableKey*)GetNativePtr2<gemfire::Cacheable>(key));
          gemfire::CacheablePtr valuePtr(
            GetNativePtr2<gemfire::Cacheable>(value));
          gemfire::UserDataPtr callbackArgPtr(
            SafeMSerializableConvert(callbackArg));

          NativePtr->localPut(keyPtr, valuePtr, callbackArgPtr);

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::Create( GemStone::GemFire::Cache::ICacheableKey^ key, IGFSerializable^ value,
        IGFSerializable^ cacheWriterArg )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyptr( SafeMKeyConvert( key ) );
          gemfire::CacheablePtr valueptr( SafeMSerializableConvert( value ) );
          gemfire::UserDataPtr callbackptr(
            SafeMSerializableConvert( cacheWriterArg ) );

          NativePtr->create( keyptr, valueptr, callbackptr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::Create( CacheableKey^ key, IGFSerializable^ value,
        IGFSerializable^ cacheWriterArg )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyptr(
            (gemfire::CacheableKey*)GetNativePtr2<gemfire::Cacheable>( key ) );
          gemfire::CacheablePtr valueptr( SafeMSerializableConvert( value ) );
          gemfire::UserDataPtr callbackptr(
            SafeMSerializableConvert( cacheWriterArg ) );

          NativePtr->create( keyptr, valueptr, callbackptr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::Create( GemStone::GemFire::Cache::ICacheableKey^ key, Serializable^ value,
        IGFSerializable^ cacheWriterArg )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyptr( SafeMKeyConvert( key ) );
          gemfire::CacheablePtr valueptr(
            GetNativePtr2<gemfire::Cacheable>( value ) );
          gemfire::UserDataPtr callbackptr(
            SafeMSerializableConvert( cacheWriterArg ) );

          NativePtr->create( keyptr, valueptr, callbackptr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::Create( CacheableKey^ key, Serializable^ value,
        IGFSerializable^ cacheWriterArg )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyptr(
            (gemfire::CacheableKey*)GetNativePtr2<gemfire::Cacheable>( key ) );
          gemfire::CacheablePtr valueptr(
            GetNativePtr2<gemfire::Cacheable>( value ) );
          gemfire::UserDataPtr callbackptr(
            SafeMSerializableConvert( cacheWriterArg ) );

          NativePtr->create( keyptr, valueptr, callbackptr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::LocalCreate(GemStone::GemFire::Cache::ICacheableKey^ key, IGFSerializable^ value,
        IGFSerializable^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyPtr(SafeMKeyConvert(key));
          gemfire::CacheablePtr valuePtr(SafeMSerializableConvert(value));
          gemfire::UserDataPtr callbackArgPtr(
            SafeMSerializableConvert(callbackArg));

          NativePtr->localCreate(keyPtr, valuePtr, callbackArgPtr);

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::LocalCreate(CacheableKey^ key, IGFSerializable^ value,
        IGFSerializable^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyPtr(
            (gemfire::CacheableKey*)GetNativePtr2<gemfire::Cacheable>(key));
          gemfire::CacheablePtr valuePtr(SafeMSerializableConvert(value));
          gemfire::UserDataPtr callbackArgPtr(
            SafeMSerializableConvert(callbackArg));

          NativePtr->localCreate(keyPtr, valuePtr, callbackArgPtr);

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::LocalCreate(GemStone::GemFire::Cache::ICacheableKey^ key, Serializable^ value,
        IGFSerializable^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyPtr(SafeMKeyConvert(key));
          gemfire::CacheablePtr valuePtr(
            GetNativePtr2<gemfire::Cacheable>(value));
          gemfire::UserDataPtr callbackArgPtr(
            SafeMSerializableConvert(callbackArg));

          NativePtr->localCreate(keyPtr, valuePtr, callbackArgPtr);

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::LocalCreate(CacheableKey^ key, Serializable^ value,
        IGFSerializable^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyPtr(
            (gemfire::CacheableKey*)GetNativePtr2<gemfire::Cacheable>(key));
          gemfire::CacheablePtr valuePtr(
            GetNativePtr2<gemfire::Cacheable>(value));
          gemfire::UserDataPtr callbackArgPtr(
            SafeMSerializableConvert(callbackArg));

          NativePtr->localCreate(keyPtr, valuePtr, callbackArgPtr);

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::Invalidate( GemStone::GemFire::Cache::ICacheableKey^ key,
        IGFSerializable^ callback )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyptr( SafeMKeyConvert( key ) );
          gemfire::UserDataPtr callbackptr(
            SafeMSerializableConvert( callback ) );

          NativePtr->invalidate( keyptr, callbackptr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::Invalidate( CacheableKey^ key,
        IGFSerializable^ callback )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyptr(
            (gemfire::CacheableKey*)GetNativePtr2<gemfire::Cacheable>( key ) );
          gemfire::UserDataPtr callbackptr(
            SafeMSerializableConvert( callback ) );

          NativePtr->invalidate( keyptr, callbackptr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::LocalInvalidate( GemStone::GemFire::Cache::ICacheableKey^ key,
        IGFSerializable^ callback )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyptr( SafeMKeyConvert( key ) );
          gemfire::UserDataPtr callbackptr(
            SafeMSerializableConvert( callback ) );

          NativePtr->localInvalidate( keyptr, callbackptr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::LocalInvalidate( CacheableKey^ key,
        IGFSerializable^ callback )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyptr(
            (gemfire::CacheableKey*)GetNativePtr2<gemfire::Cacheable>( key ) );
          gemfire::UserDataPtr callbackptr(
            SafeMSerializableConvert( callback ) );

          NativePtr->localInvalidate( keyptr, callbackptr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::Destroy( GemStone::GemFire::Cache::ICacheableKey^ key,
        IGFSerializable^ cacheWriterArg )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyptr( SafeMKeyConvert( key ) );
          gemfire::UserDataPtr callbackptr(
            SafeMSerializableConvert( cacheWriterArg ) );

          NativePtr->destroy( keyptr, callbackptr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::Destroy( CacheableKey^ key,
        IGFSerializable^ cacheWriterArg )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyptr(
            (gemfire::CacheableKey*)GetNativePtr2<gemfire::Cacheable>( key ) );
          gemfire::UserDataPtr callbackptr(
            SafeMSerializableConvert( cacheWriterArg ) );

          NativePtr->destroy( keyptr, callbackptr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::LocalDestroy( GemStone::GemFire::Cache::ICacheableKey^ key,
        IGFSerializable^ cacheListenerArg )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyptr( SafeMKeyConvert( key ) );
          gemfire::UserDataPtr callbackptr(
            SafeMSerializableConvert( cacheListenerArg ) );

          NativePtr->localDestroy( keyptr, callbackptr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::LocalDestroy( CacheableKey^ key,
        IGFSerializable^ cacheListenerArg )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyptr(
            (gemfire::CacheableKey*)GetNativePtr2<gemfire::Cacheable>( key ) );
          gemfire::UserDataPtr callbackptr(
            SafeMSerializableConvert( cacheListenerArg ) );

          NativePtr->localDestroy( keyptr, callbackptr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      array<GemStone::GemFire::Cache::ICacheableKey^>^ Region::GetKeys( )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::VectorOfCacheableKey vc;
          NativePtr->keys( vc );
          array<GemStone::GemFire::Cache::ICacheableKey^>^ keyarr =
            gcnew array<GemStone::GemFire::Cache::ICacheableKey^>( vc.size( ) );

          for( int32_t index = 0; index < vc.size( ); index++ )
          {
            gemfire::CacheableKeyPtr& nativeptr( vc[index] );
            keyarr[ index ] = SafeUMKeyConvert( nativeptr.ptr( ) );
          }
          return keyarr;

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      array<GemStone::GemFire::Cache::ICacheableKey^>^ Region::GetServerKeys( )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::VectorOfCacheableKey vc;
          NativePtr->serverKeys( vc );
          array<GemStone::GemFire::Cache::ICacheableKey^>^ keyarr =
            gcnew array<GemStone::GemFire::Cache::ICacheableKey^>( vc.size( ) );
          for( int32_t index = 0; index < vc.size( ); index++ )
          {
            gemfire::CacheableKeyPtr& nativeptr( vc[ index ] );
            keyarr[ index ] = SafeUMKeyConvert( nativeptr.ptr( ) );
          }
          return keyarr;

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      array<IGFSerializable^>^ Region::GetValues( )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::VectorOfCacheable vc;
          NativePtr->values( vc );
          array<IGFSerializable^>^ valarr =
            gcnew array<IGFSerializable^>( vc.size( ) );
          for( int32_t index = 0; index < vc.size( ); index++ )
          {
            gemfire::CacheablePtr& nativeptr( vc[ index ] );
            valarr[ index ] = SafeUMSerializableConvert( nativeptr.ptr( ) );
          }
          return valarr;

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      array<RegionEntry^>^ Region::GetEntries( bool recursive )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::VectorOfRegionEntry vc;
          NativePtr->entries( vc, recursive );
          array<RegionEntry^>^ entryarr = gcnew array<RegionEntry^>( vc.size( ) );

          for( int32_t index = 0; index < vc.size( ); index++ )
          {
            gemfire::RegionEntryPtr& nativeptr( vc[ index ] );
            entryarr[ index ] = RegionEntry::Create( nativeptr.ptr( ) );
          }
          return entryarr;

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      uint32_t Region::Size::get()
      {
	    _GF_MG_EXCEPTION_TRY
        return NativePtr->size();
		_GF_MG_EXCEPTION_CATCH_ALL
      }

      GemStone::GemFire::Cache::Cache^ Region::Cache::get( )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CachePtr& nativeptr( NativePtr->getCache( ) );

          return GemStone::GemFire::Cache::Cache::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      GemStone::GemFire::Cache::IRegionService^ Region::RegionService::get( )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::RegionServicePtr& nativeptr( NativePtr->getRegionService( ) );

        gemfire::Cache* realCache = dynamic_cast<gemfire::Cache*>(nativeptr.ptr());

        if(realCache != NULL)
        {
          return GemStone::GemFire::Cache::Cache::Create( ((gemfire::CachePtr)nativeptr).ptr( ) );
        }
        else
        {
          return GemStone::GemFire::Cache::AuthenticatedCache::Create( nativeptr.ptr( ) );
        }

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      bool Region::IsDestroyed::get( )
      {
        return NativePtr->isDestroyed( );
      }

      bool Region::ContainsValueForKey( GemStone::GemFire::Cache::ICacheableKey^ key )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyptr( SafeMKeyConvert( key ) );

          return NativePtr->containsValueForKey( keyptr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      bool Region::ContainsValueForKey( CacheableKey^ key )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyptr(
            (gemfire::CacheableKey*)GetNativePtr2<gemfire::Cacheable>( key ) );

          return NativePtr->containsValueForKey( keyptr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      bool Region::ContainsKey( GemStone::GemFire::Cache::ICacheableKey^ key )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyptr( SafeMKeyConvert( key ) );

          return NativePtr->containsKey( keyptr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      bool Region::ContainsKey( CacheableKey^ key )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyptr(
            (gemfire::CacheableKey*)GetNativePtr2<gemfire::Cacheable>( key ) );

          return NativePtr->containsKey( keyptr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::RegisterKeys(array<GemStone::GemFire::Cache::ICacheableKey^>^ keys,
        bool isDurable, bool getInitialValues, bool receiveValues)
      {
        if (keys != nullptr)
        {
          _GF_MG_EXCEPTION_TRY

            gemfire::VectorOfCacheableKey vecKeys;

            for (int32_t index = 0; index < keys->Length; index++)
            {
              vecKeys.push_back(gemfire::CacheableKeyPtr(
                SafeMKeyConvert(keys[index])));
            }
            NativePtr->registerKeys(vecKeys, isDurable, getInitialValues, receiveValues);

          _GF_MG_EXCEPTION_CATCH_ALL
        }
      }

      void Region::RegisterKeys(array<CacheableKey^>^ keys,
        bool isDurable, bool getInitialValues, bool receiveValues)
      {
        if (keys != nullptr)
        {
          _GF_MG_EXCEPTION_TRY

            gemfire::VectorOfCacheableKey vecKeys;

            for (int32_t index = 0; index < keys->Length; index++)
            {
              vecKeys.push_back(gemfire::CacheableKeyPtr(
                (gemfire::CacheableKey*)GetNativePtr2<gemfire::Cacheable>(
                keys[index])));
            }
            NativePtr->registerKeys(vecKeys, isDurable, getInitialValues, receiveValues);

          _GF_MG_EXCEPTION_CATCH_ALL
        }
      }

      void Region::UnregisterKeys(array<GemStone::GemFire::Cache::ICacheableKey^>^ keys)
      {
        if (keys != nullptr)
        {
          _GF_MG_EXCEPTION_TRY

            gemfire::VectorOfCacheableKey vecKeys;

            for (int32_t index = 0; index < keys->Length; index++)
            {
              vecKeys.push_back(gemfire::CacheableKeyPtr(
                SafeMKeyConvert(keys[index])));
            }
            NativePtr->unregisterKeys(vecKeys);

          _GF_MG_EXCEPTION_CATCH_ALL
        }
      }

      void Region::UnregisterKeys(array<CacheableKey^>^ keys)
      {
        if (keys != nullptr)
        {
          _GF_MG_EXCEPTION_TRY

            gemfire::VectorOfCacheableKey vecKeys;

            for (int32_t index = 0; index < keys->Length; index++)
            {
              vecKeys.push_back(gemfire::CacheableKeyPtr(
                (gemfire::CacheableKey*)GetNativePtr2<gemfire::Cacheable>(
                keys[index])));
            }
            NativePtr->unregisterKeys(vecKeys);

          _GF_MG_EXCEPTION_CATCH_ALL
        }
      }

      void Region::RegisterAllKeys(bool isDurable,
          List<GemStone::GemFire::Cache::ICacheableKey^>^ resultKeys, bool getInitialValues, bool receiveValues)
      {
        _GF_MG_EXCEPTION_TRY

          if (resultKeys != nullptr) {
            gemfire::VectorOfCacheableKeyPtr mg_keys(
              new gemfire::VectorOfCacheableKey());
            NativePtr->registerAllKeys(isDurable, mg_keys, getInitialValues, receiveValues);
            for (int32_t index = 0; index < mg_keys->size(); ++index) {
              resultKeys->Add(SafeUMKeyConvert(
                mg_keys->operator[](index).ptr()));
            }
          }
          else {
            NativePtr->registerAllKeys(isDurable, NULLPTR, getInitialValues, receiveValues);
          }

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::UnregisterAllKeys( )
      {
        _GF_MG_EXCEPTION_TRY

          NativePtr->unregisterAllKeys( );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::RegisterRegex(String^ regex, bool isDurable,
          List<GemStone::GemFire::Cache::ICacheableKey^>^ resultKeys, bool getInitialValues, bool receiveValues)
      {
        _GF_MG_EXCEPTION_TRY

          ManagedString mg_regex( regex );
          if (resultKeys != nullptr) {
            gemfire::VectorOfCacheableKeyPtr mg_keys(
              new gemfire::VectorOfCacheableKey());
            NativePtr->registerRegex(mg_regex.CharPtr, isDurable,
              mg_keys, getInitialValues, receiveValues);
            for (int32_t index = 0; index < mg_keys->size(); ++index) {
              resultKeys->Add(SafeUMKeyConvert(
                mg_keys->operator [](index).ptr()));
            }
          }
          else {
            NativePtr->registerRegex(mg_regex.CharPtr, isDurable,
              NULLPTR, getInitialValues, receiveValues);
          }

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::UnregisterRegex( String^ regex )
      {
        _GF_MG_EXCEPTION_TRY

          ManagedString mg_regex( regex );
          NativePtr->unregisterRegex( mg_regex.CharPtr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      void Region::GetAll(array<GemStone::GemFire::Cache::ICacheableKey^>^ keys,
        Dictionary<GemStone::GemFire::Cache::ICacheableKey^, IGFSerializable^>^ values,
        Dictionary<GemStone::GemFire::Cache::ICacheableKey^, System::Exception^>^ exceptions,
        bool addToLocalCache)
      {
        if (keys != nullptr) {
          _GF_MG_EXCEPTION_TRY

            gemfire::VectorOfCacheableKey vecKeys;
            for (int32_t index = 0; index < keys->Length; ++index) {
              vecKeys.push_back(gemfire::CacheableKeyPtr(
                SafeMKeyConvert(keys[index])));
            }
            gemfire::HashMapOfCacheablePtr valuesPtr(NULLPTR);
            if (values != nullptr) {
              valuesPtr = new gemfire::HashMapOfCacheable();
            }
            gemfire::HashMapOfExceptionPtr exceptionsPtr(NULLPTR);
            if (exceptions != nullptr) {
              exceptionsPtr = new gemfire::HashMapOfException();
            }
            NativePtr->getAll(vecKeys, valuesPtr, exceptionsPtr,
              addToLocalCache);
            if (values != nullptr) {
              for (gemfire::HashMapOfCacheable::Iterator iter =
                valuesPtr->begin(); iter != valuesPtr->end(); ++iter) {
                  GemStone::GemFire::Cache::ICacheableKey^ key = SafeUMKeyConvert(iter.first().ptr());
                  IGFSerializable^ val = SafeUMSerializableConvert(
                    iter.second().ptr());
                  values->Add(key, val);
              }
            }
            if (exceptions != nullptr) {
              for (gemfire::HashMapOfException::Iterator iter =
                exceptionsPtr->begin(); iter != exceptionsPtr->end(); ++iter) {
                  GemStone::GemFire::Cache::ICacheableKey^ key = SafeUMKeyConvert(iter.first().ptr());
                  System::Exception^ ex = GemFireException::Get(*iter.second());
                  exceptions->Add(key, ex);
              }
            }

          _GF_MG_EXCEPTION_CATCH_ALL
        }
        else {
          throw gcnew IllegalArgumentException("GetAll: null keys provided");
        }
      }

      void Region::GetAll(array<CacheableKey^>^ keys,
        Dictionary<GemStone::GemFire::Cache::ICacheableKey^, IGFSerializable^>^ values,
        Dictionary<GemStone::GemFire::Cache::ICacheableKey^, System::Exception^>^ exceptions,
        bool addToLocalCache)
      {
        if (keys != nullptr) {
          _GF_MG_EXCEPTION_TRY

            gemfire::VectorOfCacheableKey vecKeys;
            for (int32_t index = 0; index < keys->Length; ++index) {
              vecKeys.push_back(gemfire::CacheableKeyPtr(
                (gemfire::CacheableKey*)GetNativePtr2<gemfire::Cacheable>(
                keys[index])));
            }
            gemfire::HashMapOfCacheablePtr valuesPtr(NULLPTR);
            if (values != nullptr) {
              valuesPtr = new gemfire::HashMapOfCacheable();
            }
            gemfire::HashMapOfExceptionPtr exceptionsPtr(NULLPTR);
            if (exceptions != nullptr) {
              exceptionsPtr = new gemfire::HashMapOfException();
            }
            NativePtr->getAll(vecKeys, valuesPtr, exceptionsPtr,
              addToLocalCache);
            if (values != nullptr) {
              for (gemfire::HashMapOfCacheable::Iterator iter =
                valuesPtr->begin(); iter != valuesPtr->end(); ++iter) {
                  GemStone::GemFire::Cache::ICacheableKey^ key = SafeUMKeyConvert(iter.first().ptr());
                  IGFSerializable^ val = SafeUMSerializableConvert(
                    iter.second().ptr());
                  values->Add(key, val);
              }
            }
            if (exceptions != nullptr) {
              for (gemfire::HashMapOfException::Iterator iter =
                exceptionsPtr->begin(); iter != exceptionsPtr->end(); ++iter) {
                  GemStone::GemFire::Cache::ICacheableKey^ key = SafeUMKeyConvert(iter.first().ptr());
                  System::Exception^ ex = GemFireException::Get(*iter.second());
                  exceptions->Add(key, ex);
              }
            }

          _GF_MG_EXCEPTION_CATCH_ALL
        }
        else {
          throw gcnew IllegalArgumentException("GetAll: null keys provided");
        }
      }

      ISelectResults^ Region::Query( String^ predicate )
      {
        return Query( predicate, DEFAULT_QUERY_RESPONSE_TIMEOUT );
      }

      ISelectResults^ Region::Query( String^ predicate, uint32_t timeout )
      {
        ManagedString mg_predicate( predicate );

        _GF_MG_EXCEPTION_TRY

          gemfire::SelectResultsPtr& nativeptr = NativePtr->query(
            mg_predicate.CharPtr, timeout );
          if ( nativeptr.ptr( ) == NULL ) return nullptr;

          gemfire::ResultSet* resultptr = dynamic_cast<gemfire::ResultSet*>(
            nativeptr.ptr( ) );
          if ( resultptr == NULL )
          {
            gemfire::StructSet* structptr = dynamic_cast<gemfire::StructSet*>(
              nativeptr.ptr( ) );
            if ( structptr == NULL )
            {
              return nullptr;
            }
            return StructSet::Create(structptr);
          }
          else
          {
            return ResultSet::Create(resultptr);
          }

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      bool Region::ExistsValue( String^ predicate )
      {
        return ExistsValue( predicate, DEFAULT_QUERY_RESPONSE_TIMEOUT );
      }

      bool Region::ExistsValue( String^ predicate, uint32_t timeout )
      {
        ManagedString mg_predicate( predicate );

        _GF_MG_EXCEPTION_TRY

          return NativePtr->existsValue( mg_predicate.CharPtr, timeout );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      IGFSerializable^ Region::SelectValue( String^ predicate )
      {
        return SelectValue( predicate, DEFAULT_QUERY_RESPONSE_TIMEOUT );
      }

      IGFSerializable^ Region::SelectValue( String^ predicate, uint32_t timeout )
      {
        ManagedString mg_predicate( predicate );

        _GF_MG_EXCEPTION_TRY

          gemfire::CacheablePtr& nativeptr( NativePtr->selectValue(
            mg_predicate.CharPtr, timeout ) );
          return SafeUMSerializableConvert( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      //--------------------------Remove api----------------------------------------------

      bool Region::Remove( GemStone::GemFire::Cache::ICacheableKey^ key, IGFSerializable^ value,
        IGFSerializable^ cacheWriterArg )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyptr( SafeMKeyConvert( key ) );
          gemfire::CacheablePtr valueptr( SafeMSerializableConvert( value ) );
          gemfire::UserDataPtr callbackptr(
            SafeMSerializableConvert( cacheWriterArg ) );

          return NativePtr->remove( keyptr, valueptr, callbackptr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      bool Region::Remove( CacheableKey^ key, IGFSerializable^ value,
        IGFSerializable^ cacheWriterArg )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyptr(
            (gemfire::CacheableKey*)GetNativePtr2<gemfire::Cacheable>( key ) );
          gemfire::CacheablePtr valueptr( SafeMSerializableConvert( value ) );
          gemfire::UserDataPtr callbackptr(
            SafeMSerializableConvert( cacheWriterArg ) );

          return NativePtr->remove( keyptr, valueptr, callbackptr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      bool Region::Remove( GemStone::GemFire::Cache::ICacheableKey^ key, Serializable^ value,
        IGFSerializable^ cacheWriterArg )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyptr( SafeMKeyConvert( key ) );
          gemfire::CacheablePtr valuePtr ( GetNativePtr2<gemfire::Cacheable>(value));
          gemfire::UserDataPtr callbackptr(
            SafeMSerializableConvert( cacheWriterArg ) );

          return NativePtr->remove( keyptr, valuePtr, callbackptr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      bool Region::Remove( CacheableKey^ key, Serializable^ value,
        IGFSerializable^ cacheWriterArg )
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyptr(
            (gemfire::CacheableKey*)GetNativePtr2<gemfire::Cacheable>( key ) );          
          gemfire::CacheablePtr valuePtr ( GetNativePtr2<gemfire::Cacheable>(value));          
          gemfire::UserDataPtr callbackptr(
            SafeMSerializableConvert( cacheWriterArg ) );

          return NativePtr->remove( keyptr, valuePtr, callbackptr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      bool Region::LocalRemove(GemStone::GemFire::Cache::ICacheableKey^ key, IGFSerializable^ value,
        IGFSerializable^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyPtr(SafeMKeyConvert(key));
          gemfire::CacheablePtr valuePtr(SafeMSerializableConvert(value));
          gemfire::UserDataPtr callbackArgPtr(
            SafeMSerializableConvert(callbackArg));

          return NativePtr->localRemove(keyPtr, valuePtr, callbackArgPtr);

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      bool Region::LocalRemove(CacheableKey^ key, IGFSerializable^ value,
        IGFSerializable^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyPtr(
            (gemfire::CacheableKey*)GetNativePtr2<gemfire::Cacheable>(key));
          gemfire::CacheablePtr valuePtr(SafeMSerializableConvert(value));
          gemfire::UserDataPtr callbackArgPtr(
            SafeMSerializableConvert(callbackArg));

          return NativePtr->localRemove(keyPtr, valuePtr, callbackArgPtr);

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      bool Region::LocalRemove(GemStone::GemFire::Cache::ICacheableKey^ key, Serializable^ value,
        IGFSerializable^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyPtr(SafeMKeyConvert(key));
          gemfire::CacheablePtr valuePtr ( GetNativePtr2<gemfire::Cacheable>(value));
          gemfire::UserDataPtr callbackArgPtr(
            SafeMSerializableConvert(callbackArg));

          return NativePtr->localRemove(keyPtr, valuePtr, callbackArgPtr);

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      bool Region::LocalRemove(CacheableKey^ key, Serializable^ value,
        IGFSerializable^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY

          gemfire::CacheableKeyPtr keyPtr(
            (gemfire::CacheableKey*)GetNativePtr2<gemfire::Cacheable>(key));
          gemfire::CacheablePtr valuePtr ( GetNativePtr2<gemfire::Cacheable>(value));
          gemfire::UserDataPtr callbackArgPtr(
            SafeMSerializableConvert(callbackArg));

          return NativePtr->localRemove(keyPtr, valuePtr, callbackArgPtr);

        _GF_MG_EXCEPTION_CATCH_ALL
      }
    }
  }
}
