/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "gf_includes.hpp"
#include "LocalRegion.hpp"
#include "Cache.hpp"
#include "CacheStatistics.hpp"
#include "AttributesMutator.hpp"
#include "RegionEntry.hpp"
#include "impl/AuthenticatedCache.hpp"
#include "impl/SafeConvert.hpp"
//#include <gfcpp/Serializable.hpp>
//#include <cppcache/DataOutPut.hpp>

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      generic<class TKey, class TValue>
      TValue LocalRegion<TKey, TValue>::Get(TKey key, Object^ callbackArg)
      {
        gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( key ) );
        gemfire::CacheablePtr nativeptr(this->getRegionEntryValue(keyptr));
        if (nativeptr == NULLPTR)
        {
          throw gcnew KeyNotFoundException("The given key was not present in the region");
        }
        TValue returnVal = Serializable::GetManagedValueGeneric<TValue>( nativeptr );
        return returnVal;        
      }     

      generic<class TKey, class TValue>
      gemfire::SerializablePtr LocalRegion<TKey, TValue>::getRegionEntryValue(gemfire::CacheableKeyPtr& keyptr)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          gemfire::RegionEntryPtr entryPtr =  NativePtr->getEntry( keyptr );
          if (entryPtr != NULLPTR) {
            return entryPtr->getValue() ;
          }
          else {
            return NULLPTR;
          }
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void LocalRegion<TKey, TValue>::Put(TKey key, TValue value, Object^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

        gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( key ) );
        gemfire::CacheablePtr valueptr( Serializable::GetUnmanagedValueGeneric<TValue>( value ) );        
        gemfire::UserDataPtr callbackptr(
          Serializable::GetUnmanagedValueGeneric<Object^>( callbackArg ) );
        NativePtr->localPut( keyptr, valueptr, callbackptr );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      TValue LocalRegion<TKey, TValue>::default::get(TKey key)
      { 
        gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( key ) );
        gemfire::CacheablePtr nativeptr(this->getRegionEntryValue(keyptr));
        if (nativeptr == NULLPTR)
        {
          throw gcnew KeyNotFoundException("The given key was not present in the region");
        }
        TValue returnVal = Serializable::GetManagedValueGeneric<TValue>( nativeptr );
        return returnVal;
      }

      generic<class TKey, class TValue>      
      void LocalRegion<TKey, TValue>::default::set(TKey key, TValue value)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

        gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( key ) );
        gemfire::CacheablePtr valueptr( Serializable::GetUnmanagedValueGeneric<TValue>( value ) );
        NativePtr->localPut( keyptr, valueptr );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      System::Collections::Generic::IEnumerator<KeyValuePair<TKey,TValue>>^ 
        LocalRegion<TKey, TValue>::GetEnumerator()
      {
        array<KeyValuePair<TKey,TValue>>^ toArray;
        gemfire::VectorOfRegionEntry vc;

        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          NativePtr->entries( vc, false );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */ 

          toArray = gcnew array<KeyValuePair<TKey,TValue>>(vc.size());

        for( int32_t index = 0; index < vc.size( ); index++ )
        {
          gemfire::RegionEntryPtr nativeptr =  vc[ index ];  
          TKey key = Serializable::GetManagedValueGeneric<TKey> (nativeptr->getKey());
          TValue val = Serializable::GetManagedValueGeneric<TValue> (nativeptr->getValue());
          toArray[ index ] = KeyValuePair<TKey,TValue>(key, val);           
        }                      
        return ((System::Collections::Generic::IEnumerable<KeyValuePair<TKey,TValue>>^)toArray)->GetEnumerator();
      }

      generic<class TKey, class TValue>
      System::Collections::IEnumerator^ 
        LocalRegion<TKey, TValue>::GetEnumeratorOld()
      {
        array<Object^>^ toArray;
        gemfire::VectorOfRegionEntry vc;

        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          NativePtr->entries( vc, false );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */

          toArray = gcnew array<Object^>(vc.size());

        for( int32_t index = 0; index < vc.size( ); index++ )
        {
          gemfire::RegionEntryPtr nativeptr =  vc[ index ];                       
          TKey key = Serializable::GetManagedValueGeneric<TKey> (nativeptr->getKey());
          TValue val = Serializable::GetManagedValueGeneric<TValue> (nativeptr->getValue());            
          toArray[ index ] = KeyValuePair<TKey,TValue>(key, val);           
        }
        return ((System::Collections::Generic::IEnumerable<Object^>^)toArray)->GetEnumerator();        
      }


      generic<class TKey, class TValue>
      bool LocalRegion<TKey, TValue>::AreValuesEqual(gemfire::CacheablePtr& val1, gemfire::CacheablePtr& val2)
      {
        if ( val1 == NULLPTR && val2 == NULLPTR )
        {
          return true;
        }
        else if ((val1 == NULLPTR && val2 != NULLPTR) || (val1 != NULLPTR && val2 == NULLPTR))
        {
          return false;
        }
        else if( val1 != NULLPTR && val2 != NULLPTR )
        {
          if (val1->classId() != val2->classId() || val1->typeId() != val2->typeId())
          {
            return false;
          }
          gemfire::DataOutput out1;
          gemfire::DataOutput out2;
          val1->toData(out1);
          val2->toData(out2);
          if ( out1.getBufferLength() != out2.getBufferLength() )
          {
            return false;
          }
          else if (memcmp(out1.getBuffer(), out2.getBuffer(), out1.getBufferLength()) != 0)
          {
            return false;
          }
          return true;
        }
        return false;
      }

      generic<class TKey, class TValue> 
      bool LocalRegion<TKey, TValue>::Contains(KeyValuePair<TKey,TValue> keyValuePair) 
      { 
        gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( keyValuePair.Key ) ); 
        gemfire::CacheablePtr nativeptr(this->getRegionEntryValue(keyptr));
        //This means that key is not present.
        if (nativeptr == NULLPTR) {
          return false;
        }        
        TValue value = Serializable::GetManagedValueGeneric<TValue>(nativeptr);
        return ((Object^)value)->Equals(keyValuePair.Value);
      } 

      generic<class TKey, class TValue>
      bool LocalRegion<TKey, TValue>::ContainsKey(TKey key)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( key ) );          

          return NativePtr->containsKey( keyptr );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      bool LocalRegion<TKey, TValue>::TryGetValue(TKey key, TValue %val)
      {        
        gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( key ) );
        gemfire::CacheablePtr nativeptr(this->getRegionEntryValue(keyptr));
        if (nativeptr == NULLPTR) {            
          val = TValue();
          return false;
        }
        else {
          val = Serializable::GetManagedValueGeneric<TValue>( nativeptr );
          return true;
        }          
      }      

      generic<class TKey, class TValue>
      System::Collections::Generic::ICollection<TKey>^ LocalRegion<TKey, TValue>::Keys::get()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

        gemfire::VectorOfCacheableKey vc;
        NativePtr->keys( vc );
        //List<TKey>^ collectionlist = gcnew List<TKey>(vc.size());
        array<TKey>^ keyarr =
          gcnew array<TKey>( vc.size( ) );
        for( int32_t index = 0; index < vc.size( ); index++ )
        {            
          gemfire::CacheableKeyPtr& nativeptr( vc[ index ] );
          keyarr[ index ] = Serializable::GetManagedValueGeneric<TKey>(nativeptr);
          //collectionlist[ index ] = Serializable::GetManagedValue<TKey>(nativeptr);
        }
        System::Collections::Generic::ICollection<TKey>^ collectionlist = (System::Collections::Generic::ICollection<TKey>^)keyarr;
        return collectionlist;

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      System::Collections::Generic::ICollection<TValue>^ LocalRegion<TKey, TValue>::Values::get()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::VectorOfCacheable vc;
          NativePtr->values( vc );
          //List<TValue>^ collectionlist = gcnew List<TValue>(vc.size());
          array<TValue>^ valarr =
            gcnew array<TValue>( vc.size( ) );
          for( int32_t index = 0; index < vc.size( ); index++ )
          {
            gemfire::CacheablePtr& nativeptr( vc[ index ] );            
            valarr[ index ] = Serializable::GetManagedValueGeneric<TValue>(nativeptr);
            //collectionlist[ index ] = Serializable::GetManagedValueGeneric<TValue>(nativeptr);
          }
          System::Collections::Generic::ICollection<TValue>^ collectionlist = (System::Collections::Generic::ICollection<TValue>^)valarr;
          return collectionlist;

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void LocalRegion<TKey, TValue>::Add(TKey key, TValue value)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( key ) );
          gemfire::CacheablePtr valueptr( Serializable::GetUnmanagedValueGeneric<TValue>( value ) );
          NativePtr->localCreate( keyptr, valueptr );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void LocalRegion<TKey, TValue>::Add(KeyValuePair<TKey, TValue> keyValuePair)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( keyValuePair.Key ) );
          gemfire::CacheablePtr valueptr( Serializable::GetUnmanagedValueGeneric<TValue>( keyValuePair.Value ) );
          NativePtr->localCreate( keyptr, valueptr );

       _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void LocalRegion<TKey, TValue>::Add(TKey key, TValue value, Object^ callbackArg)
      {
          _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( key ) );
          gemfire::CacheablePtr valueptr( Serializable::GetUnmanagedValueGeneric<TValue>( value ) );          
          gemfire::UserDataPtr callbackptr(
            Serializable::GetUnmanagedValueGeneric<Object^>( callbackArg ) );
          NativePtr->localCreate( keyptr, valueptr, callbackptr );

         _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      bool LocalRegion<TKey, TValue>::Remove(TKey key)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
    
          try
          {
            gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( key ) );          
            NativePtr->localDestroy( keyptr );
            return true;
          }
          catch(gemfire::EntryNotFoundException /*ex*/)
          {
            return false;
          }

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
        
      }

      generic<class TKey, class TValue>
      bool LocalRegion<TKey, TValue>::Remove( TKey key, Object^ callbackArg )
      {
         _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          try
          {
            gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( key ) );                    
            gemfire::UserDataPtr callbackptr(
            Serializable::GetUnmanagedValueGeneric<Object^>( callbackArg ) );
            NativePtr->localDestroy( keyptr, callbackptr );
            return true;
          }
          catch(gemfire::EntryNotFoundException /*ex*/)
          {
            return false;
          }

          _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      bool LocalRegion<TKey, TValue>::Remove(KeyValuePair<TKey,TValue> keyValuePair)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( keyValuePair.Key ) );
          gemfire::CacheablePtr valueptr( Serializable::GetUnmanagedValueGeneric<TValue>( keyValuePair.Value ) );
          return NativePtr->localRemove(keyptr, valueptr);

         _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */

        //_GF_MG_EXCEPTION_TRY2/* due to auto replace */

        //gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( keyValuePair.Key ) );
        //if (NativePtr->containsKey( keyptr )) {
        //  gemfire::CacheablePtr nativeptr(this->getRegionEntryValue(keyptr));
        //  TValue returnVal = Serializable::GetManagedValueGeneric<TValue>( nativeptr );
        //  gemfire::CacheablePtr valueptr( Serializable::GetUnmanagedValueGeneric<TValue>( keyValuePair.Value ) );
        //  TValue actualVal = Serializable::GetManagedValueGeneric<TValue>( valueptr );
        //  if (actualVal->Equals(returnVal)) {
        //    NativePtr->localDestroy( keyptr );
        //    return true;
        //  }
        //  else {
        //    return false;
        //  }
        //}
        //else {
        //  return false;
        //} 
        //_GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      bool LocalRegion<TKey, TValue>::Remove(TKey key, TValue value, Object^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( key ) );                   
          gemfire::CacheablePtr valueptr ( Serializable::GetUnmanagedValueGeneric<TValue>( value ));                 
          gemfire::UserDataPtr callbackptr( Serializable::GetUnmanagedValueGeneric<Object^>( callbackArg ) );          
          return NativePtr->localRemove(keyptr, valueptr, callbackptr);

         _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void LocalRegion<TKey, TValue>::InvalidateRegion()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          InvalidateRegion( nullptr );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void LocalRegion<TKey, TValue>::InvalidateRegion(Object^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
                    
          gemfire::UserDataPtr callbackptr(
            Serializable::GetUnmanagedValueGeneric<Object^>( callbackArg ) );
          NativePtr->localInvalidateRegion( callbackptr );
      
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void LocalRegion<TKey, TValue>::DestroyRegion()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          DestroyRegion( nullptr );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void LocalRegion<TKey, TValue>::DestroyRegion(Object^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */          
          gemfire::UserDataPtr callbackptr(
            Serializable::GetUnmanagedValueGeneric<Object^>( callbackArg ) );
          NativePtr->localDestroyRegion( callbackptr );
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void LocalRegion<TKey, TValue>::Invalidate(TKey key)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

         Invalidate(key, nullptr);

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */

      }

      generic<class TKey, class TValue>
      void LocalRegion<TKey, TValue>::Invalidate(TKey key, Object^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( key ) );          
          gemfire::UserDataPtr callbackptr(
            Serializable::GetUnmanagedValueGeneric<Object^>( callbackArg ) );            
          NativePtr->localInvalidate( keyptr, callbackptr );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void LocalRegion<TKey, TValue>::PutAll(System::Collections::Generic::IDictionary<TKey, TValue>^ map)
      {
        throw gcnew System::NotSupportedException;
      }

      generic<class TKey, class TValue>
      void LocalRegion<TKey, TValue>::PutAll(System::Collections::Generic::IDictionary<TKey, TValue>^ map, int timeout)
      {
        throw gcnew System::NotSupportedException;
      }

      generic<class TKey, class TValue>
      void LocalRegion<TKey, TValue>::PutAll(System::Collections::Generic::IDictionary<TKey, TValue>^ map, int timeout, Object^ callbackArg)
      {
        throw gcnew System::NotSupportedException;
      }

      generic<class TKey, class TValue>
      void LocalRegion<TKey, TValue>::GetAll(System::Collections::Generic::ICollection<TKey>^ keys, 
        System::Collections::Generic::IDictionary<TKey, TValue>^ values, 
        System::Collections::Generic::IDictionary<TKey, System::Exception^>^ exceptions)
      {
        throw gcnew System::NotSupportedException;      
      }

      generic<class TKey, class TValue>
      void LocalRegion<TKey, TValue>::GetAll(System::Collections::Generic::ICollection<TKey>^ keys, 
        System::Collections::Generic::IDictionary<TKey, TValue>^ values, 
        System::Collections::Generic::IDictionary<TKey, System::Exception^>^ exceptions, 
        bool addToLocalCache)
      {    
        throw gcnew System::NotSupportedException;
      }

      generic<class TKey, class TValue>
      void LocalRegion<TKey, TValue>::GetAll(System::Collections::Generic::ICollection<TKey>^ keys, 
        System::Collections::Generic::IDictionary<TKey, TValue>^ values, 
        System::Collections::Generic::IDictionary<TKey, System::Exception^>^ exceptions, 
        bool addToLocalCache, Object^ callbackArg)
      {    
        throw gcnew System::NotSupportedException;
      }
      
      generic<class TKey, class TValue>
      void LocalRegion<TKey, TValue>::RemoveAll(System::Collections::Generic::ICollection<TKey>^ keys)
      {
        throw gcnew System::NotSupportedException;
      }
      
      generic<class TKey, class TValue>
      void LocalRegion<TKey, TValue>::RemoveAll(System::Collections::Generic::ICollection<TKey>^ keys,
            Object^ callbackArg)
      {
        throw gcnew System::NotSupportedException;
      }

      generic<class TKey, class TValue>
      String^ LocalRegion<TKey, TValue>::Name::get()
      { 
        return ManagedString::Get( NativePtr->getName( ) ); 
      } 

      generic<class TKey, class TValue>
      String^ LocalRegion<TKey, TValue>::FullPath::get()
      { 
        return ManagedString::Get( NativePtr->getFullPath( ) ); 
      } 

      generic<class TKey, class TValue>
      IRegion<TKey, TValue>^ LocalRegion<TKey, TValue>::ParentRegion::get()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::RegionPtr& nativeptr( NativePtr->getParentRegion( ) );

         IRegion<TKey, TValue>^ region = Region<TKey, TValue>::Create( nativeptr.ptr( ) );
         if (region == nullptr) {
           return nullptr;
         }
         return region->GetLocalView();

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      GemStone::GemFire::Cache::Generic::RegionAttributes<TKey, TValue>^ LocalRegion<TKey, TValue>::Attributes::get()
      { 
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

        gemfire::RegionAttributesPtr& nativeptr( NativePtr->getAttributes( ) );

        return GemStone::GemFire::Cache::Generic::RegionAttributes<TKey, TValue>::Create(nativeptr.ptr());

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      } 

      generic<class TKey, class TValue>      
      AttributesMutator<TKey, TValue>^ LocalRegion<TKey, TValue>::AttributesMutator::get()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::AttributesMutatorPtr& nativeptr(
            NativePtr->getAttributesMutator( ) );

        return GemStone::GemFire::Cache::Generic::AttributesMutator<TKey, TValue>::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
			GemStone::GemFire::Cache::Generic::CacheStatistics^ LocalRegion<TKey, TValue>::Statistics::get()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

        gemfire::CacheStatisticsPtr& nativeptr( NativePtr->getStatistics( ) );
        return GemStone::GemFire::Cache::Generic::CacheStatistics::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      IRegion<TKey, TValue>^ LocalRegion<TKey, TValue>::GetSubRegion( String^ path )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          ManagedString mg_path( path );
          gemfire::RegionPtr& nativeptr(
            NativePtr->getSubregion( mg_path.CharPtr ) );
          IRegion<TKey, TValue>^ region = Region<TKey, TValue>::Create( nativeptr.ptr( ) );
          if (region == nullptr) {
            return nullptr;
          }
          return region->GetLocalView();          

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      IRegion<TKey, TValue>^ LocalRegion<TKey, TValue>::CreateSubRegion( String^ subRegionName, 
        GemStone::GemFire::Cache::Generic::RegionAttributes<TKey, TValue>^ attributes)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          ManagedString mg_subregionName( subRegionName );
				//TODO::split
        /*  gemfire::RegionAttributesPtr p_attrs(
            GetNativePtrFromSBWrap<gemfire::RegionAttributes>( attributes ) );*/

          gemfire::RegionPtr& nativeptr( NativePtr->createSubregion(
            mg_subregionName.CharPtr, /*p_attrs*/NULLPTR ) );
          return Region<TKey, TValue>::Create( nativeptr.ptr( ) )->GetLocalView();

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */

      }

      generic<class TKey, class TValue>
      System::Collections::Generic::ICollection<IRegion<TKey, TValue>^>^ LocalRegion<TKey, TValue>::SubRegions( bool recursive )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::VectorOfRegion vsr;
          NativePtr->subregions( recursive, vsr );
          array<IRegion<TKey, TValue>^>^ subRegions =
            gcnew array<IRegion<TKey, TValue>^>( vsr.size( ) );

          for( int32_t index = 0; index < vsr.size( ); index++ )
          {
            gemfire::RegionPtr& nativeptr( vsr[ index ] );
            subRegions[ index ] = Region<TKey, TValue>::Create( nativeptr.ptr( ) )->GetLocalView();
          }
          System::Collections::Generic::ICollection<IRegion<TKey, TValue>^>^ collection =
            (System::Collections::Generic::ICollection<IRegion<TKey, TValue>^>^)subRegions;
          return collection;

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      RegionEntry<TKey, TValue>^ LocalRegion<TKey, TValue>::GetEntry( TKey key )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( key ) );
          gemfire::RegionEntryPtr& nativeptr( NativePtr->getEntry( keyptr ) );
          return RegionEntry<TKey, TValue>::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      System::Collections::Generic::ICollection<RegionEntry<TKey, TValue>^>^ LocalRegion<TKey, TValue>::GetEntries(bool recursive)
      {
         _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::VectorOfRegionEntry vc;
          NativePtr->entries( vc, recursive );          
          array<RegionEntry<TKey, TValue>^>^ entryarr = gcnew array<RegionEntry<TKey, TValue>^>( vc.size( ) );

          for( int32_t index = 0; index < vc.size( ); index++ )
          {
            gemfire::RegionEntryPtr& nativeptr( vc[ index ] );
            entryarr[ index ] = RegionEntry<TKey, TValue>::Create( nativeptr.ptr( ) );
          }
          System::Collections::Generic::ICollection<RegionEntry<TKey, TValue>^>^ collection =
            (System::Collections::Generic::ICollection<RegionEntry<TKey, TValue>^>^)entryarr;

          return collection;          

         _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
        
      }

      generic<class TKey, class TValue>
      IRegionService^ LocalRegion<TKey, TValue>::RegionService::get()
      {        
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::RegionServicePtr& nativeptr( NativePtr->getRegionService( ) );

          gemfire::Cache* realCache = dynamic_cast<gemfire::Cache*>(nativeptr.ptr());

          if(realCache != NULL)
          {
						return GemStone::GemFire::Cache::Generic::Cache::Create( ((gemfire::CachePtr)nativeptr).ptr( ) );
          }
          else
          {
            return GemStone::GemFire::Cache::Generic::AuthenticatedCache::Create( nativeptr.ptr( ) );
          }
          
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      bool LocalRegion<TKey, TValue>::ContainsValueForKey( TKey key )
      {
         _GF_MG_EXCEPTION_TRY2/* due to auto replace */

           gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( key ) );
           return NativePtr->containsValueForKey( keyptr );

         _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      int LocalRegion<TKey, TValue>::Count::get()
      {
        return NativePtr->size();
      }

      generic<class TKey, class TValue>
      void LocalRegion<TKey, TValue>::Clear()
      {
        Clear(nullptr);
      }

      generic<class TKey, class TValue>
      void LocalRegion<TKey, TValue>::Clear(Object^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          gemfire::UserDataPtr callbackptr(
              Serializable::GetUnmanagedValueGeneric<Object^>( callbackArg ) );          
          NativePtr->localClear(callbackptr );
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }


      generic<class TKey, class TValue>
      void LocalRegion<TKey, TValue>::CopyTo(array<KeyValuePair<TKey,TValue>>^ toArray,
        int startIdx)
      {
        if (toArray == nullptr)
        {
          throw gcnew System::ArgumentNullException;            
        }
        if (startIdx < 0)
        {
          throw gcnew System::ArgumentOutOfRangeException;
        }

        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

        gemfire::VectorOfRegionEntry vc;
        NativePtr->entries( vc, false );        

        if (toArray->Rank > 1 || (vc.size() > (toArray->Length - startIdx)))
        {
          throw gcnew System::ArgumentException;
        }          

        for( int32_t index = 0; index < vc.size( ); index++ )
        {
          gemfire::RegionEntryPtr nativeptr =  vc[ index ];                       
          TKey key = Serializable::GetManagedValueGeneric<TKey> (nativeptr->getKey());
          TValue val = Serializable::GetManagedValueGeneric<TValue> (nativeptr->getValue());            
          toArray[ startIdx ] = KeyValuePair<TKey,TValue>(key, val);
          ++startIdx;
        }               

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      bool LocalRegion<TKey, TValue>::IsDestroyed::get()
      {
        return NativePtr->isDestroyed();
      }
      
      generic<class TKey, class TValue>
      generic<class TResult>
      ISelectResults<TResult>^ LocalRegion<TKey, TValue>::Query( String^ predicate )
      {
        throw gcnew System::NotSupportedException;
      }

      generic<class TKey, class TValue>
      generic<class TResult>
      ISelectResults<TResult>^ LocalRegion<TKey, TValue>::Query( String^ predicate, uint32_t timeout )
      {
        throw gcnew System::NotSupportedException;
      }

      generic<class TKey, class TValue>
      bool LocalRegion<TKey, TValue>::ExistsValue( String^ predicate )
      {
        throw gcnew System::NotSupportedException;
      }

      generic<class TKey, class TValue>
      bool LocalRegion<TKey, TValue>::ExistsValue( String^ predicate, uint32_t timeout )
      {
        throw gcnew System::NotSupportedException;
      }

      generic<class TKey, class TValue>
      Object^ LocalRegion<TKey, TValue>::SelectValue( String^ predicate )
      {
        throw gcnew System::NotSupportedException;
      }

      generic<class TKey, class TValue>
      Object^ LocalRegion<TKey, TValue>::SelectValue( String^ predicate, uint32_t timeout )
      {
        throw gcnew System::NotSupportedException;
      }

      generic<class TKey, class TValue>
      ISubscriptionService<TKey>^ LocalRegion<TKey, TValue>::GetSubscriptionService()
      {
        throw gcnew System::NotSupportedException;
      }

      generic<class TKey, class TValue>
      IRegion<TKey, TValue>^ LocalRegion<TKey, TValue>::GetLocalView()
      {
        throw gcnew System::NotSupportedException;   
      }
    }
  }
} } //namespace 
