/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "gf_includes.hpp"
#include "Region.hpp"
#include "Cache.hpp"
#include "CacheStatistics.hpp"
#include "RegionAttributes.hpp"
#include "AttributesMutator.hpp"
#include "RegionEntry.hpp"
#include "ISelectResults.hpp"
#include "IGFSerializable.hpp"
#include "ResultSet.hpp"
#include "StructSet.hpp"
#include "impl/AuthenticatedCache.hpp"
#include "impl/SafeConvert.hpp"
//#include <gfcpp/Serializable.hpp>
//#include <cppcache/DataOutPut.hpp>
#include "LocalRegion.hpp"
#include "Pool.hpp"
#include "PoolManager.hpp"
#include "SystemProperties.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      generic<class TKey, class TValue>
      TValue Region<TKey, TValue>::Get(TKey key, Object^ callbackArg)
      {
        gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( key ) );        
        gemfire::UserDataPtr callbackptr(
          Serializable::GetUnmanagedValueGeneric<Object^>( callbackArg ) );
        gemfire::CacheablePtr nativeptr(this->get(keyptr, callbackptr));
        if (nativeptr == NULLPTR)
        {
          throw gcnew KeyNotFoundException("The given key was not present in the region.");
        }
        TValue returnVal = Serializable::GetManagedValueGeneric<TValue>( nativeptr );
        return returnVal;

      }
      
      generic<class TKey, class TValue>
      gemfire::SerializablePtr Region<TKey, TValue>::get(gemfire::CacheableKeyPtr& keyptr, gemfire::SerializablePtr& callbackptr)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          return NativePtr->get( keyptr, callbackptr ) ;
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      gemfire::SerializablePtr Region<TKey, TValue>::get(gemfire::CacheableKeyPtr& keyptr)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          return NativePtr->get( keyptr ) ;
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      bool Region<TKey, TValue>::isPoolInMultiuserMode()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          GemStone::GemFire::Cache::Generic::RegionAttributes<TKey, TValue>^ rAttributes = this->Attributes;
          String^ poolName = rAttributes->PoolName;
          if (poolName != nullptr) {
            Pool/*<TKey, TValue>*/^ pool = PoolManager/*<TKey, TValue>*/::Find(poolName);
            if (pool != nullptr && !pool->Destroyed) {
              return pool->MultiuserAuthentication;
            }
          }
          return false;
          _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::Put(TKey key, TValue value, Object^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

        gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( key ) );
        gemfire::CacheablePtr valueptr( Serializable::GetUnmanagedValueGeneric<TValue>( value ) );                
          gemfire::UserDataPtr callbackptr(
            Serializable::GetUnmanagedValueGeneric<Object^>( callbackArg ) );
          NativePtr->put( keyptr, valueptr, callbackptr );       

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      TValue Region<TKey, TValue>::default::get(TKey key)
      {  
        gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( key ) );
        gemfire::CacheablePtr nativeptr(this->get(keyptr));
        if (nativeptr == NULLPTR)
        {
          throw gcnew KeyNotFoundException("The given key was not present in the region.");
        }
        TValue returnVal = Serializable::GetManagedValueGeneric<TValue>( nativeptr );
        return returnVal;
      }

      generic<class TKey, class TValue>      
      void Region<TKey, TValue>::default::set(TKey key, TValue value)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

        gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( key ) );
        gemfire::CacheablePtr valueptr( Serializable::GetUnmanagedValueGeneric<TValue>( value ) );
        NativePtr->put( keyptr, valueptr );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      System::Collections::Generic::IEnumerator<KeyValuePair<TKey,TValue>>^ 
        Region<TKey, TValue>::GetEnumerator()
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
        Region<TKey, TValue>::GetEnumeratorOld()
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
      bool Region<TKey, TValue>::AreValuesEqual(gemfire::CacheablePtr& val1, gemfire::CacheablePtr& val2)
      {
        if ( val1 == NULLPTR && val2 == NULLPTR )
        {          
          return true;
        }
        else if ((val1 == NULLPTR && val2 != NULLPTR) || (val1 != NULLPTR && val2 == NULLPTR))
        {          
          return false;
        }
        else if ( val1 != NULLPTR && val2 != NULLPTR )
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
      bool Region<TKey, TValue>::Contains(KeyValuePair<TKey,TValue> keyValuePair) 
      { 
        gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( keyValuePair.Key ) ); 
        gemfire::CacheablePtr nativeptr(this->get(keyptr)); 
        //This means that key is not present.
        if (nativeptr == NULLPTR) {
          return false;
        }
        TValue value = Serializable::GetManagedValueGeneric<TValue>(nativeptr);
        return ((Object^)value)->Equals(keyValuePair.Value);
      } 

      generic<class TKey, class TValue>
      bool Region<TKey, TValue>::ContainsKey(TKey key)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( key ) );          

          return NativePtr->containsKeyOnServer( keyptr );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      bool Region<TKey, TValue>::TryGetValue(TKey key, TValue %val)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
        gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( key ) );
        gemfire::CacheablePtr nativeptr(this->get(keyptr));
        if (nativeptr == NULLPTR) {            
          val = TValue();
          return false;
        }
        else {
          val = Serializable::GetManagedValueGeneric<TValue>( nativeptr );
          return true;
        }                           

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }      

      generic<class TKey, class TValue>
      System::Collections::Generic::ICollection<TKey>^ Region<TKey, TValue>::Keys::get()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

        gemfire::VectorOfCacheableKey vc;
        NativePtr->serverKeys( vc );
        //List<TKey>^ collectionlist = gcnew List<TKey>(vc.size());
        array<TKey>^ keyarr =
          gcnew array<TKey>( vc.size( ) );
        for( int32_t index = 0; index < vc.size( ); index++ )
        {            
          gemfire::CacheableKeyPtr& nativeptr( vc[ index ] );
          keyarr[ index ] = Serializable::GetManagedValueGeneric<TKey>(nativeptr);
          //collectionlist[ index ] = Serializable::GetManagedValue<TKey>(nativeptr);
        }
        System::Collections::Generic::ICollection<TKey>^ collectionlist =
          (System::Collections::Generic::ICollection<TKey>^)keyarr;
        return collectionlist;

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      System::Collections::Generic::ICollection<TValue>^ Region<TKey, TValue>::Values::get()
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
          System::Collections::Generic::ICollection<TValue>^ collectionlist =
            (System::Collections::Generic::ICollection<TValue>^)valarr;
          return collectionlist;

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::Add(TKey key, TValue value)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( key ) );
          gemfire::CacheablePtr valueptr( Serializable::GetUnmanagedValueGeneric<TValue>( value ) );
          NativePtr->create( keyptr, valueptr );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::Add(KeyValuePair<TKey, TValue> keyValuePair)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( keyValuePair.Key ) );
          gemfire::CacheablePtr valueptr( Serializable::GetUnmanagedValueGeneric<TValue>( keyValuePair.Value ) );
          NativePtr->create( keyptr, valueptr );

       _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }
      
      generic<class TKey, class TValue>
      void Region<TKey, TValue>::Add(TKey key, TValue value, Object^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( key ) );
          gemfire::CacheablePtr valueptr( Serializable::GetUnmanagedValueGeneric<TValue>( value ) );          
          gemfire::UserDataPtr callbackptr(
            Serializable::GetUnmanagedValueGeneric<Object^>( callbackArg ) );
          NativePtr->create( keyptr, valueptr, callbackptr );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      bool Region<TKey, TValue>::Remove(TKey key)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( key ) );          
          return NativePtr->removeEx( keyptr );
          
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */

      }
      
      generic<class TKey, class TValue>
      bool Region<TKey, TValue>::Remove( TKey key, Object^ callbackArg )
      {
         _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( key ) );                    
          gemfire::UserDataPtr callbackptr(
            Serializable::GetUnmanagedValueGeneric<Object^>( callbackArg ) );
          return NativePtr->removeEx( keyptr, callbackptr );
          
          _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      bool Region<TKey, TValue>::Remove(KeyValuePair<TKey,TValue> keyValuePair)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( keyValuePair.Key ) );                  
          gemfire::CacheablePtr valueptr( Serializable::GetUnmanagedValueGeneric<TValue>( keyValuePair.Value ) );

          return NativePtr->remove(keyptr, valueptr);

         _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      bool Region<TKey, TValue>::Remove(TKey key, TValue value, Object^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( key ) );                   
          gemfire::CacheablePtr valueptr ( Serializable::GetUnmanagedValueGeneric<TValue>( value ));                    
          gemfire::UserDataPtr callbackptr( Serializable::GetUnmanagedValueGeneric<Object^>( callbackArg ) );          
          return NativePtr->remove(keyptr, valueptr, callbackptr);

         _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::InvalidateRegion()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          InvalidateRegion( nullptr );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::InvalidateRegion(Object^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
                    
          gemfire::UserDataPtr callbackptr(
            Serializable::GetUnmanagedValueGeneric<Object^>( callbackArg ) );
          NativePtr->invalidateRegion( callbackptr );
                
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::DestroyRegion()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          DestroyRegion( nullptr );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::DestroyRegion(Object^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::UserDataPtr callbackptr(
            Serializable::GetUnmanagedValueGeneric<Object^>( callbackArg ) );
          NativePtr->destroyRegion( callbackptr );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::Invalidate(TKey key)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

         Invalidate(key, nullptr);

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */

      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::Invalidate(TKey key, Object^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( key ) );                    
          gemfire::UserDataPtr callbackptr(
            Serializable::GetUnmanagedValueGeneric<Object^>( callbackArg ) );            
          NativePtr->invalidate( keyptr, callbackptr );
          
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::PutAll(System::Collections::Generic::IDictionary<TKey, TValue>^ map)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          return PutAll( map, DEFAULT_RESPONSE_TIMEOUT );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */

      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::PutAll(System::Collections::Generic::IDictionary<TKey, TValue>^ map, int timeout)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::HashMapOfCacheable nativeMap;
          for each ( KeyValuePair<TKey, TValue> keyValPair in map )
          {
            gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( keyValPair.Key ) );
            gemfire::CacheablePtr valueptr( Serializable::GetUnmanagedValueGeneric<TValue>( keyValPair.Value ) );
            nativeMap.insert( keyptr, valueptr );
          }
          NativePtr->putAll( nativeMap, timeout );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */

      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::PutAll(System::Collections::Generic::IDictionary<TKey, TValue>^ map, int timeout, Object^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::HashMapOfCacheable nativeMap;
          for each ( KeyValuePair<TKey, TValue> keyValPair in map )
          {
            gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( keyValPair.Key ) );
            gemfire::CacheablePtr valueptr( Serializable::GetUnmanagedValueGeneric<TValue>( keyValPair.Value ) );
            nativeMap.insert( keyptr, valueptr );
          }
          gemfire::UserDataPtr callbackptr(
            Serializable::GetUnmanagedValueGeneric<Object^>( callbackArg ) );
          NativePtr->putAll( nativeMap, timeout, callbackptr );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */

      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::GetAll(System::Collections::Generic::ICollection<TKey>^ keys, 
        System::Collections::Generic::IDictionary<TKey, TValue>^ values, 
        System::Collections::Generic::IDictionary<TKey, System::Exception^>^ exceptions)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          GetAll(keys, values, exceptions, false);

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */

      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::GetAll(System::Collections::Generic::ICollection<TKey>^ keys, 
        System::Collections::Generic::IDictionary<TKey, TValue>^ values, 
        System::Collections::Generic::IDictionary<TKey, System::Exception^>^ exceptions, 
        bool addToLocalCache)
      {
        if (keys != nullptr) {
          _GF_MG_EXCEPTION_TRY2/* due to auto replace */

            gemfire::VectorOfCacheableKey vecKeys;

            for each(TKey item in keys)
            {
              vecKeys.push_back(
                Serializable::GetUnmanagedValueGeneric<TKey>( item ));
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
                  TKey key = Serializable::GetManagedValueGeneric<TKey>(iter.first());                  
                  TValue val = Serializable::GetManagedValueGeneric<TValue>( iter.second());                  
                  values->Add(key, val);
              }
            }
            if (exceptions != nullptr) {
              for (gemfire::HashMapOfException::Iterator iter =
                exceptionsPtr->begin(); iter != exceptionsPtr->end(); ++iter) {
                  TKey key = Serializable::GetManagedValueGeneric<TKey>(iter.first());                  
                  System::Exception^ ex = GemFireException::Get(*iter.second());                  
                  exceptions->Add(key, ex);
              }
            }

          _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
        }
        else {
          throw gcnew IllegalArgumentException("GetAll: null keys provided");
        }         
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::GetAll(System::Collections::Generic::ICollection<TKey>^ keys, 
        System::Collections::Generic::IDictionary<TKey, TValue>^ values, 
        System::Collections::Generic::IDictionary<TKey, System::Exception^>^ exceptions, 
        bool addToLocalCache, Object^ callbackArg)
      {
        if (keys != nullptr) {
          _GF_MG_EXCEPTION_TRY2/* due to auto replace */

            gemfire::VectorOfCacheableKey vecKeys;

            for each(TKey item in keys)
            {
              vecKeys.push_back(
                Serializable::GetUnmanagedValueGeneric<TKey>( item ));
            }
                        
            gemfire::HashMapOfCacheablePtr valuesPtr(NULLPTR);
            if (values != nullptr) {
              valuesPtr = new gemfire::HashMapOfCacheable();
            }
            gemfire::HashMapOfExceptionPtr exceptionsPtr(NULLPTR);
            if (exceptions != nullptr) {
              exceptionsPtr = new gemfire::HashMapOfException();
            }

            gemfire::UserDataPtr callbackptr(
            Serializable::GetUnmanagedValueGeneric<Object^>( callbackArg ) );

            NativePtr->getAll(vecKeys, valuesPtr, exceptionsPtr,
              addToLocalCache, callbackptr);
            if (values != nullptr) {
              for (gemfire::HashMapOfCacheable::Iterator iter =
                valuesPtr->begin(); iter != valuesPtr->end(); ++iter) {                             
                  TKey key = Serializable::GetManagedValueGeneric<TKey>(iter.first());
                  TValue val = Serializable::GetManagedValueGeneric<TValue>( iter.second());
                  values->Add(key, val);
              }
            }
            if (exceptions != nullptr) {
              for (gemfire::HashMapOfException::Iterator iter =
                exceptionsPtr->begin(); iter != exceptionsPtr->end(); ++iter) {
                  TKey key = Serializable::GetManagedValueGeneric<TKey>(iter.first());            
                  System::Exception^ ex = GemFireException::Get(*iter.second());            
                  exceptions->Add(key, ex);
              }
            }

          _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
        }
        else {
          throw gcnew IllegalArgumentException("GetAll: null keys provided");
        }         
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::RemoveAll(System::Collections::Generic::ICollection<TKey>^ keys)
      {
         _GF_MG_EXCEPTION_TRY2/* due to auto replace */

         RemoveAll(keys, nullptr);

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */

      }
      
      generic<class TKey, class TValue>
      void Region<TKey, TValue>::RemoveAll(System::Collections::Generic::ICollection<TKey>^ keys,
            Object^ callbackArg)
      {
         _GF_MG_EXCEPTION_TRY2/* due to auto replace */
           
         gemfire::VectorOfCacheableKey vecKeys;
         for each(TKey item in keys)
           vecKeys.push_back(Serializable::GetUnmanagedValueGeneric<TKey>( item ));
         
         gemfire::UserDataPtr callbackptr(
            Serializable::GetUnmanagedValueGeneric<Object^>( callbackArg ) );   
         
         NativePtr->removeAll(vecKeys, callbackptr);
        
         _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */

      }
      generic<class TKey, class TValue>
      String^ Region<TKey, TValue>::Name::get()
      { 
        return ManagedString::Get( NativePtr->getName( ) ); 
      } 

      generic<class TKey, class TValue>
      String^ Region<TKey, TValue>::FullPath::get()
      { 
        return ManagedString::Get( NativePtr->getFullPath( ) ); 
      } 

      generic<class TKey, class TValue>
      IRegion<TKey, TValue>^ Region<TKey, TValue>::ParentRegion::get()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::RegionPtr& nativeptr( NativePtr->getParentRegion( ) );

          return Region::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      GemStone::GemFire::Cache::Generic::RegionAttributes<TKey, TValue>^ Region<TKey, TValue>::Attributes::get()
      { 
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

        gemfire::RegionAttributesPtr& nativeptr( NativePtr->getAttributes( ) );

        return GemStone::GemFire::Cache::Generic::RegionAttributes<TKey, TValue>::Create(nativeptr.ptr());

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      } 

      generic<class TKey, class TValue>      
      AttributesMutator<TKey, TValue>^ Region<TKey, TValue>::AttributesMutator::get()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::AttributesMutatorPtr& nativeptr(
            NativePtr->getAttributesMutator( ) );

        return GemStone::GemFire::Cache::Generic::AttributesMutator<TKey, TValue>::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      GemStone::GemFire::Cache::Generic::CacheStatistics^ Region<TKey, TValue>::Statistics::get()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

        gemfire::CacheStatisticsPtr& nativeptr( NativePtr->getStatistics( ) );
        return GemStone::GemFire::Cache::Generic::CacheStatistics::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      IRegion<TKey, TValue>^ Region<TKey, TValue>::GetSubRegion( String^ path )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          ManagedString mg_path( path );
          gemfire::RegionPtr& nativeptr(
            NativePtr->getSubregion( mg_path.CharPtr ) );
          return Region::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      IRegion<TKey, TValue>^ Region<TKey, TValue>::CreateSubRegion( String^ subRegionName, 
        GemStone::GemFire::Cache::Generic::RegionAttributes<TKey, TValue>^ attributes)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          ManagedString mg_subregionName( subRegionName );
				//TODO:split
          gemfire::RegionAttributesPtr p_attrs(
            GetNativePtrFromSBWrapGeneric<gemfire::RegionAttributes>( attributes ) );

          gemfire::RegionPtr& nativeptr( NativePtr->createSubregion(
            mg_subregionName.CharPtr, p_attrs /*NULLPTR*/ ) );
          return Region::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */

      }

      generic<class TKey, class TValue>
      System::Collections::Generic::ICollection<IRegion<TKey, TValue>^>^ Region<TKey, TValue>::SubRegions( bool recursive )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::VectorOfRegion vsr;
          NativePtr->subregions( recursive, vsr );
          array<IRegion<TKey, TValue>^>^ subRegions =
            gcnew array<IRegion<TKey, TValue>^>( vsr.size( ) );

          for( int32_t index = 0; index < vsr.size( ); index++ )
          {
            gemfire::RegionPtr& nativeptr( vsr[ index ] );
            subRegions[ index ] = Region<TKey, TValue>::Create( nativeptr.ptr( ) );
          }
          System::Collections::Generic::ICollection<IRegion<TKey, TValue>^>^ collection =
            (System::Collections::Generic::ICollection<IRegion<TKey, TValue>^>^)subRegions;
          return collection;

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      RegionEntry<TKey, TValue>^ Region<TKey, TValue>::GetEntry( TKey key )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( key ) );
          gemfire::RegionEntryPtr& nativeptr( NativePtr->getEntry( keyptr ) );
          return RegionEntry<TKey, TValue>::Create( nativeptr.ptr( ) );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      System::Collections::Generic::ICollection<RegionEntry<TKey, TValue>^>^ Region<TKey, TValue>::GetEntries(bool recursive)
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
      IRegionService^ Region<TKey, TValue>::RegionService::get()
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
      bool Region<TKey, TValue>::ContainsValueForKey( TKey key )
      {
         _GF_MG_EXCEPTION_TRY2/* due to auto replace */

           gemfire::CacheableKeyPtr keyptr( Serializable::GetUnmanagedValueGeneric<TKey>( key ) );
           return NativePtr->containsValueForKey( keyptr );

         _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      int Region<TKey, TValue>::Count::get()
      {
		_GF_MG_EXCEPTION_TRY2/* due to auto replace */
        return NativePtr->size();
		_GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::Clear()
      {
        Clear(nullptr);
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::Clear(Object^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          gemfire::UserDataPtr callbackptr(
              Serializable::GetUnmanagedValueGeneric<Object^>( callbackArg ) );          
          NativePtr->clear(callbackptr );
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::CopyTo(array<KeyValuePair<TKey,TValue>>^ toArray,
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
      bool Region<TKey, TValue>::IsDestroyed::get()
      {
        return NativePtr->isDestroyed();
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::RegisterKeys( System::Collections::Generic::ICollection<TKey>^ keys )
      {
        RegisterKeys(keys, false, false);
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::RegisterKeys(System::Collections::Generic::ICollection<TKey>^ keys, bool isDurable, bool getInitialValues)
      {
        RegisterKeys(keys, isDurable, getInitialValues, true);
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::RegisterKeys(System::Collections::Generic::ICollection<TKey>^ keys,
        bool isDurable,
        bool getInitialValues,
        bool receiveValues)
      {
        if (keys != nullptr)
        {
          _GF_MG_EXCEPTION_TRY2/* due to auto replace */

            gemfire::VectorOfCacheableKey vecKeys;

            for each(TKey item in keys)
            {
              vecKeys.push_back(
                Serializable::GetUnmanagedValueGeneric<TKey>( item ));
            }
            NativePtr->registerKeys(vecKeys, isDurable, getInitialValues, receiveValues);

          _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
        }        
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::UnregisterKeys(System::Collections::Generic::ICollection<TKey>^ keys)
      {
        if (keys != nullptr)
        {
          _GF_MG_EXCEPTION_TRY2/* due to auto replace */

            gemfire::VectorOfCacheableKey vecKeys;

            for each(TKey item in keys)
            {
              vecKeys.push_back(
                Serializable::GetUnmanagedValueGeneric<TKey>( item ));
            }

            NativePtr->unregisterKeys(vecKeys);

          _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
        }
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::RegisterAllKeys( )
      {
        RegisterAllKeys( false, nullptr, false );
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::RegisterAllKeys( bool isDurable )
      {
        RegisterAllKeys( isDurable, nullptr, false );
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::RegisterAllKeys(bool isDurable,
            System::Collections::Generic::ICollection<TKey>^ resultKeys,
            bool getInitialValues)
      {
        RegisterAllKeys(isDurable, resultKeys, getInitialValues, true);
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::RegisterAllKeys(bool isDurable,
            System::Collections::Generic::ICollection<TKey>^ resultKeys,
            bool getInitialValues,
            bool receiveValues)
      {
         _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          if (resultKeys != nullptr) {
            gemfire::VectorOfCacheableKeyPtr mg_keys(
              new gemfire::VectorOfCacheableKey());

            NativePtr->registerAllKeys(isDurable, mg_keys, getInitialValues, receiveValues);            

            for (int32_t index = 0; index < mg_keys->size(); ++index) {              
              resultKeys->Add(Serializable::GetManagedValueGeneric<TKey>(
                mg_keys->operator[](index)));
            }
          }
          else {
            NativePtr->registerAllKeys(isDurable, NULLPTR, getInitialValues, receiveValues);
          }

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      System::Collections::Generic::ICollection<TKey>^ Region<TKey, TValue>::GetInterestList()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::VectorOfCacheableKey vc;
          NativePtr->getInterestList( vc );
          //List<TValue>^ collectionlist = gcnew List<TValue>(vc.size());
          array<TKey>^ keyarr =
            gcnew array<TKey>( vc.size( ) );
          for( int32_t index = 0; index < vc.size( ); index++ )
          {
            gemfire::CacheableKeyPtr& nativeptr( vc[ index ] );
            keyarr[ index ] = Serializable::GetManagedValueGeneric<TKey>(nativeptr);
            //collectionlist[ index ] = Serializable::GetManagedValueGeneric<TValue>(nativeptr);
          }

          System::Collections::Generic::ICollection<TKey>^ collectionlist =
            (System::Collections::Generic::ICollection<TKey>^)keyarr;
          return collectionlist;

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      System::Collections::Generic::ICollection<String^>^ Region<TKey, TValue>::GetInterestListRegex()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::VectorOfCacheableString vc;
          NativePtr->getInterestListRegex( vc );
          array<String^>^ strarr =
            gcnew array<String^>( vc.size( ) );
          //List<String>^ collectionlist = gcnew List<String>(vc.size());
          for( int32_t index = 0; index < vc.size( ); index++ )
          {
            strarr[index] = ManagedString::Get( vc[index]->asChar());
            //collectionlist[ index ] = Serializable::GetManagedValue<TValue>(nativeptr);
          }
          System::Collections::Generic::ICollection<String^>^ collectionlist =
            (System::Collections::Generic::ICollection<String^>^)strarr;
          return collectionlist;

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::UnregisterAllKeys( )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          NativePtr->unregisterAllKeys( );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */

      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::RegisterRegex(String^ regex )
      {
        RegisterRegex(regex, false, nullptr, false);
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::RegisterRegex(String^ regex , bool isDurable)
      {
        RegisterRegex(regex, isDurable, nullptr, false);
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::RegisterRegex(String^ regex, bool isDurable,
            System::Collections::Generic::ICollection<TKey>^ resultKeys)
      {
        RegisterRegex(regex, isDurable, resultKeys, false);
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::RegisterRegex(String^ regex, bool isDurable,
            System::Collections::Generic::ICollection<TKey>^ resultKeys, bool getInitialValues)
      {
        RegisterRegex(regex, isDurable, resultKeys, getInitialValues, true);
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::RegisterRegex(String^ regex, bool isDurable,
            System::Collections::Generic::ICollection<TKey>^ resultKeys, bool getInitialValues, bool receiveValues)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          ManagedString mg_regex( regex );
          if (resultKeys != nullptr) {
            gemfire::VectorOfCacheableKeyPtr mg_keys(
              new gemfire::VectorOfCacheableKey());
            NativePtr->registerRegex(mg_regex.CharPtr, isDurable,
              mg_keys, getInitialValues, receiveValues);

            for (int32_t index = 0; index < mg_keys->size(); ++index) {              
              resultKeys->Add(Serializable::GetManagedValueGeneric<TKey>(
                mg_keys->operator[](index)));
            }
          }
          else {
            NativePtr->registerRegex(mg_regex.CharPtr, isDurable,
              NULLPTR, getInitialValues, receiveValues);
          }

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::UnregisterRegex( String^ regex )
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          ManagedString mg_regex( regex );
          NativePtr->unregisterRegex( mg_regex.CharPtr );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */

      }

      generic<class TKey, class TValue>
      generic<class TResult>
      ISelectResults<TResult>^ Region<TKey, TValue>::Query( String^ predicate )
      {
        //return Query( predicate, DEFAULT_QUERY_RESPONSE_TIMEOUT );
        ManagedString mg_predicate( predicate );

        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::SelectResultsPtr& nativeptr = NativePtr->query(
            mg_predicate.CharPtr, DEFAULT_QUERY_RESPONSE_TIMEOUT );
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
            return StructSet<TResult>::Create(structptr);
          }
          else
          {
            return ResultSet<TResult>::Create(resultptr);
          }
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */

      }

      generic<class TKey, class TValue>
      generic<class TResult>
      ISelectResults<TResult>^ Region<TKey, TValue>::Query( String^ predicate, uint32_t timeout )
      {
        ManagedString mg_predicate( predicate );

        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

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
            return StructSet<TResult>::Create(structptr);
          }
          else
          {
            return ResultSet<TResult>::Create(resultptr);
          }
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      bool Region<TKey, TValue>::ExistsValue( String^ predicate )
      {
        return ExistsValue( predicate, DEFAULT_QUERY_RESPONSE_TIMEOUT );
      }

      generic<class TKey, class TValue>
      bool Region<TKey, TValue>::ExistsValue( String^ predicate, uint32_t timeout )
      {
        ManagedString mg_predicate( predicate );

        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          return NativePtr->existsValue( mg_predicate.CharPtr, timeout );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */

      }

      generic<class TKey, class TValue>
      Object^ Region<TKey, TValue>::SelectValue( String^ predicate )
      {
        return SelectValue( predicate, DEFAULT_QUERY_RESPONSE_TIMEOUT );
      }

      generic<class TKey, class TValue>
      Object^ Region<TKey, TValue>::SelectValue( String^ predicate, uint32_t timeout )
      {
        ManagedString mg_predicate( predicate );

        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          gemfire::CacheablePtr& nativeptr( NativePtr->selectValue(
            mg_predicate.CharPtr, timeout ) );

          return Serializable::GetManagedValueGeneric<Object^>(nativeptr);

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */

      }
      
      generic<class TKey, class TValue>
      ISubscriptionService<TKey>^ Region<TKey, TValue>::GetSubscriptionService()
      {
        return (ISubscriptionService<TKey>^) this;
      }

      generic<class TKey, class TValue>
      IRegion<TKey, TValue>^ Region<TKey, TValue>::GetLocalView()
      {
        return ( _NativePtr != nullptr ?
            gcnew LocalRegion<TKey, TValue>( _NativePtr ) : nullptr );       
      }
    }
  }
} } //namespace 
