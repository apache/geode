/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include <gfcpp/Cache.hpp>
//#include "impl/NativeWrapper.hpp"
#include "IRegion.hpp"
#include "Log.hpp"
#include "ExceptionTypes.hpp"
#include "RegionAttributes.hpp"

using namespace System;
using namespace System::Collections::Generic;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      generic<class TKey, class TValue>
      ref class RegionEntry;

      generic<class TKey, class TValue>
      ref class AttributesMutator;

      generic<class TKey, class TValue>
			public ref class LocalRegion : public Generic::Internal::SBWrap<gemfire::Region>, public IRegion<TKey, TValue>  
      {
      public:

          virtual property TValue default[TKey]
          {
            TValue get(TKey key);
            void set(TKey key, TValue value);
          }         
                    
          virtual System::Collections::Generic::IEnumerator<KeyValuePair<TKey,TValue>>^ GetEnumerator();
          
          virtual System::Collections::IEnumerator^ GetEnumeratorOld() = 
            System::Collections::IEnumerable::GetEnumerator;

          virtual bool ContainsKey(TKey key);
          
          virtual void Add(TKey key, TValue val);
          
          virtual void Add(KeyValuePair<TKey, TValue> keyValuePair);
          
          virtual void Add(TKey key, TValue value, Object^ callbackArg);

          virtual bool Remove(TKey key);        

          virtual bool Remove( TKey key, Object^ callbackArg );      

          virtual bool Remove(KeyValuePair<TKey,TValue> keyValuePair);          

          virtual bool Remove(TKey key, TValue value, Object^ callbackArg );          

          virtual bool Contains(KeyValuePair<TKey,TValue> keyValuePair);          

          virtual void Clear();  

          virtual void Clear(Object^ callbackArg);

          virtual void CopyTo(array<KeyValuePair<TKey,TValue>>^ toArray, int startIdx);                   

          virtual bool TryGetValue(TKey key, TValue %val);
          
          virtual property int Count
          {
            int get();
          }

          virtual property bool IsReadOnly
          {
            bool get() {throw gcnew System::NotImplementedException;/*return false;*/}
          }
          
          virtual property System::Collections::Generic::ICollection<TKey>^ Keys
          {
            System::Collections::Generic::ICollection<TKey>^ get();
          }

          virtual property System::Collections::Generic::ICollection<TValue>^ Values
          {
            System::Collections::Generic::ICollection<TValue>^ get();
          }

          virtual void Put(TKey key, TValue value, Object^ callbackArg);

          virtual TValue Get(TKey key, Object^ callbackArg);

          virtual void InvalidateRegion();

          virtual void InvalidateRegion(Object^ callbackArg);

          virtual void DestroyRegion();

          virtual void DestroyRegion(Object^ callbackArg);

          virtual void Invalidate(TKey key);

          virtual void Invalidate(TKey key, Object^ callbackArg);

          virtual void PutAll(System::Collections::Generic::IDictionary<TKey, TValue>^ map);

          virtual void PutAll(System::Collections::Generic::IDictionary<TKey, TValue>^ map, int timeout);

          virtual void PutAll(System::Collections::Generic::IDictionary<TKey, TValue>^ map, int timeout, Object^ callbackArg);

          virtual void GetAll(System::Collections::Generic::ICollection<TKey>^ keys, 
            System::Collections::Generic::IDictionary<TKey, TValue>^ values, 
            System::Collections::Generic::IDictionary<TKey, System::Exception^>^ exceptions);

          virtual void GetAll(System::Collections::Generic::ICollection<TKey>^ keys, 
            System::Collections::Generic::IDictionary<TKey, TValue>^ values, 
            System::Collections::Generic::IDictionary<TKey, System::Exception^>^ exceptions,
            bool addToLocalCache);

          virtual void GetAll(System::Collections::Generic::ICollection<TKey>^ keys, 
            System::Collections::Generic::IDictionary<TKey, TValue>^ values, 
            System::Collections::Generic::IDictionary<TKey, System::Exception^>^ exceptions,
            bool addToLocalCache, Object^ callbackArg);
          
          virtual void RemoveAll(System::Collections::Generic::ICollection<TKey>^ keys);
          virtual void RemoveAll(System::Collections::Generic::ICollection<TKey>^ keys,
            Object^ callbackArg);

          virtual property String^ Name
          { 
            String^ get();
          } 

          virtual property String^ FullPath
          {
            String^ get();
          }

          virtual property IRegion<TKey, TValue>^ ParentRegion
          {
            IRegion<TKey, TValue>^ get( );
          }

          virtual property RegionAttributes<TKey, TValue>^ Attributes 
          {
            RegionAttributes<TKey, TValue>^ get();
          }

          virtual property AttributesMutator<TKey, TValue>^ AttributesMutator
          {
            GemStone::GemFire::Cache::Generic::AttributesMutator<TKey, TValue>^ get();
          }

          virtual property GemStone::GemFire::Cache::Generic::CacheStatistics^ Statistics 
          {
            GemStone::GemFire::Cache::Generic::CacheStatistics^ get();
          }

          virtual IRegion<TKey, TValue>^ GetSubRegion( String^ path );
          
          virtual IRegion<TKey, TValue>^ CreateSubRegion( String^ subRegionName,
            RegionAttributes<TKey,TValue>^ attributes );

          virtual System::Collections::Generic::ICollection<IRegion<TKey, TValue>^>^ SubRegions( bool recursive );

          virtual RegionEntry<TKey, TValue>^ GetEntry( TKey key );

          virtual System::Collections::Generic::ICollection<RegionEntry<TKey,TValue>^>^ GetEntries(bool recursive);

          virtual property GemStone::GemFire::Cache::Generic::IRegionService^ RegionService
          {
            GemStone::GemFire::Cache::Generic::IRegionService^ get( );
          }

          virtual bool ContainsValueForKey( TKey key );

          //Additional Region properties and methods
          virtual property bool IsDestroyed
          {
            bool get();
          }
          
          generic<class TResult>
          virtual ISelectResults<TResult>^ Query( String^ predicate );

          generic<class TResult>
          virtual ISelectResults<TResult>^ Query( String^ predicate, uint32_t timeout );

          virtual bool ExistsValue( String^ predicate );

          virtual bool ExistsValue( String^ predicate, uint32_t timeout );

          virtual Object^ SelectValue( String^ predicate );

          virtual Object^ SelectValue( String^ predicate, uint32_t timeout );

          virtual ISubscriptionService<TKey>^ GetSubscriptionService();

          virtual IRegion<TKey, TValue>^ GetLocalView();


      internal:
        /// <summary>
        /// Internal factory function to wrap a native object pointer inside
        /// this managed class with null pointer check.
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        /// <returns>
        /// The managed wrapper object; null if the native pointer is null.
        /// </returns>
        //generic<class TKey, class TValue>
        inline static IRegion<TKey, TValue>^ Create( gemfire::Region* nativeptr )
        {
          return ( nativeptr != nullptr ?
            gcnew LocalRegion<TKey, TValue>( nativeptr ) : nullptr );
        }

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline LocalRegion( gemfire::Region* nativeptr )
          : SBWrap( nativeptr ) { }

        private:        
        inline gemfire::SerializablePtr getRegionEntryValue(gemfire::CacheableKeyPtr& key);
        bool AreValuesEqual(gemfire::CacheablePtr& val1, gemfire::CacheablePtr& val2);
      };


    }
  }
} } //namespace 
