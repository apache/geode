/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "gf_defs.hpp"
#include <gfcpp/Cache.hpp>
#include "impl/NativeWrapper.hpp"
#include "IRegion.hpp"
//#include "Log.hpp"
//#include "ExceptionTypes.hpp"
#include "ISubscriptionService.hpp"

using namespace System;
//using namespace System::Collections;
//using namespace System::Collections::Generic;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      //generic<class TKey, class TValue>
     // ref class AttributesMutator;

      generic<class TKey, class TValue>
			public ref class Region : public Internal::SBWrap<apache::geode::client::Region>,
        public IRegion<TKey, TValue>,
        public ISubscriptionService<TKey>
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
            Apache::Geode::Client::AttributesMutator<TKey, TValue>^ get();
          }

          virtual property Apache::Geode::Client::CacheStatistics^ Statistics 
          {
            Apache::Geode::Client::CacheStatistics^ get();
          }

          virtual IRegion<TKey, TValue>^ GetSubRegion( String^ path );
          
          virtual IRegion<TKey, TValue>^ CreateSubRegion( String^ subRegionName,
            RegionAttributes<TKey, TValue>^ attributes );

          virtual System::Collections::Generic::ICollection<IRegion<TKey, TValue>^>^ SubRegions( bool recursive );

          virtual Client::RegionEntry<TKey, TValue>^ GetEntry( TKey key );

          virtual System::Collections::Generic::ICollection<Client::RegionEntry<TKey, TValue>^>^ GetEntries(bool recursive);

          virtual property Client::IRegionService^ RegionService
          {
            Client::IRegionService^ get( );
          }

          virtual bool ContainsValueForKey( TKey key );

          //Additional Region properties and methods
          virtual property bool IsDestroyed
          {
            bool get();
          }
          virtual Apache::Geode::Client::ISubscriptionService<TKey>^ GetSubscriptionService();

          virtual IRegion<TKey, TValue>^ GetLocalView();

          virtual void RegisterKeys(System::Collections::Generic::ICollection<TKey>^ keys );

          virtual void RegisterKeys(System::Collections::Generic::ICollection<TKey>^ keys, bool isDurable, bool getInitialValues);

          virtual void RegisterKeys(System::Collections::Generic::ICollection<TKey>^ keys,
            bool isDurable,
            bool getInitialValues,
            bool receiveValues);

          virtual void UnregisterKeys(System::Collections::Generic::ICollection<TKey>^ keys);

          virtual void RegisterAllKeys( );

          virtual void RegisterAllKeys( bool isDurable );

          virtual void RegisterAllKeys(bool isDurable,
            System::Collections::Generic::ICollection<TKey>^ resultKeys,
            bool getInitialValues);

          virtual void RegisterAllKeys(bool isDurable,
            System::Collections::Generic::ICollection<TKey>^ resultKeys,
            bool getInitialValues,
            bool receiveValues);

          virtual System::Collections::Generic::ICollection<TKey>^ GetInterestList();

          virtual System::Collections::Generic::ICollection<String^>^ GetInterestListRegex();

          virtual void UnregisterAllKeys( );

          virtual void RegisterRegex(String^ regex );

          virtual void RegisterRegex( String^ regex, bool isDurable );

          virtual void RegisterRegex(String^ regex, bool isDurable,
            System::Collections::Generic::ICollection<TKey>^ resultKeys);

          virtual void RegisterRegex(String^ regex, bool isDurable,
            System::Collections::Generic::ICollection<TKey>^ resultKeys, bool getInitialValues);

          virtual void RegisterRegex(String^ regex, bool isDurable,
            System::Collections::Generic::ICollection<TKey>^ resultKeys, bool getInitialValues, bool receiveValues);

          virtual void UnregisterRegex( String^ regex );

          generic<class TResult>
          virtual ISelectResults<TResult>^ Query( String^ predicate );

          generic<class TResult>
          virtual ISelectResults<TResult>^ Query( String^ predicate, uint32_t timeout );

          virtual bool ExistsValue( String^ predicate );

          virtual bool ExistsValue( String^ predicate, uint32_t timeout );

          virtual Object^ SelectValue( String^ predicate );

          virtual Object^ SelectValue( String^ predicate, uint32_t timeout );


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
        inline static Apache::Geode::Client::IRegion<TKey, TValue>^
        Create( apache::geode::client::Region* nativeptr )
        {
          return ( nativeptr != nullptr ?
            gcnew Region<TKey, TValue>( nativeptr ) : nullptr );
        }


        private:
        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline Region( apache::geode::client::Region* nativeptr )
					: SBWrap<apache::geode::client::Region>( nativeptr ) { }

        inline apache::geode::client::SerializablePtr get(apache::geode::client::CacheableKeyPtr& key, apache::geode::client::SerializablePtr& callbackArg);
        inline apache::geode::client::SerializablePtr get(apache::geode::client::CacheableKeyPtr& key);
        bool AreValuesEqual(apache::geode::client::CacheablePtr& val1, apache::geode::client::CacheablePtr& val2);
        bool isPoolInMultiuserMode();
      };
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache
 //namespace 
