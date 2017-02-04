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

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      generic<class TKey, class TValue>
      TValue Region<TKey, TValue>::Get(TKey key, Object^ callbackArg)
      {
        apache::geode::client::CacheableKeyPtr keyptr(Serializable::GetUnmanagedValueGeneric<TKey>(key));
        apache::geode::client::UserDataPtr callbackptr(
          Serializable::GetUnmanagedValueGeneric<Object^>(callbackArg));
        apache::geode::client::CacheablePtr nativeptr(this->get(keyptr, callbackptr));
        if (nativeptr == NULLPTR)
        {
          throw gcnew KeyNotFoundException("The given key was not present in the region.");
        }
        TValue returnVal = Serializable::GetManagedValueGeneric<TValue>(nativeptr);
        return returnVal;

      }

      generic<class TKey, class TValue>
      apache::geode::client::SerializablePtr Region<TKey, TValue>::get(apache::geode::client::CacheableKeyPtr& keyptr, apache::geode::client::SerializablePtr& callbackptr)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          return NativePtr->get(keyptr, callbackptr);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      apache::geode::client::SerializablePtr Region<TKey, TValue>::get(apache::geode::client::CacheableKeyPtr& keyptr)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          return NativePtr->get(keyptr);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      bool Region<TKey, TValue>::isPoolInMultiuserMode()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          Apache::Geode::Client::RegionAttributes<TKey, TValue>^ rAttributes = this->Attributes;
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

          apache::geode::client::CacheableKeyPtr keyptr(Serializable::GetUnmanagedValueGeneric<TKey>(key));
        apache::geode::client::CacheablePtr valueptr(Serializable::GetUnmanagedValueGeneric<TValue>(value));
        apache::geode::client::UserDataPtr callbackptr(
          Serializable::GetUnmanagedValueGeneric<Object^>(callbackArg));
        NativePtr->put(keyptr, valueptr, callbackptr);

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      TValue Region<TKey, TValue>::default::get(TKey key)
      {
        apache::geode::client::CacheableKeyPtr keyptr(Serializable::GetUnmanagedValueGeneric<TKey>(key));
        apache::geode::client::CacheablePtr nativeptr(this->get(keyptr));
        if (nativeptr == NULLPTR)
        {
          throw gcnew KeyNotFoundException("The given key was not present in the region.");
        }
        TValue returnVal = Serializable::GetManagedValueGeneric<TValue>(nativeptr);
        return returnVal;
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::default::set(TKey key, TValue value)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          apache::geode::client::CacheableKeyPtr keyptr(Serializable::GetUnmanagedValueGeneric<TKey>(key));
        apache::geode::client::CacheablePtr valueptr(Serializable::GetUnmanagedValueGeneric<TValue>(value));
        NativePtr->put(keyptr, valueptr);

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      System::Collections::Generic::IEnumerator<KeyValuePair<TKey, TValue>>^
        Region<TKey, TValue>::GetEnumerator()
      {
        array<KeyValuePair<TKey, TValue>>^ toArray;
        apache::geode::client::VectorOfRegionEntry vc;

        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          NativePtr->entries(vc, false);

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */

          toArray = gcnew array<KeyValuePair<TKey, TValue>>(vc.size());

        for (int32_t index = 0; index < vc.size(); index++)
        {
          apache::geode::client::RegionEntryPtr nativeptr = vc[index];
          TKey key = Serializable::GetManagedValueGeneric<TKey>(nativeptr->getKey());
          TValue val = Serializable::GetManagedValueGeneric<TValue>(nativeptr->getValue());
          toArray[index] = KeyValuePair<TKey, TValue>(key, val);
        }
        return ((System::Collections::Generic::IEnumerable<KeyValuePair<TKey, TValue>>^)toArray)->GetEnumerator();
      }

      generic<class TKey, class TValue>
      System::Collections::IEnumerator^
        Region<TKey, TValue>::GetEnumeratorOld()
      {
        array<Object^>^ toArray;
        apache::geode::client::VectorOfRegionEntry vc;

        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          NativePtr->entries(vc, false);

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */

          toArray = gcnew array<Object^>(vc.size());

        for (int32_t index = 0; index < vc.size(); index++)
        {
          apache::geode::client::RegionEntryPtr nativeptr = vc[index];
          TKey key = Serializable::GetManagedValueGeneric<TKey>(nativeptr->getKey());
          TValue val = Serializable::GetManagedValueGeneric<TValue>(nativeptr->getValue());
          toArray[index] = KeyValuePair<TKey, TValue>(key, val);
        }
        return ((System::Collections::Generic::IEnumerable<Object^>^)toArray)->GetEnumerator();
      }

      generic<class TKey, class TValue>
      bool Region<TKey, TValue>::AreValuesEqual(apache::geode::client::CacheablePtr& val1, apache::geode::client::CacheablePtr& val2)
      {
        if (val1 == NULLPTR && val2 == NULLPTR)
        {
          return true;
        }
        else if ((val1 == NULLPTR && val2 != NULLPTR) || (val1 != NULLPTR && val2 == NULLPTR))
        {
          return false;
        }
        else if (val1 != NULLPTR && val2 != NULLPTR)
        {
          if (val1->classId() != val2->classId() || val1->typeId() != val2->typeId())
          {
            return false;
          }
          apache::geode::client::DataOutput out1;
          apache::geode::client::DataOutput out2;
          val1->toData(out1);
          val2->toData(out2);
          if (out1.getBufferLength() != out2.getBufferLength())
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
      bool Region<TKey, TValue>::Contains(KeyValuePair<TKey, TValue> keyValuePair)
      {
        apache::geode::client::CacheableKeyPtr keyptr(Serializable::GetUnmanagedValueGeneric<TKey>(keyValuePair.Key));
        apache::geode::client::CacheablePtr nativeptr(this->get(keyptr));
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

          apache::geode::client::CacheableKeyPtr keyptr(Serializable::GetUnmanagedValueGeneric<TKey>(key));

        return NativePtr->containsKeyOnServer(keyptr);

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      bool Region<TKey, TValue>::TryGetValue(TKey key, TValue %val)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */
          apache::geode::client::CacheableKeyPtr keyptr(Serializable::GetUnmanagedValueGeneric<TKey>(key));
        apache::geode::client::CacheablePtr nativeptr(this->get(keyptr));
        if (nativeptr == NULLPTR) {
          val = TValue();
          return false;
        }
        else {
          val = Serializable::GetManagedValueGeneric<TValue>(nativeptr);
          return true;
        }

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      System::Collections::Generic::ICollection<TKey>^ Region<TKey, TValue>::Keys::get()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          apache::geode::client::VectorOfCacheableKey vc;
        NativePtr->serverKeys(vc);
        //List<TKey>^ collectionlist = gcnew List<TKey>(vc.size());
        array<TKey>^ keyarr =
          gcnew array<TKey>(vc.size());
        for (int32_t index = 0; index < vc.size(); index++)
        {
          apache::geode::client::CacheableKeyPtr& nativeptr(vc[index]);
          keyarr[index] = Serializable::GetManagedValueGeneric<TKey>(nativeptr);
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

          apache::geode::client::VectorOfCacheable vc;
        NativePtr->values(vc);
        //List<TValue>^ collectionlist = gcnew List<TValue>(vc.size());
        array<TValue>^ valarr =
          gcnew array<TValue>(vc.size());
        for (int32_t index = 0; index < vc.size(); index++)
        {
          apache::geode::client::CacheablePtr& nativeptr(vc[index]);
          valarr[index] = Serializable::GetManagedValueGeneric<TValue>(nativeptr);
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

          apache::geode::client::CacheableKeyPtr keyptr(Serializable::GetUnmanagedValueGeneric<TKey>(key));
        apache::geode::client::CacheablePtr valueptr(Serializable::GetUnmanagedValueGeneric<TValue>(value));
        NativePtr->create(keyptr, valueptr);

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::Add(KeyValuePair<TKey, TValue> keyValuePair)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          apache::geode::client::CacheableKeyPtr keyptr(Serializable::GetUnmanagedValueGeneric<TKey>(keyValuePair.Key));
        apache::geode::client::CacheablePtr valueptr(Serializable::GetUnmanagedValueGeneric<TValue>(keyValuePair.Value));
        NativePtr->create(keyptr, valueptr);

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::Add(TKey key, TValue value, Object^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          apache::geode::client::CacheableKeyPtr keyptr(Serializable::GetUnmanagedValueGeneric<TKey>(key));
        apache::geode::client::CacheablePtr valueptr(Serializable::GetUnmanagedValueGeneric<TValue>(value));
        apache::geode::client::UserDataPtr callbackptr(
          Serializable::GetUnmanagedValueGeneric<Object^>(callbackArg));
        NativePtr->create(keyptr, valueptr, callbackptr);

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      bool Region<TKey, TValue>::Remove(TKey key)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          apache::geode::client::CacheableKeyPtr keyptr(Serializable::GetUnmanagedValueGeneric<TKey>(key));
        return NativePtr->removeEx(keyptr);

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */

      }

      generic<class TKey, class TValue>
      bool Region<TKey, TValue>::Remove(TKey key, Object^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          apache::geode::client::CacheableKeyPtr keyptr(Serializable::GetUnmanagedValueGeneric<TKey>(key));
        apache::geode::client::UserDataPtr callbackptr(
          Serializable::GetUnmanagedValueGeneric<Object^>(callbackArg));
        return NativePtr->removeEx(keyptr, callbackptr);

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      bool Region<TKey, TValue>::Remove(KeyValuePair<TKey, TValue> keyValuePair)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          apache::geode::client::CacheableKeyPtr keyptr(Serializable::GetUnmanagedValueGeneric<TKey>(keyValuePair.Key));
        apache::geode::client::CacheablePtr valueptr(Serializable::GetUnmanagedValueGeneric<TValue>(keyValuePair.Value));

        return NativePtr->remove(keyptr, valueptr);

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      bool Region<TKey, TValue>::Remove(TKey key, TValue value, Object^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          apache::geode::client::CacheableKeyPtr keyptr(Serializable::GetUnmanagedValueGeneric<TKey>(key));
        apache::geode::client::CacheablePtr valueptr(Serializable::GetUnmanagedValueGeneric<TValue>(value));
        apache::geode::client::UserDataPtr callbackptr(Serializable::GetUnmanagedValueGeneric<Object^>(callbackArg));
        return NativePtr->remove(keyptr, valueptr, callbackptr);

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::InvalidateRegion()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          InvalidateRegion(nullptr);

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::InvalidateRegion(Object^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          apache::geode::client::UserDataPtr callbackptr(
          Serializable::GetUnmanagedValueGeneric<Object^>(callbackArg));
        NativePtr->invalidateRegion(callbackptr);

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::DestroyRegion()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          DestroyRegion(nullptr);

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::DestroyRegion(Object^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          apache::geode::client::UserDataPtr callbackptr(
          Serializable::GetUnmanagedValueGeneric<Object^>(callbackArg));
        NativePtr->destroyRegion(callbackptr);

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

          apache::geode::client::CacheableKeyPtr keyptr(Serializable::GetUnmanagedValueGeneric<TKey>(key));
        apache::geode::client::UserDataPtr callbackptr(
          Serializable::GetUnmanagedValueGeneric<Object^>(callbackArg));
        NativePtr->invalidate(keyptr, callbackptr);

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::PutAll(System::Collections::Generic::IDictionary<TKey, TValue>^ map)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          return PutAll(map, DEFAULT_RESPONSE_TIMEOUT);

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */

      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::PutAll(System::Collections::Generic::IDictionary<TKey, TValue>^ map, int timeout)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          apache::geode::client::HashMapOfCacheable nativeMap;
        for each (KeyValuePair<TKey, TValue> keyValPair in map)
        {
          apache::geode::client::CacheableKeyPtr keyptr(Serializable::GetUnmanagedValueGeneric<TKey>(keyValPair.Key));
          apache::geode::client::CacheablePtr valueptr(Serializable::GetUnmanagedValueGeneric<TValue>(keyValPair.Value));
          nativeMap.insert(keyptr, valueptr);
        }
        NativePtr->putAll(nativeMap, timeout);

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */

      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::PutAll(System::Collections::Generic::IDictionary<TKey, TValue>^ map, int timeout, Object^ callbackArg)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          apache::geode::client::HashMapOfCacheable nativeMap;
        for each (KeyValuePair<TKey, TValue> keyValPair in map)
        {
          apache::geode::client::CacheableKeyPtr keyptr(Serializable::GetUnmanagedValueGeneric<TKey>(keyValPair.Key));
          apache::geode::client::CacheablePtr valueptr(Serializable::GetUnmanagedValueGeneric<TValue>(keyValPair.Value));
          nativeMap.insert(keyptr, valueptr);
        }
        apache::geode::client::UserDataPtr callbackptr(
          Serializable::GetUnmanagedValueGeneric<Object^>(callbackArg));
        NativePtr->putAll(nativeMap, timeout, callbackptr);

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

            apache::geode::client::VectorOfCacheableKey vecKeys;

          for each(TKey item in keys)
          {
            vecKeys.push_back(
              Serializable::GetUnmanagedValueGeneric<TKey>(item));
          }

          apache::geode::client::HashMapOfCacheablePtr valuesPtr(NULLPTR);
          if (values != nullptr) {
            valuesPtr = new apache::geode::client::HashMapOfCacheable();
          }
          apache::geode::client::HashMapOfExceptionPtr exceptionsPtr(NULLPTR);
          if (exceptions != nullptr) {
            exceptionsPtr = new apache::geode::client::HashMapOfException();
          }
          NativePtr->getAll(vecKeys, valuesPtr, exceptionsPtr,
                            addToLocalCache);
          if (values != nullptr) {
            for (apache::geode::client::HashMapOfCacheable::Iterator iter =
                  valuesPtr->begin(); iter != valuesPtr->end(); ++iter) {
              TKey key = Serializable::GetManagedValueGeneric<TKey>(iter.first());
              TValue val = Serializable::GetManagedValueGeneric<TValue>(iter.second());
              values->Add(key, val);
            }
          }
          if (exceptions != nullptr) {
            for (apache::geode::client::HashMapOfException::Iterator iter =
                  exceptionsPtr->begin(); iter != exceptionsPtr->end(); ++iter) {
              TKey key = Serializable::GetManagedValueGeneric<TKey>(iter.first());
              System::Exception^ ex = GeodeException::Get(*iter.second());
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

            apache::geode::client::VectorOfCacheableKey vecKeys;

          for each(TKey item in keys)
          {
            vecKeys.push_back(
              Serializable::GetUnmanagedValueGeneric<TKey>(item));
          }

          apache::geode::client::HashMapOfCacheablePtr valuesPtr(NULLPTR);
          if (values != nullptr) {
            valuesPtr = new apache::geode::client::HashMapOfCacheable();
          }
          apache::geode::client::HashMapOfExceptionPtr exceptionsPtr(NULLPTR);
          if (exceptions != nullptr) {
            exceptionsPtr = new apache::geode::client::HashMapOfException();
          }

          apache::geode::client::UserDataPtr callbackptr(
            Serializable::GetUnmanagedValueGeneric<Object^>(callbackArg));

          NativePtr->getAll(vecKeys, valuesPtr, exceptionsPtr,
                            addToLocalCache, callbackptr);
          if (values != nullptr) {
            for (apache::geode::client::HashMapOfCacheable::Iterator iter =
                  valuesPtr->begin(); iter != valuesPtr->end(); ++iter) {
              TKey key = Serializable::GetManagedValueGeneric<TKey>(iter.first());
              TValue val = Serializable::GetManagedValueGeneric<TValue>(iter.second());
              values->Add(key, val);
            }
          }
          if (exceptions != nullptr) {
            for (apache::geode::client::HashMapOfException::Iterator iter =
                  exceptionsPtr->begin(); iter != exceptionsPtr->end(); ++iter) {
              TKey key = Serializable::GetManagedValueGeneric<TKey>(iter.first());
              System::Exception^ ex = GeodeException::Get(*iter.second());
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

          apache::geode::client::VectorOfCacheableKey vecKeys;
        for each(TKey item in keys)
          vecKeys.push_back(Serializable::GetUnmanagedValueGeneric<TKey>(item));

        apache::geode::client::UserDataPtr callbackptr(
          Serializable::GetUnmanagedValueGeneric<Object^>(callbackArg));

        NativePtr->removeAll(vecKeys, callbackptr);

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */

      }
      generic<class TKey, class TValue>
      String^ Region<TKey, TValue>::Name::get()
      {
        return ManagedString::Get(NativePtr->getName());
      }

      generic<class TKey, class TValue>
      String^ Region<TKey, TValue>::FullPath::get()
      {
        return ManagedString::Get(NativePtr->getFullPath());
      }

      generic<class TKey, class TValue>
      IRegion<TKey, TValue>^ Region<TKey, TValue>::ParentRegion::get()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          apache::geode::client::RegionPtr& nativeptr(NativePtr->getParentRegion());

        return Region::Create(nativeptr.ptr());

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      Apache::Geode::Client::RegionAttributes<TKey, TValue>^ Region<TKey, TValue>::Attributes::get()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          apache::geode::client::RegionAttributesPtr& nativeptr(NativePtr->getAttributes());

        return Apache::Geode::Client::RegionAttributes<TKey, TValue>::Create(nativeptr.ptr());

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      AttributesMutator<TKey, TValue>^ Region<TKey, TValue>::AttributesMutator::get()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          apache::geode::client::AttributesMutatorPtr& nativeptr(
          NativePtr->getAttributesMutator());

        return Apache::Geode::Client::AttributesMutator<TKey, TValue>::Create(nativeptr.ptr());

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      Apache::Geode::Client::CacheStatistics^ Region<TKey, TValue>::Statistics::get()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          apache::geode::client::CacheStatisticsPtr& nativeptr(NativePtr->getStatistics());
        return Apache::Geode::Client::CacheStatistics::Create(nativeptr.ptr());

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      IRegion<TKey, TValue>^ Region<TKey, TValue>::GetSubRegion(String^ path)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          ManagedString mg_path(path);
        apache::geode::client::RegionPtr& nativeptr(
          NativePtr->getSubregion(mg_path.CharPtr));
        return Region::Create(nativeptr.ptr());

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      IRegion<TKey, TValue>^ Region<TKey, TValue>::CreateSubRegion(String^ subRegionName,
                                                                    Apache::Geode::Client::RegionAttributes<TKey, TValue>^ attributes)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          ManagedString mg_subregionName(subRegionName);
        //TODO:split
        apache::geode::client::RegionAttributesPtr p_attrs(
          GetNativePtrFromSBWrapGeneric<apache::geode::client::RegionAttributes>(attributes));

        apache::geode::client::RegionPtr& nativeptr(NativePtr->createSubregion(
          mg_subregionName.CharPtr, p_attrs /*NULLPTR*/));
        return Region::Create(nativeptr.ptr());

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */

      }

      generic<class TKey, class TValue>
      System::Collections::Generic::ICollection<IRegion<TKey, TValue>^>^ Region<TKey, TValue>::SubRegions(bool recursive)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          apache::geode::client::VectorOfRegion vsr;
        NativePtr->subregions(recursive, vsr);
        array<IRegion<TKey, TValue>^>^ subRegions =
          gcnew array<IRegion<TKey, TValue>^>(vsr.size());

        for (int32_t index = 0; index < vsr.size(); index++)
        {
          apache::geode::client::RegionPtr& nativeptr(vsr[index]);
          subRegions[index] = Region<TKey, TValue>::Create(nativeptr.ptr());
        }
        System::Collections::Generic::ICollection<IRegion<TKey, TValue>^>^ collection =
          (System::Collections::Generic::ICollection<IRegion<TKey, TValue>^>^)subRegions;
        return collection;

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      RegionEntry<TKey, TValue>^ Region<TKey, TValue>::GetEntry(TKey key)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          apache::geode::client::CacheableKeyPtr keyptr(Serializable::GetUnmanagedValueGeneric<TKey>(key));
        apache::geode::client::RegionEntryPtr& nativeptr(NativePtr->getEntry(keyptr));
        return RegionEntry<TKey, TValue>::Create(nativeptr.ptr());

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      System::Collections::Generic::ICollection<RegionEntry<TKey, TValue>^>^ Region<TKey, TValue>::GetEntries(bool recursive)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          apache::geode::client::VectorOfRegionEntry vc;
        NativePtr->entries(vc, recursive);
        array<RegionEntry<TKey, TValue>^>^ entryarr = gcnew array<RegionEntry<TKey, TValue>^>(vc.size());

        for (int32_t index = 0; index < vc.size(); index++)
        {
          apache::geode::client::RegionEntryPtr& nativeptr(vc[index]);
          entryarr[index] = RegionEntry<TKey, TValue>::Create(nativeptr.ptr());
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

          apache::geode::client::RegionServicePtr& nativeptr(NativePtr->getRegionService());

        apache::geode::client::Cache* realCache = dynamic_cast<apache::geode::client::Cache*>(nativeptr.ptr());

        if (realCache != NULL)
        {
          return Apache::Geode::Client::Cache::Create(((apache::geode::client::CachePtr)nativeptr).ptr());
        }
        else
        {
          return Apache::Geode::Client::AuthenticatedCache::Create(nativeptr.ptr());
        }

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      bool Region<TKey, TValue>::ContainsValueForKey(TKey key)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          apache::geode::client::CacheableKeyPtr keyptr(Serializable::GetUnmanagedValueGeneric<TKey>(key));
        return NativePtr->containsValueForKey(keyptr);

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
          apache::geode::client::UserDataPtr callbackptr(
          Serializable::GetUnmanagedValueGeneric<Object^>(callbackArg));
        NativePtr->clear(callbackptr);
        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::CopyTo(array<KeyValuePair<TKey, TValue>>^ toArray,
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

          apache::geode::client::VectorOfRegionEntry vc;
        NativePtr->entries(vc, false);

        if (toArray->Rank > 1 || (vc.size() > (toArray->Length - startIdx)))
        {
          throw gcnew System::ArgumentException;
        }

        for (int32_t index = 0; index < vc.size(); index++)
        {
          apache::geode::client::RegionEntryPtr nativeptr = vc[index];
          TKey key = Serializable::GetManagedValueGeneric<TKey>(nativeptr->getKey());
          TValue val = Serializable::GetManagedValueGeneric<TValue>(nativeptr->getValue());
          toArray[startIdx] = KeyValuePair<TKey, TValue>(key, val);
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
      void Region<TKey, TValue>::RegisterKeys(System::Collections::Generic::ICollection<TKey>^ keys)
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

            apache::geode::client::VectorOfCacheableKey vecKeys;

          for each(TKey item in keys)
          {
            vecKeys.push_back(
              Serializable::GetUnmanagedValueGeneric<TKey>(item));
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

            apache::geode::client::VectorOfCacheableKey vecKeys;

          for each(TKey item in keys)
          {
            vecKeys.push_back(
              Serializable::GetUnmanagedValueGeneric<TKey>(item));
          }

          NativePtr->unregisterKeys(vecKeys);

          _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
        }
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::RegisterAllKeys()
      {
        RegisterAllKeys(false, nullptr, false);
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::RegisterAllKeys(bool isDurable)
      {
        RegisterAllKeys(isDurable, nullptr, false);
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
            apache::geode::client::VectorOfCacheableKeyPtr mg_keys(
              new apache::geode::client::VectorOfCacheableKey());

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

          apache::geode::client::VectorOfCacheableKey vc;
        NativePtr->getInterestList(vc);
        //List<TValue>^ collectionlist = gcnew List<TValue>(vc.size());
        array<TKey>^ keyarr =
          gcnew array<TKey>(vc.size());
        for (int32_t index = 0; index < vc.size(); index++)
        {
          apache::geode::client::CacheableKeyPtr& nativeptr(vc[index]);
          keyarr[index] = Serializable::GetManagedValueGeneric<TKey>(nativeptr);
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

          apache::geode::client::VectorOfCacheableString vc;
        NativePtr->getInterestListRegex(vc);
        array<String^>^ strarr =
          gcnew array<String^>(vc.size());
        //List<String>^ collectionlist = gcnew List<String>(vc.size());
        for (int32_t index = 0; index < vc.size(); index++)
        {
          strarr[index] = ManagedString::Get(vc[index]->asChar());
          //collectionlist[ index ] = Serializable::GetManagedValue<TValue>(nativeptr);
        }
        System::Collections::Generic::ICollection<String^>^ collectionlist =
          (System::Collections::Generic::ICollection<String^>^)strarr;
        return collectionlist;

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::UnregisterAllKeys()
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          NativePtr->unregisterAllKeys();

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */

      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::RegisterRegex(String^ regex)
      {
        RegisterRegex(regex, false, nullptr, false);
      }

      generic<class TKey, class TValue>
      void Region<TKey, TValue>::RegisterRegex(String^ regex, bool isDurable)
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

          ManagedString mg_regex(regex);
        if (resultKeys != nullptr) {
          apache::geode::client::VectorOfCacheableKeyPtr mg_keys(
            new apache::geode::client::VectorOfCacheableKey());
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
      void Region<TKey, TValue>::UnregisterRegex(String^ regex)
      {
        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          ManagedString mg_regex(regex);
        NativePtr->unregisterRegex(mg_regex.CharPtr);

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */

      }

      generic<class TKey, class TValue>
      generic<class TResult>
      ISelectResults<TResult>^ Region<TKey, TValue>::Query(String^ predicate)
      {
        //return Query( predicate, DEFAULT_QUERY_RESPONSE_TIMEOUT );
        ManagedString mg_predicate(predicate);

        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          apache::geode::client::SelectResultsPtr& nativeptr = NativePtr->query(
          mg_predicate.CharPtr, DEFAULT_QUERY_RESPONSE_TIMEOUT);
        if (nativeptr.ptr() == NULL) return nullptr;

        apache::geode::client::ResultSet* resultptr = dynamic_cast<apache::geode::client::ResultSet*>(
          nativeptr.ptr());
        if (resultptr == NULL)
        {
          apache::geode::client::StructSet* structptr = dynamic_cast<apache::geode::client::StructSet*>(
            nativeptr.ptr());
          if (structptr == NULL)
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
      ISelectResults<TResult>^ Region<TKey, TValue>::Query(String^ predicate, uint32_t timeout)
      {
        ManagedString mg_predicate(predicate);

        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          apache::geode::client::SelectResultsPtr& nativeptr = NativePtr->query(
          mg_predicate.CharPtr, timeout);
        if (nativeptr.ptr() == NULL) return nullptr;

        apache::geode::client::ResultSet* resultptr = dynamic_cast<apache::geode::client::ResultSet*>(
          nativeptr.ptr());
        if (resultptr == NULL)
        {
          apache::geode::client::StructSet* structptr = dynamic_cast<apache::geode::client::StructSet*>(
            nativeptr.ptr());
          if (structptr == NULL)
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
      bool Region<TKey, TValue>::ExistsValue(String^ predicate)
      {
        return ExistsValue(predicate, DEFAULT_QUERY_RESPONSE_TIMEOUT);
      }

      generic<class TKey, class TValue>
      bool Region<TKey, TValue>::ExistsValue(String^ predicate, uint32_t timeout)
      {
        ManagedString mg_predicate(predicate);

        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          return NativePtr->existsValue(mg_predicate.CharPtr, timeout);

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */

      }

      generic<class TKey, class TValue>
      Object^ Region<TKey, TValue>::SelectValue(String^ predicate)
      {
        return SelectValue(predicate, DEFAULT_QUERY_RESPONSE_TIMEOUT);
      }

      generic<class TKey, class TValue>
      Object^ Region<TKey, TValue>::SelectValue(String^ predicate, uint32_t timeout)
      {
        ManagedString mg_predicate(predicate);

        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          apache::geode::client::CacheablePtr& nativeptr(NativePtr->selectValue(
          mg_predicate.CharPtr, timeout));

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
        return (_NativePtr != nullptr ?
                gcnew LocalRegion<TKey, TValue>(_NativePtr) : nullptr);
      }
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache
