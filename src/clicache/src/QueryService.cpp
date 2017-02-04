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
#include "QueryService.hpp"
#include "Query.hpp"
#include "Log.hpp"
#include "CqAttributes.hpp"
#include "CqQuery.hpp"
#include "CqServiceStatistics.hpp"
#include "impl/ManagedString.hpp"
#include "ExceptionTypes.hpp"
#include "impl/SafeConvert.hpp"

using namespace System;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      generic<class TKey, class TResult>
      //generic<class TResult>
      Query<TResult>^ QueryService<TKey, TResult>::NewQuery(String^ query)
      {
        ManagedString mg_queryStr(query);

        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          return Query<TResult>::Create(NativePtr->newQuery(
          mg_queryStr.CharPtr).ptr());

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TKey, class TResult>
      CqQuery<TKey, TResult>^ QueryService<TKey, TResult>::NewCq(String^ query, CqAttributes<TKey, TResult>^ cqAttr, bool isDurable)
      {
        ManagedString mg_queryStr(query);
        apache::geode::client::CqAttributesPtr attr(GetNativePtrFromSBWrapGeneric<apache::geode::client::CqAttributes>(cqAttr));
        try
        {
          return CqQuery<TKey, TResult>::Create(NativePtr->newCq(
            mg_queryStr.CharPtr, attr, isDurable).ptr());
        }
        catch (const apache::geode::client::Exception& ex)
        {
          throw GeodeException::Get(ex);
        }
      }

      generic<class TKey, class TResult>
      CqQuery<TKey, TResult>^ QueryService<TKey, TResult>::NewCq(String^ name, String^ query, CqAttributes<TKey, TResult>^ cqAttr, bool isDurable)
      {
        ManagedString mg_queryStr(query);
        ManagedString mg_nameStr(name);
        apache::geode::client::CqAttributesPtr attr(GetNativePtrFromSBWrapGeneric<apache::geode::client::CqAttributes>(cqAttr));
        try
        {
          return CqQuery<TKey, TResult>::Create(NativePtr->newCq(
            mg_nameStr.CharPtr, mg_queryStr.CharPtr, attr, isDurable).ptr());
        }
        catch (const apache::geode::client::Exception& ex)
        {
          throw GeodeException::Get(ex);
        }
      }

      generic<class TKey, class TResult>
      void QueryService<TKey, TResult>::CloseCqs()
      {
        try
        {
          NativePtr->closeCqs();
        }
        catch (const apache::geode::client::Exception& ex)
        {
          throw GeodeException::Get(ex);
        }
      }

      generic<class TKey, class TResult>
      array<CqQuery<TKey, TResult>^>^ QueryService<TKey, TResult>::GetCqs()
      {
        try
        {
          apache::geode::client::VectorOfCqQuery vrr;
          NativePtr->getCqs(vrr);
          array<CqQuery<TKey, TResult>^>^ cqs = gcnew array<CqQuery<TKey, TResult>^>(vrr.size());

          for (int32_t index = 0; index < vrr.size(); index++)
          {
            cqs[index] = CqQuery<TKey, TResult>::Create(vrr[index].ptr());
          }
          return cqs;
        }
        catch (const apache::geode::client::Exception& ex)
        {
          throw GeodeException::Get(ex);
        }
      }

      generic<class TKey, class TResult>
      CqQuery<TKey, TResult>^ QueryService<TKey, TResult>::GetCq(String^ name)
      {
        ManagedString mg_queryStr(name);
        try
        {
          return CqQuery<TKey, TResult>::Create(NativePtr->getCq(
            mg_queryStr.CharPtr).ptr());
        }
        catch (const apache::geode::client::Exception& ex)
        {
          throw GeodeException::Get(ex);
        }
      }

      generic<class TKey, class TResult>
      void QueryService<TKey, TResult>::ExecuteCqs()
      {
        try
        {
          NativePtr->executeCqs();
        }
        catch (const apache::geode::client::Exception& ex)
        {
          throw GeodeException::Get(ex);
        }
      }

      generic<class TKey, class TResult>
      void QueryService<TKey, TResult>::StopCqs()
      {
        try
        {
          NativePtr->stopCqs();
        }
        catch (const apache::geode::client::Exception& ex)
        {
          throw GeodeException::Get(ex);
        }
      }

      generic<class TKey, class TResult>
      CqServiceStatistics^ QueryService<TKey, TResult>::GetCqStatistics()
      {
        try
        {
          return CqServiceStatistics::Create(NativePtr->getCqServiceStatistics().ptr());
        }
        catch (const apache::geode::client::Exception& ex)
        {
          throw GeodeException::Get(ex);
        }
      }

      generic<class TKey, class TResult>
      System::Collections::Generic::List<String^>^ QueryService<TKey, TResult>::GetAllDurableCqsFromServer()
      {
        try
        {
          apache::geode::client::CacheableArrayListPtr durableCqsArrayListPtr = NativePtr->getAllDurableCqsFromServer();
          int length = durableCqsArrayListPtr != NULLPTR ? durableCqsArrayListPtr->length() : 0;
          System::Collections::Generic::List<String^>^ durableCqsList = gcnew System::Collections::Generic::List<String^>();
          if (length > 0)
          {
            for (int i = 0; i < length; i++)
            {
              durableCqsList->Add(CacheableString::GetString(durableCqsArrayListPtr->at(i)));
            }
          }
          return durableCqsList;
        }
        catch (const apache::geode::client::Exception& ex)
        {
          throw GeodeException::Get(ex);
        }
      }
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache
