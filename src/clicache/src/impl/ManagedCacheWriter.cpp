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

//#include "../gf_includes.hpp"
#include "ManagedCacheWriter.hpp"
#include "../ICacheWriter.hpp"
#include "../Region.hpp"
#include "../RegionEvent.hpp"
#include "../EntryEvent.hpp"
#include "../Log.hpp"
#include "ManagedString.hpp"
#include "../ExceptionTypes.hpp"
#include "SafeConvert.hpp"
#include "CacheWriter.hpp"

using namespace System;
using namespace System::Text;
using namespace System::Reflection;

namespace apache
{
  namespace geode
  {
    namespace client
    {

      CacheWriter* ManagedCacheWriterGeneric::create(const char* assemblyPath,
                                                     const char* factoryFunctionName)
      {
        try
        {
          String^ mg_assemblyPath =
            Apache::Geode::Client::ManagedString::Get(assemblyPath);
          String^ mg_factoryFunctionName =
            Apache::Geode::Client::ManagedString::Get(factoryFunctionName);
          String^ mg_typeName = nullptr;

          String^ mg_genericKey = nullptr;
          String^ mg_genericVal = nullptr;

          int32_t dotIndx = -1;
          int32_t genericsOpenIndx = -1;
          int32_t genericsCloseIndx = -1;
          int32_t commaIndx = -1;

          if (mg_factoryFunctionName == nullptr ||
              (dotIndx = mg_factoryFunctionName->LastIndexOf('.')) < 0)
          {
            std::string ex_str = "ManagedCacheWriterGeneric: Factory function name '";
            ex_str += factoryFunctionName;
            ex_str += "' does not contain type name";
            throw IllegalArgumentException(ex_str.c_str());
          }

          if ((genericsCloseIndx = mg_factoryFunctionName->LastIndexOf('>')) < 0)
          {
            std::string ex_str = "ManagedCacheWriterGeneric: Factory function name '";
            ex_str += factoryFunctionName;
            ex_str += "' does not contain any generic type parameters";
            throw apache::geode::client::IllegalArgumentException(ex_str.c_str());
          }

          if ((genericsOpenIndx = mg_factoryFunctionName->LastIndexOf('<')) < 0 ||
              genericsOpenIndx > genericsCloseIndx)
          {
            std::string ex_str = "ManagedCacheWriterGeneric: Factory function name '";
            ex_str += factoryFunctionName;
            ex_str += "' does not contain expected generic type parameters";
            throw apache::geode::client::IllegalArgumentException(ex_str.c_str());
          }

          if ((commaIndx = mg_factoryFunctionName->LastIndexOf(',')) < 0 ||
              (commaIndx < genericsOpenIndx || commaIndx > genericsCloseIndx))
          {
            std::string ex_str = "ManagedCacheWriterGeneric: Factory function name '";
            ex_str += factoryFunctionName;
            ex_str += "' does not contain expected generic type parameter comma separator";
            throw apache::geode::client::IllegalArgumentException(ex_str.c_str());
          }

          StringBuilder^ typeBuilder = gcnew StringBuilder(mg_factoryFunctionName->Substring(0, genericsOpenIndx));
          mg_typeName = typeBuilder->ToString();
          mg_genericKey = mg_factoryFunctionName->Substring(genericsOpenIndx + 1, commaIndx - genericsOpenIndx - 1);
          mg_genericKey = mg_genericKey->Trim();
          mg_genericVal = mg_factoryFunctionName->Substring(commaIndx + 1, genericsCloseIndx - commaIndx - 1);
          mg_genericVal = mg_genericVal->Trim();
          mg_factoryFunctionName = mg_factoryFunctionName->Substring(dotIndx + 1);

          Apache::Geode::Client::Log::Fine(
            "Attempting to instantiate a [{0}<{1}, {2}>] via the [{3}] factory method.",
            mg_typeName, mg_genericKey, mg_genericVal, mg_factoryFunctionName);

          typeBuilder->Append("`2");
          mg_typeName = typeBuilder->ToString();

          Assembly^ assmb = nullptr;
          try
          {
            assmb = Assembly::Load(mg_assemblyPath);
          }
          catch (System::Exception^)
          {
            assmb = nullptr;
          }
          if (assmb == nullptr)
          {
            std::string ex_str = "ManagedCacheWriterGeneric: Could not load assembly: ";
            ex_str += assemblyPath;
            throw IllegalArgumentException(ex_str.c_str());
          }

          Apache::Geode::Client::Log::Debug("Loading type: [{0}]", mg_typeName);

          Type^ typeInst = assmb->GetType(mg_typeName, false, true);

          if (typeInst != nullptr)
          {
            array<Type^>^ types = gcnew array<Type^>(2);
            types[0] = Type::GetType(mg_genericKey, false, true);
            types[1] = Type::GetType(mg_genericVal, false, true);

            if (types[0] == nullptr || types[1] == nullptr)
            {
              std::string ex_str = "ManagedCacheWriterGeneric: Could not get both generic type argument instances";
              throw apache::geode::client::IllegalArgumentException(ex_str.c_str());
            }

            typeInst = typeInst->MakeGenericType(types);
            Apache::Geode::Client::Log::Info("Loading function: [{0}]", mg_factoryFunctionName);

            MethodInfo^ mInfo = typeInst->GetMethod(mg_factoryFunctionName,
                                                    BindingFlags::Public | BindingFlags::Static | BindingFlags::IgnoreCase);
            if (mInfo != nullptr)
            {
              Object^ managedptr = nullptr;
              try
              {
                managedptr = mInfo->Invoke(typeInst, nullptr);
              }
              catch (System::Exception^ ex)
              {
                Apache::Geode::Client::Log::Debug("{0}: {1}", ex->GetType()->Name, ex->Message);
                managedptr = nullptr;
              }
              if (managedptr == nullptr)
              {
                std::string ex_str = "ManagedCacheWriterGeneric: Could not create "
                  "object on invoking factory function [";
                ex_str += factoryFunctionName;
                ex_str += "] in assembly: ";
                ex_str += assemblyPath;
                throw IllegalArgumentException(ex_str.c_str());
              }
              ManagedCacheWriterGeneric* mgcw = new ManagedCacheWriterGeneric(managedptr);

              Type^ cwgType = Type::GetType("Apache.Geode.Client.CacheWriterGeneric`2");
              cwgType = cwgType->MakeGenericType(types);
              Object^ cwg = Activator::CreateInstance(cwgType);

              mInfo = cwgType->GetMethod("SetCacheWriter");
              array<Object^>^ params = gcnew array<Object^>(1);
              params[0] = managedptr;
              mInfo->Invoke(cwg, params);

              mgcw->setptr((Apache::Geode::Client::ICacheWriter<Object^, Object^>^)cwg);

              return mgcw;
            }
            else
            {
              std::string ex_str = "ManagedCacheWriterGeneric: Could not load "
                "function with name [";
              ex_str += factoryFunctionName;
              ex_str += "] in assembly: ";
              ex_str += assemblyPath;
              throw IllegalArgumentException(ex_str.c_str());
            }
          }
          else
          {
            Apache::Geode::Client::ManagedString typeName(mg_typeName);
            std::string ex_str = "ManagedCacheWriterGeneric: Could not load type [";
            ex_str += typeName.CharPtr;
            ex_str += "] in assembly: ";
            ex_str += assemblyPath;
            throw IllegalArgumentException(ex_str.c_str());
          }
        }
        catch (const apache::geode::client::Exception&)
        {
          throw;
        }
        catch (System::Exception^ ex)
        {
          Apache::Geode::Client::ManagedString mg_exStr(ex->ToString());
          std::string ex_str = "ManagedCacheWriterGeneric: Got an exception while "
            "loading managed library: ";
          ex_str += mg_exStr.CharPtr;
          throw IllegalArgumentException(ex_str.c_str());
        }
        return NULL;
      }

      bool ManagedCacheWriterGeneric::beforeUpdate(const EntryEvent& ev)
      {
        try {
          Apache::Geode::Client::EntryEvent<Object^, Object^> mevent(&ev);

          return m_managedptr->BeforeUpdate(%mevent);
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
        return false;
      }

      bool ManagedCacheWriterGeneric::beforeCreate(const EntryEvent& ev)
      {
        try {
          Apache::Geode::Client::EntryEvent<Object^, Object^> mevent(&ev);

          return m_managedptr->BeforeCreate(%mevent);
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
        return false;
      }

      bool ManagedCacheWriterGeneric::beforeDestroy(const EntryEvent& ev)
      {
        try {
          Apache::Geode::Client::EntryEvent<Object^, Object^> mevent(&ev);

          return m_managedptr->BeforeDestroy(%mevent);
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
        return false;
      }
      bool ManagedCacheWriterGeneric::beforeRegionClear(const RegionEvent& ev)
      {
        try {
          Apache::Geode::Client::RegionEvent<Object^, Object^> mevent(&ev);

          return m_managedptr->BeforeRegionClear(%mevent);
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
        return false;
      }

      bool ManagedCacheWriterGeneric::beforeRegionDestroy(const RegionEvent& ev)
      {
        try {
          Apache::Geode::Client::RegionEvent<Object^, Object^> mevent(&ev);

          return m_managedptr->BeforeRegionDestroy(%mevent);
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
        return false;
      }

      void ManagedCacheWriterGeneric::close(const RegionPtr& rp)
      {
        try {
          Apache::Geode::Client::IRegion<Object^, Object^>^ mregion =
            Apache::Geode::Client::Region<Object^, Object^>::Create(rp.ptr());

          m_managedptr->Close(reinterpret_cast<Apache::Geode::Client::Region<Object^, Object^>^>(mregion));
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
      }

    }  // namespace client
  }  // namespace geode
}  // namespace apache

