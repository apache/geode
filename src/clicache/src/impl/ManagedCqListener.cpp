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
#include "ManagedCqListener.hpp"
#include "../ICqListener.hpp"
#include "../CqEvent.hpp"

#include "ManagedString.hpp"
#include "../ExceptionTypes.hpp"
#include "SafeConvert.hpp"
#include <string>


using namespace System;
using namespace System::Text;
using namespace System::Reflection;


//using namespace apache::geode::client;
namespace apache
{
  namespace geode
  {
    namespace client
    {

      apache::geode::client::CqListener* ManagedCqListenerGeneric::create(const char* assemblyPath,
                                                                          const char* factoryFunctionName)
      {
        try
        {
          String^ mg_assemblyPath =
            Apache::Geode::Client::ManagedString::Get(assemblyPath);
          String^ mg_factoryFunctionName =
            Apache::Geode::Client::ManagedString::Get(factoryFunctionName);
          String^ mg_typeName = nullptr;
          int32_t dotIndx = -1;

          if (mg_factoryFunctionName == nullptr ||
              (dotIndx = mg_factoryFunctionName->LastIndexOf('.')) < 0)
          {
            std::string ex_str = "ManagedCqListener: Factory function name '";
            ex_str += factoryFunctionName;
            ex_str += "' does not contain type name";
            throw IllegalArgumentException(ex_str.c_str());
          }

          mg_typeName = mg_factoryFunctionName->Substring(0, dotIndx);
          mg_factoryFunctionName = mg_factoryFunctionName->Substring(dotIndx + 1);

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
            std::string ex_str = "ManagedCqListener: Could not load assembly: ";
            ex_str += assemblyPath;
            throw IllegalArgumentException(ex_str.c_str());
          }
          Object^ typeInst = assmb->CreateInstance(mg_typeName, true);
          if (typeInst != nullptr)
          {
            MethodInfo^ mInfo = typeInst->GetType()->GetMethod(mg_factoryFunctionName,
                                                               BindingFlags::Public | BindingFlags::Static | BindingFlags::IgnoreCase);
            if (mInfo != nullptr)
            {
              Apache::Geode::Client::ICqListener<Object^, Object^>^ managedptr = nullptr;
              try
              {
                managedptr = dynamic_cast<Apache::Geode::Client::ICqListener<Object^, Object^>^>(
                  mInfo->Invoke(typeInst, nullptr));
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
                throw IllegalArgumentException(ex_str.c_str());
              }
              return new ManagedCqListenerGeneric((Apache::Geode::Client::ICqListener<Object^, Object^>^)managedptr);
            }
            else
            {
              std::string ex_str = "ManagedCqListener: Could not load "
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
            std::string ex_str = "ManagedCqListener: Could not load type [";
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
          std::string ex_str = "ManagedCqListener: Got an exception while "
            "loading managed library: ";
          ex_str += mg_exStr.CharPtr;
          throw IllegalArgumentException(ex_str.c_str());
        }
        return NULL;
      }

      void ManagedCqListenerGeneric::onEvent(const CqEvent& ev)
      {
        try {

          Apache::Geode::Client::CqEvent<Object^, Object^> mevent(&ev);
          m_managedptr->OnEvent(%mevent);
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::ManagedString mg_exStr(ex->ToString());
          std::string ex_str = "ManagedCqListener: Got an exception in"
            "onEvent: ";
          ex_str += mg_exStr.CharPtr;
          throw IllegalArgumentException(ex_str.c_str());
        }
      }

      void ManagedCqListenerGeneric::onError(const CqEvent& ev)
      {
        Apache::Geode::Client::CqEvent<Object^, Object^> mevent(&ev);
        m_managedptr->OnError(%mevent);
      }

      void ManagedCqListenerGeneric::close()
      {
        try {
          m_managedptr->Close();
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::ManagedString mg_exStr(ex->ToString());
          std::string ex_str = "ManagedCqListener: Got an exception in"
            "close: ";
          ex_str += mg_exStr.CharPtr;
          throw IllegalArgumentException(ex_str.c_str());
        }
      }

    }  // namespace client
  }  // namespace geode
}  // namespace apache

