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
#include "ManagedAuthInitialize.hpp"
#include "../IAuthInitialize.hpp"
#include "ManagedString.hpp"
#include "../ExceptionTypes.hpp"
#include "Properties.hpp"
#include <string>

using namespace System;
using namespace System::Text;
using namespace System::Reflection;

namespace apache
{
  namespace geode
  {
    namespace client
    {

      AuthInitialize* ManagedAuthInitializeGeneric::create(const char* assemblyPath,
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
            std::string ex_str = "ManagedAuthInitializeGeneric: Factory function name '";
            ex_str += factoryFunctionName;
            ex_str += "' does not contain type name";
            throw AuthenticationRequiredException(ex_str.c_str());
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
            std::string ex_str = "ManagedAuthInitializeGeneric: Could not load assembly: ";
            ex_str += assemblyPath;
            throw AuthenticationRequiredException(ex_str.c_str());
          }

          Object^ typeInst = assmb->CreateInstance(mg_typeName, true);

          //Type^ typeInst = assmb->GetType(mg_typeName, false, true);

          if (typeInst != nullptr)
          {
            /*
            array<Type^>^ types = gcnew array<Type^>(2);
            types[0] = Type::GetType(mg_genericKey, false, true);
            types[1] = Type::GetType(mg_genericVal, false, true);

            if (types[0] == nullptr || types[1] == nullptr)
            {
            std::string ex_str = "ManagedAuthInitializeGeneric: Could not get both generic type argument instances";
            throw apache::geode::client::IllegalArgumentException( ex_str.c_str( ) );
            }
            */

            //typeInst = typeInst->GetType()->MakeGenericType(types);
            Apache::Geode::Client::Log::Info("Loading function: [{0}]", mg_factoryFunctionName);

            /*
            MethodInfo^ mInfo = typeInst->GetMethod( mg_factoryFunctionName,
            BindingFlags::Public | BindingFlags::Static | BindingFlags::IgnoreCase );
            */

            MethodInfo^ mInfo = typeInst->GetType()->GetMethod(mg_factoryFunctionName,
                                                               BindingFlags::Public | BindingFlags::Static | BindingFlags::IgnoreCase);

            if (mInfo != nullptr)
            {
              Object^ userptr = nullptr;
              try
              {
                userptr = mInfo->Invoke(typeInst, nullptr);
              }
              catch (System::Exception^)
              {
                userptr = nullptr;
              }
              if (userptr == nullptr)
              {
                std::string ex_str = "ManagedAuthInitializeGeneric: Could not create "
                  "object on invoking factory function [";
                ex_str += factoryFunctionName;
                ex_str += "] in assembly: ";
                ex_str += assemblyPath;
                throw AuthenticationRequiredException(ex_str.c_str());
              }
              ManagedAuthInitializeGeneric * maig = new ManagedAuthInitializeGeneric(safe_cast<Apache::Geode::Client::IAuthInitialize^>(userptr));
              return maig;
            }
            else
            {
              std::string ex_str = "ManagedAuthInitializeGeneric: Could not load "
                "function with name [";
              ex_str += factoryFunctionName;
              ex_str += "] in assembly: ";
              ex_str += assemblyPath;
              throw AuthenticationRequiredException(ex_str.c_str());
            }
          }
          else
          {
            Apache::Geode::Client::ManagedString typeName(mg_typeName);
            std::string ex_str = "ManagedAuthInitializeGeneric: Could not load type [";
            ex_str += typeName.CharPtr;
            ex_str += "] in assembly: ";
            ex_str += assemblyPath;
            throw AuthenticationRequiredException(ex_str.c_str());
          }
        }
        catch (const apache::geode::client::AuthenticationRequiredException&)
        {
          throw;
        }
        catch (const apache::geode::client::Exception& ex)
        {
          std::string ex_str = "ManagedAuthInitializeGeneric: Got an exception while "
            "loading managed library: ";
          ex_str += ex.getName();
          ex_str += ": ";
          ex_str += ex.getMessage();
          throw AuthenticationRequiredException(ex_str.c_str());
        }
        catch (System::Exception^ ex)
        {
          Apache::Geode::Client::ManagedString mg_exStr(ex->ToString());
          std::string ex_str = "ManagedAuthInitializeGeneric: Got an exception while "
            "loading managed library: ";
          ex_str += mg_exStr.CharPtr;
          throw AuthenticationRequiredException(ex_str.c_str());
        }
        return NULL;
      }

      PropertiesPtr ManagedAuthInitializeGeneric::getCredentials(PropertiesPtr&
                                                                 securityprops, const char* server)
      {
        try {
          Apache::Geode::Client::Properties<String^, String^>^ mprops =
            Apache::Geode::Client::Properties<String^, String^>::Create<String^, String^>(securityprops.ptr());
          String^ mg_server = Apache::Geode::Client::ManagedString::Get(server);

          return PropertiesPtr(m_managedptr->GetCredentials(mprops, mg_server)->NativePtr());
        }
        catch (Apache::Geode::Client::GeodeException^ ex) {
          ex->ThrowNative();
        }
        catch (System::Exception^ ex) {
          Apache::Geode::Client::GeodeException::ThrowNative(ex);
        }
        return NULLPTR;
      }

      void ManagedAuthInitializeGeneric::close()
      {
        try {
          m_managedptr->Close();
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
