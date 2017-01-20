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
#include "../IGFSerializable.hpp"
#include "../GemFireClassIds.hpp"
using namespace System;
using namespace System::Collections::Generic;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {
namespace Generic
      {
      namespace Internal
      {
        public ref class EnumInfo : public IGFSerializable
        {
          private:
            String^ _enumClassName;
            String^ _enumName;
            Int32   _hashcode;
          public:
           
           EnumInfo()
           {
             _hashcode = -1;
           }

           EnumInfo(String^  enumClassName, String^  enumName, int hashcode)
           {
              _enumClassName = enumClassName;
              _enumName = enumName;
              _hashcode = hashcode;
           }
           
           static IGFSerializable^ CreateDeserializable()
           {
             return gcnew EnumInfo();
           }
           virtual void ToData( DataOutput^ output );
           virtual IGFSerializable^ FromData( DataInput^ input );
           virtual property uint32_t ObjectSize
           {
             uint32_t get( ){return 0;}
           }
           virtual property uint32_t ClassId
           {
             uint32_t get( ){return GemFireClassIds::EnumInfo;}
           }
           virtual String^ ToString( ) override
           {
            return "EnumInfo";
           }
          
          virtual int GetHashCode()override 
          {
            if(_hashcode != -1)
              return _hashcode;
           
            return ((_enumClassName != nullptr?_enumClassName->GetHashCode():0) 
                    + (_enumName != nullptr?_enumName->GetHashCode():0)  );
          }

          virtual  bool Equals(Object^ obj)override
          {
            if(obj != nullptr)
            {
              EnumInfo^ other = dynamic_cast<EnumInfo^>(obj);
              if(other != nullptr)
              {
                return _enumClassName == other->_enumClassName
                         && _enumName == other->_enumName
                          && _hashcode == other->_hashcode;
              }
              return false;
            }
            return false;
          }

           Object^ GetEnum();
          
          };
			}
      }
    }
  }
}