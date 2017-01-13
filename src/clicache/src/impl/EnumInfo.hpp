/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once
#include "../IGFSerializable.hpp"
#include "../GemFireClassIds.hpp"
using namespace System;
using namespace System::Collections::Generic;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
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