/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "EnumInfo.hpp"
#include "../DataOutput.hpp"
#include "../DataInput.hpp"
using namespace System;

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
        void EnumInfo::ToData( DataOutput^ output )
        {
					output->WriteString(_enumClassName);
          output->WriteString(_enumName);
          output->WriteInt32(_hashcode);
        }
        
        IGFSerializable^ EnumInfo::FromData( DataInput^ input )
        {
          _enumClassName = input->ReadString();
          _enumName = input->ReadString();
          _hashcode = input->ReadInt32();
					return this;
        }

       Object^ EnumInfo::GetEnum()
       {
         String^ tmp = Serializable::GetLocalTypeName(_enumClassName);
         Type^ t = Serializable::GetType(tmp);
         Object^ obj = Enum::Parse(t, _enumName);

         return obj;
       }        
        

        
        
       }
			}
    }
  }
}