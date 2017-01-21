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

#include "EnumInfo.hpp"
#include "../DataOutput.hpp"
#include "../DataInput.hpp"
using namespace System;

namespace Apache
{
  namespace Geode
  {
    namespace Client
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
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache

  }
}