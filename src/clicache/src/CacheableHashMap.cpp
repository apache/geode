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
#include "CacheableHashMap.hpp"
#include "DataOutput.hpp"
#include "DataInput.hpp"
#include "impl/SafeConvert.hpp"

using namespace System;
using namespace System::Collections::Generic;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      // Region: IGFSerializable Members

      void Generic::CacheableHashMap::ToData(DataOutput^ output)
      {
        output->WriteDictionary((System::Collections::IDictionary^)m_dictionary);        
      }

      IGFSerializable^ Generic::CacheableHashMap::FromData(DataInput^ input)
      {
        m_dictionary = input->ReadDictionary();
        return this;
      }

      uint32_t Generic::CacheableHashMap::ObjectSize::get()
      {
        return ((System::Collections::IDictionary^)m_dictionary)->Count;
      }
      // End Region: IGFSerializable Members
    }
  }
}
 } //namespace 
