/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
