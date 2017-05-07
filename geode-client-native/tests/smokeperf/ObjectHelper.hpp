/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef TEST_QUERYHELPER_HPP
#define TEST_QUERYHELPER_HPP

#include "fwklib/FrameworkTest.hpp"
#include "fwklib/FwkObjects.hpp"
#include "fwklib/GsRandom.hpp"
#include "fwklib/FwkLog.hpp"
#include "fwklib/PerfFwk.hpp"
#include "fwklib/FwkExport.hpp"
#include "testobject/PSTObject.hpp"
#include "testobject/ArrayOfByte.hpp"
#include "testobject/FastAssetAccount.hpp"
#include "testobject/BatchObject.hpp"
#include "testobject/DeltaFastAssetAccount.hpp"
#include "testobject/DeltaPSTObject.hpp"
#include "testobject/EqStruct.hpp"

using namespace gemfire::testframework;
using namespace testobject;

namespace gemfire {
  namespace testframework {
class ObjectHelper
{
  public:
    static CacheablePtr createObject(std::string objectname, uint32_t size, bool encodeKey,bool encodeTimestamp,
        int32_t assetSize = 0,int32_t maxVal = 0 ,int32_t idx = 0) {
      if (objectname == "PSTObject")
      {
        return CacheablePtr(new PSTObject(size,encodeKey,encodeTimestamp));
      }
      else if (objectname == "ArrayOfByte")
      {
        return  CacheablePtr(ArrayOfByte::init(size,encodeKey,encodeTimestamp));
      }
      else if( objectname == "FastAssetAccount")
      {
        return CacheablePtr(new FastAssetAccount(idx,encodeTimestamp,maxVal,assetSize));
      }
      else if( objectname == "BatchObject")
      {
        return CacheablePtr(new BatchObject(idx,assetSize,size));
      }
      else if (objectname == "DeltaFastAssetAccount")
      {
        return CacheablePtr(new DeltaFastAssetAccount(idx, encodeTimestamp,maxVal,assetSize,encodeKey));
      }
      else if (objectname == "DeltaPSTObject")
      {
        return CacheablePtr(new DeltaPSTObject(size,encodeKey,encodeTimestamp));
      }
      else if (objectname == "EqStruct")
      {
         return CacheablePtr(new EqStruct(idx));
      }
      /*else if(objectname == "FlatObject")
      {
      }
      else if( objectname == "SizedString")
      {
        return;
      }
      else if( objectname == "BatchString")
      {
        return;
      }
      else if( objectname == "TestInteger")
      {
        return;
      }*/
      else {
          char * buf = new char[size];
          memset( buf, 'V', size );
          int32_t rsiz = ( size <= 10 ) ? size : 10;
          GsRandom::getAlphanumericString( rsiz, buf );
          CacheableBytesPtr m_CValue = CacheableBytes::create( ( const unsigned char * )buf, size );
          delete [] buf;
          return CacheablePtr(m_CValue);
      }
    }
};

  }
}

#endif // TEST_QUERYHELPER_HPP
