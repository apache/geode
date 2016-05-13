/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */



//#include "gf_includesN.hpp"
#include "CacheAttributesFactoryMN.hpp"
#include "CacheAttributesMN.hpp"
#include "../../ExceptionTypesM.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      void CacheAttributesFactory::SetRedundancyLevel( int32_t redundancyLevel )
      {
        NativePtr->setRedundancyLevel( redundancyLevel );
      }

      void CacheAttributesFactory::SetEndpoints( String^ endpoints )
      {
        ManagedString mg_endpoints( endpoints );
        NativePtr->setEndpoints( mg_endpoints.CharPtr );
      }

      CacheAttributes^ CacheAttributesFactory::CreateCacheAttributes( )
      {
        _GF_MG_EXCEPTION_TRY/* due to auto replace */

          gemfire::CacheAttributesPtr& nativeptr =
            NativePtr->createCacheAttributes();

          return CacheAttributes::Create(nativeptr.ptr());

        _GF_MG_EXCEPTION_CATCH_ALL/* due to auto replace */
      }

    }
  }
}
 } //namespace 

