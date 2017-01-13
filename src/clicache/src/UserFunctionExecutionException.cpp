/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*=========================================================================
*/

#include "UserFunctionExecutionException.hpp"
#include "CacheableString.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Generic
      {
        // IGFSerializable methods

        void UserFunctionExecutionException::ToData( DataOutput^ output )
        {
          throw gcnew IllegalStateException("UserFunctionExecutionException::ToData is not intended for use.");
        }

        IGFSerializable^ UserFunctionExecutionException::FromData( DataInput^ input )
        {
          throw gcnew IllegalStateException("UserFunctionExecutionException::FromData is not intended for use.");
          return this;
        } 

        uint32_t UserFunctionExecutionException::ObjectSize::get( )
        {        
          _GF_MG_EXCEPTION_TRY2
            throw gcnew IllegalStateException("UserFunctionExecutionException::ObjectSize is not intended for use.");
            return NativePtr->objectSize( );

          _GF_MG_EXCEPTION_CATCH_ALL2
        }

       String^ UserFunctionExecutionException::Message::get() 
        {
          _GF_MG_EXCEPTION_TRY2

            gemfire::CacheableStringPtr value = NativePtr->getMessage(  );
            return CacheableString::GetString( value.ptr( ) );          

          _GF_MG_EXCEPTION_CATCH_ALL2
        }

       String^ UserFunctionExecutionException::Name::get() 
        {
          _GF_MG_EXCEPTION_TRY2

            gemfire::CacheableStringPtr value = NativePtr->getName(  );
            return CacheableString::GetString( value.ptr( ) );          

          _GF_MG_EXCEPTION_CATCH_ALL2
        }
      }
    }
  }
}

