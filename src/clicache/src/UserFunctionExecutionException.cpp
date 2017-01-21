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


#include "UserFunctionExecutionException.hpp"
#include "CacheableString.hpp"

using namespace System;

namespace Apache
{
  namespace Geode
  {
    namespace Client
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

            apache::geode::client::CacheableStringPtr value = NativePtr->getMessage(  );
            return CacheableString::GetString( value.ptr( ) );          

          _GF_MG_EXCEPTION_CATCH_ALL2
        }

       String^ UserFunctionExecutionException::Name::get() 
        {
          _GF_MG_EXCEPTION_TRY2

            apache::geode::client::CacheableStringPtr value = NativePtr->getName(  );
            return CacheableString::GetString( value.ptr( ) );          

          _GF_MG_EXCEPTION_CATCH_ALL2
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache

}

