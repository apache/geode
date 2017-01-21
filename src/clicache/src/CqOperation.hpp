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

#include "gf_defs.hpp"
#include <gfcpp/CqOperation.hpp>


using namespace System;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      /// <summary>
      /// Enumerated type for CqOperationType
      /// </summary>
      public enum class CqOperationType
      {
	OP_TYPE_INVALID = -1,
        OP_TYPE_CREATE = 0,
        OP_TYPE_UPDATE = 2,
        OP_TYPE_INVALIDATE = 4,
        OP_TYPE_REGION_CLEAR = 8,
        OP_TYPE_DESTROY = 16,
        OP_TYPE_MARKER = 32
      };
	public ref class CqOperation sealed
        : public Internal::UMWrap<apache::geode::client::CqOperation>
      {
      public:

      /// <summary>
      /// conenience function for convertin from c++ 
      /// apache::geode::client::CqOperation::CqOperationType to
      /// CqOperationType here.
      /// </summary>
	  inline static CqOperationType ConvertFromNative(apache::geode::client::CqOperation::CqOperationType tp)
	  {
		  if(tp==apache::geode::client::CqOperation::OP_TYPE_CREATE)
			  return CqOperationType::OP_TYPE_CREATE;
  		  if(tp==apache::geode::client::CqOperation::OP_TYPE_UPDATE)
			  return CqOperationType::OP_TYPE_UPDATE;
		  if(tp==apache::geode::client::CqOperation::OP_TYPE_INVALIDATE)
			  return CqOperationType::OP_TYPE_INVALIDATE;
		  if(tp==apache::geode::client::CqOperation::OP_TYPE_REGION_CLEAR)
			  return CqOperationType::OP_TYPE_REGION_CLEAR;
  		  if(tp==apache::geode::client::CqOperation::OP_TYPE_DESTROY)
			  return CqOperationType::OP_TYPE_DESTROY;
  		  if(tp==apache::geode::client::CqOperation::OP_TYPE_MARKER)
			  return CqOperationType::OP_TYPE_MARKER;
		  return CqOperationType::OP_TYPE_INVALID;
	  }
	        internal:

        /// <summary>
        /// Internal constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline CqOperation( apache::geode::client::CqOperation* nativeptr )
		            : UMWrap( nativeptr, false ) { }
	  };
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache


