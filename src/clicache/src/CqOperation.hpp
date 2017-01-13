/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */



#pragma once

#include "gf_defs.hpp"
#include <gfcpp/CqOperation.hpp>


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
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
        : public Internal::UMWrap<gemfire::CqOperation>
      {
      public:

      /// <summary>
      /// conenience function for convertin from c++ 
      /// gemfire::CqOperation::CqOperationType to
      /// CqOperationType here.
      /// </summary>
	  inline static CqOperationType ConvertFromNative(gemfire::CqOperation::CqOperationType tp)
	  {
		  if(tp==gemfire::CqOperation::OP_TYPE_CREATE)
			  return CqOperationType::OP_TYPE_CREATE;
  		  if(tp==gemfire::CqOperation::OP_TYPE_UPDATE)
			  return CqOperationType::OP_TYPE_UPDATE;
		  if(tp==gemfire::CqOperation::OP_TYPE_INVALIDATE)
			  return CqOperationType::OP_TYPE_INVALIDATE;
		  if(tp==gemfire::CqOperation::OP_TYPE_REGION_CLEAR)
			  return CqOperationType::OP_TYPE_REGION_CLEAR;
  		  if(tp==gemfire::CqOperation::OP_TYPE_DESTROY)
			  return CqOperationType::OP_TYPE_DESTROY;
  		  if(tp==gemfire::CqOperation::OP_TYPE_MARKER)
			  return CqOperationType::OP_TYPE_MARKER;
		  return CqOperationType::OP_TYPE_INVALID;
	  }
	        internal:

        /// <summary>
        /// Internal constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline CqOperation( gemfire::CqOperation* nativeptr )
		            : UMWrap( nativeptr, false ) { }
	  };
    }
  }
}
 } //namespace 

