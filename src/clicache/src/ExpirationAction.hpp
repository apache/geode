/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */



#pragma once

#include "gf_defs.hpp"
#include <gfcpp/ExpirationAction.hpp>


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      /// <summary>
      /// Enumerated type for expiration (LRU) actions.
      /// Contains values for setting an action type.
      /// </summary>
      public enum class ExpirationAction
      {
        /// <summary>
        /// When the region or cached object expires, it is invalidated.
        /// </summary>
        Invalidate = 0,

        /// <summary>
        /// When expired, invalidated locally only.
        /// </summary>
        LocalInvalidate,

        /// <summary>
        /// When the region or cached object expires, it is destroyed.
        /// </summary>
        Destroy,

        /// <summary>
        /// When expired, destroyed locally only.
        /// </summary>
        LocalDestroy,

        /// <summary>Invalid action type.</summary>
        InvalidAction
      };


      /// <summary>
      /// Static class containing convenience methods for <c>ExpirationAction</c>.
      /// </summary>
      public ref class Expiration STATICCLASS
      {
      public:

        /// <summary>
        /// Returns true if this action is distributed invalidate.
        /// </summary>
        /// <returns>true if this an <c>Invalidate</c></returns>
        inline static bool IsInvalidate( ExpirationAction type ) 
        {
          return (type == ExpirationAction::Invalidate);
        }

        /// <summary>
        /// Returns true if this action is local invalidate.
        /// </summary>
        /// <returns>true if this is a <c>LocalInvalidate</c></returns>
        inline static bool IsLocalInvalidate( ExpirationAction type ) 
        {
          return (type == ExpirationAction::LocalInvalidate);
        }

        /// <summary>
        /// Returns true if this action is distributed destroy.
        /// </summary>
        /// <returns>true if this is <c>Destroy</c></returns>
        inline static bool IsDestroy( ExpirationAction type ) 
        {
          return (type == ExpirationAction::Destroy);
        }

        /// <summary>
        /// Returns true if this action is local destroy.
        /// </summary>
        /// <returns>true if this is <c>LocalDestroy</c></returns>
        inline static bool IsLocalDestroy( ExpirationAction type )
        {
          return (type == ExpirationAction::LocalDestroy);
        }

        /// <summary>
        /// Returns true if this action is local.
        /// </summary>
        /// <returns>true if this is <c>LocalInvalidate</c> or
        /// <c>LocalDestroy</c></returns>
        inline static bool IsLocal( ExpirationAction type )
        {
          return (type == ExpirationAction::LocalInvalidate) ||
            (type == ExpirationAction::LocalDestroy);
        }

        /// <summary>
        /// Returns true if this action is distributed.
        /// </summary>
        /// <returns>true if this is an <c>Invalidate</c> or
        /// a <c>Destroy</c></returns>
        inline static bool IsDistributed( ExpirationAction type ) 
        {
          return (type == ExpirationAction::Invalidate) ||
            (type == ExpirationAction::Destroy);
        }
      };

    }
  }
}
 } //namespace 

