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
#include <gfcpp/ExpirationAction.hpp>


using namespace System;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {
namespace Generic
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

