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
#include <gfcpp/CqServiceStatistics.hpp>
#include "impl/NativeWrapper.hpp"


namespace Apache
{
  namespace Geode
  {
    namespace Client
    {
namespace Generic
    {

      /// <summary>
      /// Defines common statistical information for cqservice 
      /// </summary>
      public ref class CqServiceStatistics sealed
        : public Internal::SBWrap<apache::geode::client::CqServiceStatistics>
      {
      public:

        /// <summary>
        ///Get the number of CQs currently active. 
        ///Active CQs are those which are executing (in running state).
        /// </summary>
          uint32_t numCqsActive( );

        /// <summary>
        ///Get the total number of CQs created. This is a cumulative number.
        /// </summary>
          uint32_t numCqsCreated( );

        /// <summary>
        ///Get the total number of closed CQs. This is a cumulative number.
        /// </summary>
          uint32_t numCqsClosed( );

        /// <summary>
        ///Get the number of stopped CQs currently.
        /// </summary>
          uint32_t numCqsStopped( );

        /// <summary>
        ///Get number of CQs that are currently active or stopped. 
        ///The CQs included in this number are either running or stopped (suspended).
        ///Closed CQs are not included.
        /// </summary>
          uint32_t numCqsOnClient( );

      internal:

        /// <summary>
        /// Internal factory function to wrap a native object pointer inside
        /// this managed class with null pointer check.
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        /// <returns>
        /// The managed wrapper object; null if the native pointer is null.
        /// </returns>
        inline static CqServiceStatistics^ Create( apache::geode::client::CqServiceStatistics* nativeptr )
        {
          if (nativeptr == nullptr)
          {
            return nullptr;
          }
          return gcnew CqServiceStatistics( nativeptr );
        }


      private:

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline CqServiceStatistics( apache::geode::client::CqServiceStatistics* nativeptr )
          : SBWrap( nativeptr ) { }
      };

    }
  }
}
 } //namespace 
