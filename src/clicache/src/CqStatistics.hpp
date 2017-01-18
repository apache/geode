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
#include <gfcpp/CqStatistics.hpp>
#include "impl/NativeWrapper.hpp"


namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      /// <summary>
      /// Defines common statistical information for a cq.
      /// </summary>
      public ref class CqStatistics sealed
        : public Internal::SBWrap<apache::geode::client::CqStatistics>
      {
      public:

        /// <summary>
        /// get number of inserts qualified by this Cq
        /// </summary>
          uint32_t numInserts( );

        /// <summary>
        /// get number of deletes qualified by this Cq
        /// </summary>
          uint32_t numDeletes( );

        /// <summary>
        /// get number of updates qualified by this Cq
        /// </summary>
          uint32_t numUpdates( );

        /// <summary>
        /// get number of events qualified by this Cq
        /// </summary>
          uint32_t numEvents( );

      internal:

        /// <summary>
        /// Internal factory function to wrap a native object pointer inside
        /// this managed class with null pointer check.
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        /// <returns>
        /// The managed wrapper object; null if the native pointer is null.
        /// </returns>
        inline static CqStatistics^ Create( apache::geode::client::CqStatistics* nativeptr )
        {
          if (nativeptr == nullptr)
          {
            return nullptr;
          }
          return gcnew CqStatistics( nativeptr );
        }


      private:

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline CqStatistics( apache::geode::client::CqStatistics* nativeptr )
          : SBWrap( nativeptr ) { }
      };

    }
  }
}
 } //namespace 
