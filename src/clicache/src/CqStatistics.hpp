/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
        : public Internal::SBWrap<gemfire::CqStatistics>
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
        inline static CqStatistics^ Create( gemfire::CqStatistics* nativeptr )
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
        inline CqStatistics( gemfire::CqStatistics* nativeptr )
          : SBWrap( nativeptr ) { }
      };

    }
  }
}
 } //namespace 
