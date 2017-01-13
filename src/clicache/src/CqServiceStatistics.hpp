/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include <gfcpp/CqServiceStatistics.hpp>
#include "impl/NativeWrapper.hpp"


namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      /// <summary>
      /// Defines common statistical information for cqservice 
      /// </summary>
      public ref class CqServiceStatistics sealed
        : public Internal::SBWrap<gemfire::CqServiceStatistics>
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
        inline static CqServiceStatistics^ Create( gemfire::CqServiceStatistics* nativeptr )
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
        inline CqServiceStatistics( gemfire::CqServiceStatistics* nativeptr )
          : SBWrap( nativeptr ) { }
      };

    }
  }
}
 } //namespace 
