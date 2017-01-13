/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include "CqState.hpp"
#include <gfcpp/CqQuery.hpp>
//#include "impl/NativeWrapper.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      generic<class TResult>
      interface class ICqResults;

      generic<class TKey, class TResult>
      ref class CqAttributes;

      ref class CqStatistics;

      generic<class TKey, class TResult>
      ref class CqAttributesMutator;

      generic<class TResult>
      ref class Query;

      /// <summary>
      /// Class to encapsulate a continuous query (CQ).
      /// </summary>
      /// <remarks>
      /// A CqQuery is obtained from a QueryService which in turn is obtained
      /// from the Cache.
      /// This can be executed to return SelectResults which can be either
      /// a ResultSet or a StructSet, or it can be just registered on the
      /// java server without returning results immediately rather only
      /// the incremental results.
      ///
      /// This class is intentionally not thread-safe. So multiple threads
      /// should not operate on the same <c>CqQuery</c> object concurrently
      /// rather should have their own <c>CqQuery</c> objects.
      /// </remarks>
      generic<class TKey, class TResult>
      public ref class CqQuery sealed
        : public Internal::SBWrap<gemfire::CqQuery>
      {
      public:

        /// <summary>
        /// Executes the Cq  Query on the cache server
        /// </summary>
        void Execute( );

        /// <summary>
        /// Executes the Cq Query on the cache server
        /// and returns the Cqresults.
        /// </summary>
        ICqResults<TResult>^ ExecuteWithInitialResults();

        /// <summary>
        /// Executes the Cq Query on the cache server
        /// with the specified timeout and returns the results.
        /// </summary>
        /// <param name="timeout">The time (in seconds) to wait for query response.
        /// This should be less than or equal to 2^31/1000 i.e. 2147483.
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// if timeout parameter is greater than 2^31/1000.
        /// </exception>
        ICqResults<TResult>^ ExecuteWithInitialResults(uint32_t timeout);

        /// <summary>
        /// Get the string for this cq query.
        /// </summary>
        property String^ QueryString
        {
          String^ get( );
        }

        /// <summary>
        /// Get the name for this cq query.
        /// </summary>
        property String^ Name
        {
          String^ get( );
        }

        /// <summary>
        /// Get the Attributes for this cq query.
        /// </summary>
        CqAttributes<TKey, TResult>^ GetCqAttributes();

        /// <summary>
        /// Get the Attributes Mutator for this cq query.
        /// </summary>
        CqAttributesMutator<TKey, TResult>^ GetCqAttributesMutator();

        /// <summary>
        /// Get the stats for this cq query.
        /// </summary>
        CqStatistics^ GetStatistics();

        /// <summary>
        /// Get the Query for this cq query.
        /// </summary>
        Query<TResult>^ GetQuery();

        /// <summary>
        /// stop the cq query
        /// </summary>
        void Stop( );

        /// <summary>
        /// stop the cq query
        /// </summary>
        void Close( );

        /// <summary>
        /// get the state of this cq query
        /// </summary>
        CqStateType GetState();

        /// <summary>
        /// Is this Cq in running state?
        /// </summary>
        bool IsRunning();

        /// <summary>
        /// Is this Cq in stopped state?
        /// </summary>
        bool IsStopped();

        /// <summary>
        /// Is this Cq in closed state?
        /// </summary>
        bool IsClosed();

      internal:

        /// <summary>
        /// Internal factory function to wrap a native object pointer inside
        /// this managed class with null pointer check.
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        /// <returns>
        /// The managed wrapper object; null if the native pointer is null.
        /// </returns>
        inline static CqQuery<TKey, TResult>^ Create( gemfire::CqQuery* nativeptr )
        {
          if (nativeptr == nullptr)
          {
            return nullptr;
          }
          return gcnew CqQuery<TKey, TResult>( nativeptr );
        }


      private:

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline CqQuery( gemfire::CqQuery* nativeptr )
          : SBWrap( nativeptr ) { }
      };

    }
  }
}
 } //namespace 
