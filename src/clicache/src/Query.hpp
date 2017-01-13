/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include <gfcpp/Query.hpp>
#include "impl/NativeWrapper.hpp"

#include "IGFSerializable.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      generic<class TResult>
      interface class ISelectResults;

      /// <summary>
      /// Class to encapsulate a query.
      /// </summary>
      /// <remarks>
      /// A Query is obtained from a QueryService which in turn is obtained
      /// from the Cache.
      /// This can be executed to return SelectResults which can be either
      /// a ResultSet or a StructSet.
      ///
      /// This class is intentionally not thread-safe. So multiple threads
      /// should not operate on the same <c>Query</c> object concurrently
      /// rather should have their own <c>Query</c> objects.
      /// </remarks>
      generic<class TResult>
      public ref class Query sealed
        : public Internal::SBWrap<gemfire::Query>
      {
      public:

        /// <summary>
        /// Executes the OQL Query on the cache server and returns
        /// the results. The default timeout for the query is 15 secs.
        /// </summary>
        /// <exception cref="QueryException">
        /// if some query error occurred at the server.
        /// </exception>
        /// <exception cref="IllegalStateException">
        /// if some other error occurred.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if no java cache server is available.
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <returns>
        /// An <see cref="ISelectResults"/> object which can either be a
        /// <see cref="ResultSet"/> or a <see cref="StructSet"/>.
        /// </returns>
        ISelectResults<TResult>^ Execute( );

        /// <summary>
        /// Executes the OQL Query on the cache server with the specified
        /// timeout and returns the results.
        /// </summary>
        /// <param name="timeout">The time (in seconds) to wait for query response.
        /// This should be less than or equal to 2^31/1000 i.e. 2147483.
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// if timeout parameter is greater than 2^31/1000.
        /// </exception>
        /// <exception cref="QueryException">
        /// if some query error occurred at the server.
        /// </exception>
        /// <exception cref="IllegalStateException">
        /// if some other error occurred.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if no java cache server is available
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <returns>
        /// An <see cref="ISelectResults"/> object which can either be a
        /// <see cref="ResultSet"/> or a <see cref="StructSet"/>.
        /// </returns>
        ISelectResults<TResult>^ Execute( uint32_t timeout );

		/// <summary>
        /// Executes the OQL Parameterized Query on the cache server with the specified
        /// paramList & timeout parameters and returns the results.
        /// </summary>
		/// <param name="paramList">The Parameter List for the specified Query.
        /// </param>
        /// <param name="timeout">The time (in seconds) to wait for query response.
        /// This should be less than or equal to 2^31/1000 i.e. 2147483.
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// if timeout parameter is greater than 2^31/1000.
        /// </exception>
        /// <exception cref="QueryException">
        /// if some query error occurred at the server.
        /// </exception>
        /// <exception cref="IllegalStateException">
        /// if some other error occurred.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if no java cache server is available
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <returns>
        /// An <see cref="ISelectResults"/> object which can either be a
        /// <see cref="ResultSet"/> or a <see cref="StructSet"/>.
        /// </returns>
        ISelectResults<TResult>^ Execute( array<Object^>^ paramList, uint32_t timeout );

        /// <summary>
        /// Executes the OQL Parameterized Query on the cache server with the specified
        /// paramList and returns the results. The default timeout for the query is 15 secs.
        /// </summary>
		/// <param name="paramList">The Parameter List for the specified Query.
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// if timeout parameter is greater than 2^31/1000.
        /// </exception>
        /// <exception cref="QueryException">
        /// if some query error occurred at the server.
        /// </exception>
        /// <exception cref="IllegalStateException">
        /// if some other error occurred.
        /// </exception>
        /// <exception cref="NotConnectedException">
        /// if no java cache server is available
        /// For pools configured with locators, if no locators are available, innerException
        /// of NotConnectedException is set to NoAvailableLocatorsException.
        /// </exception>
        /// <returns>
        /// An <see cref="ISelectResults"/> object which can either be a
        /// <see cref="ResultSet"/> or a <see cref="StructSet"/>.
        /// </returns>
        ISelectResults<TResult>^ Execute( array<Object^>^ paramList);
        /// <summary>
        /// Get the string for this query.
        /// </summary>
        property String^ QueryString
        {
          String^ get( );
        }

        /// <summary>
        /// Compile the given query -- NOT IMPLEMENTED.
        /// </summary>
        void Compile( );

        /// <summary>
        /// Check if the query is compiled -- NOT IMPLEMENTED.
        /// </summary>
        property bool IsCompiled
        {
          bool get( );
        }


      internal:

        /// <summary>
        /// Internal factory function to wrap a native object pointer inside
        /// this managed class with null pointer check.
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        /// <returns>
        /// The managed wrapper object; null if the native pointer is null.
        /// </returns>
        inline static Query<TResult>^ Create( gemfire::Query* nativeptr )
        {
          return ( nativeptr != nullptr ?
            gcnew Query<TResult>( nativeptr ) : nullptr );
        }


      private:

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline Query( gemfire::Query* nativeptr )
          : SBWrap( nativeptr ) { }
      };

    }
  }
}
 } //namespace 
