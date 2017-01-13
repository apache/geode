/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include "impl/NativeWrapper.hpp"
#include <gfcpp/QueryService.hpp>



using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      generic<class TResult>
      ref class Query;

      generic<class TKey, class TResult>
      ref class CqQuery;

      generic<class TKey, class TResult>
      ref class CqAttributes;

      ref class CqServiceStatistics;

      /// <summary>
      /// Provides a query service.
      /// </summary>
      generic<class TKey, class TResult>
      public ref class QueryService sealed
				: public Internal::SBWrap<gemfire::QueryService>
      {
      public:

        /// <summary>
        /// Get a <c>Query</c> object to enable querying.
        /// </summary>
        //generic<class TResult>
        Query<TResult>^ NewQuery( String^ query );
        /// @nativeclient
        /// <summary>
        /// Get a <c>CqQuery</c> object to enable continuous querying.
        /// </summary>
        /// @endnativeclient
        //generic<class TKey, class TResult>
        CqQuery<TKey, TResult>^ NewCq( String^ query, CqAttributes<TKey, TResult>^ cqAttr, bool isDurable );
        /// @nativeclient
        /// <summary>
        /// Get a <c>CqQuery</c> object to enable continuous querying.
        /// </summary>
        /// @endnativeclient
        //generic<class TKey, class TResult>
        CqQuery<TKey, TResult>^ NewCq( String^ name, String^ query, CqAttributes<TKey, TResult>^ cqAttr, bool isDurable );
        /// @nativeclient
        /// <summary>
        /// Close all  <c>CqQuery</c> on this client.
        /// </summary>
        /// @endnativeclient
	void CloseCqs();

        /// @nativeclient
        /// <summary>
        /// Get all  <c>CqQuery</c> on this client.
        /// </summary>
        /// @endnativeclient
  //generic<class TKey, class TResult>
	array<CqQuery<TKey, TResult>^>^ GetCqs();

        /// @nativeclient
        /// <summary>
        /// Get the  <c>CqQuery</c> with the given name on this client.
        /// </summary>
        /// @endnativeclient
  //generic<class TKey, class TResult>
	CqQuery<TKey, TResult>^ GetCq(String^ name);

        /// @nativeclient
        /// <summary>
        /// Get the  <c>CqQuery</c> with the given name on this client.
        /// </summary>
        /// @endnativeclient
	void ExecuteCqs();

        /// @nativeclient
        /// <summary>
        /// Stop all  <c>CqQuery</c>  on this client.
        /// </summary>
        /// @endnativeclient
	void StopCqs();

        /// @nativeclient
        /// <summary>
        /// Get <c>CqServiceStatistics</c>  on this client.
        /// </summary>
        /// @endnativeclient
	CqServiceStatistics^ GetCqStatistics();

        /// @nativeclient
        /// <summary>
        /// Get all durableCq nanes from server for this client.
        /// </summary>
        /// @endnativeclient
  System::Collections::Generic::List<String^>^ GetAllDurableCqsFromServer();

      internal:

        /// <summary>
        /// Internal factory function to wrap a native object pointer inside
        /// this managed class with null pointer check.
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        /// <returns>
        /// The managed wrapper object; null if the native pointer is null.
        /// </returns>
        inline static GemStone::GemFire::Cache::Generic::QueryService<TKey, TResult>^ Create( gemfire::QueryService* nativeptr )
        {
          return ( nativeptr != nullptr ?
            gcnew GemStone::GemFire::Cache::Generic::QueryService<TKey, TResult>( nativeptr ) : nullptr );
        }


      private:

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline QueryService( gemfire::QueryService* nativeptr )
          : SBWrap( nativeptr ) { }
      };

    }
  }
}
 } //namespace 
