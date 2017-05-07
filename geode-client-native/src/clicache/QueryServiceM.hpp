/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include "cppcache/QueryService.hpp"
#include "impl/NativeWrapper.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      ref class Query;
      ref class CqQuery;
      ref class CqAttributes;
      ref class CqServiceStatistics;

      /// <summary>
      /// Provides a query service.
      /// </summary>
      [Obsolete("Use classes and APIs from the GemStone.GemFire.Cache.Generic namespace")]
      public ref class QueryService sealed
        : public Internal::SBWrap<gemfire::QueryService>
      {
      public:

        /// <summary>
        /// Get a <c>Query</c> object to enable querying.
        /// </summary>
        Query^ NewQuery( String^ query );
        /// @nativeclient
        /// <summary>
        /// Get a <c>CqQuery</c> object to enable continuous querying.
        /// </summary>
        /// @endnativeclient
        CqQuery^ NewCq( String^ query, CqAttributes^ cqAttr, bool isDurable );
        /// @nativeclient
        /// <summary>
        /// Get a <c>CqQuery</c> object to enable continuous querying.
        /// </summary>
        /// @endnativeclient

        CqQuery^ NewCq( String^ name, String^ query, CqAttributes^ cqAttr, bool isDurable );
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
	array<CqQuery^>^ GetCqs();

        /// @nativeclient
        /// <summary>
        /// Get the  <c>CqQuery</c> with the given name on this client.
        /// </summary>
        /// @endnativeclient

	CqQuery^ GetCq(String^ name);

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
        /// Get <c>AllDurableCqsFromServer</c>  on this client.
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
        inline static QueryService^ Create( gemfire::QueryService* nativeptr )
        {
          return ( nativeptr != nullptr ?
            gcnew QueryService( nativeptr ) : nullptr );
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
