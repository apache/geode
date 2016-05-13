/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"

using namespace System;
namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      interface class IGFSerializable;
      ref class ResultCollector;

      /// <summary>
      /// collect function execution results, can be overriden 
      /// </summary>
      [Obsolete("Use classes and APIs from the GemStone.GemFire.Cache.Generic namespace")]
      public interface class IResultCollector
      {
      public:

        /// <summary>
        /// add result from a single function execution
        /// </summary>
        void AddResult( IGFSerializable^ rs );

        /// <summary>
        /// get result 
        /// </summary>
	array<IGFSerializable^>^  GetResult(); 

        /// <summary>
        /// get result 
        /// </summary>
	array<IGFSerializable^>^  GetResult(UInt32 timeout); 

        /// <summary>
        ///Call back provided to caller, which is called after function execution is
	///complete and caller can retrieve results using getResult()
        /// </summary>
	void EndResults(); 

  /// <summary>
  ///GemFire will invoke this method before re-executing function (in case of
  /// Function Execution HA) This is to clear the previous execution results from
   /// the result collector
  /// @since 6.5
  /// </summary>
  void ClearResults();

      };

    }
  }
}
