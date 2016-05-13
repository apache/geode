/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "../../gf_defs.hpp"
#include <cppcache/SelectResults.hpp>
//#include "impl/NativeWrapperN.hpp"
#include "ISelectResultsN.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      generic<class TResult>
      ref class SelectResultsIterator;

      /// <summary>
      /// Interface to encapsulate a select query result set.
      /// </summary>
      generic<class TResult>
      public interface class ICqResults
        : public ISelectResults<TResult>
      {
      };

    }
  }
}
 } //namespace 
