/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifdef CSTX_COMMENTED
#pragma once

#include "gf_defs.hpp"
#include "TransactionEvent.hpp"


namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      /// <summary>
      /// An application plug-in that can be installed for getting events for 
      /// cache transactions. 
      /// </summary>
      /// <seealso cref="CacheTransactionManager.AddListener" />
      generic<class TKey, class TValue>
      public interface class ITransactionListener
      {
      public:
        
        /// <summary>
        /// Called after a successful commit of a transaction.
        /// </summary>
        /// <param name="te">the transaction event</param>
        /// <seealso cref="CacheTransactionManager.Commit" />
        void AfterCommit(TransactionEvent<TKey, TValue>^ te);

	      /// <summary>
        /// Called after an unsuccessful commit operation.
        /// </summary>
        /// <param name="te">the transaction event</param>
        /// <seealso cref="CacheTransactionManager.Commit" />
        void AfterFailedCommit(TransactionEvent<TKey, TValue>^ te);

	      /// <summary>
        /// Called after an explicit rollback of a transaction.
        /// </summary>
        /// <param name="te">the transaction event</param>
        /// <seealso cref="CacheTransactionManager.Commit" />
        /// <seealso cref="CacheTransactionManager.Rollback" />
        void AfterRollback(TransactionEvent<TKey, TValue>^ te);
    
        /// <summary>
        /// alled when the region containing this callback is closed or destroyed, when
        /// the cache is closed, or when a callback is removed from a region
        /// using an <code>AttributesMutator</code>.
	      /// Implementations should cleanup any external
	      /// resources such as database connections. Any runtime exceptions this method
	      /// throws will be logged.
	      /// It is possible for this method to be called multiple times on a single
	      /// callback instance, so implementations must be tolerant of this.
	      /// </summary>
        /// <seealso cref="Cache.Close" />
        /// <seealso cref="Region.Close" />
        /// <seealso cref="Region.LocalDestroyRegion" />
        /// <seealso cref="Region.DestroyRegion" />
        void Close();
      };

    }
  }
}
 } //namespace 
#endif