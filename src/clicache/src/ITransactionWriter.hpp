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
    namespace Cache 
		{ 
			namespace Generic
			{
				/// <summary>
				/// A callback that is allowed to veto a transaction. Only one TransactionWriter can exist
				/// per cache, and only one TransactionWriter will be fired in the
				/// entire distributed system for each transaction.
				/// This writer can be used to update a backend data source before the GemFire 
				/// cache is updated during commit. If the backend update fails, the implementer 
				/// can throw a {<c>TransactionWriterException</c>} to veto the transaction.
				/// </summary>
				/// <seealso cref="CacheTransactionManager.SetWriter" />
				generic<class TKey, class TValue>
				public interface class ITransactionWriter
				{
				public:
					/// <summary>
					/// Called before the transaction has finished committing, but after conflict checking.
					/// Provides an opportunity for implementors to cause transaction abort by throwing a
					/// TransactionWriterException
					/// </summary>
					/// <param name="te">the transaction event</param>
					/// <exception cref="TransactionWriterException">
					/// in the event that the transaction should be rolled back
					/// </exception>
					/// <seealso cref="CacheTransactionManager.Commit" />
					void BeforeCommit(TransactionEvent<TKey, TValue>^ te);

      };

    }
  }
}
 } //namespace 
#endif
