/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include <gfcpp/CacheTransactionManager.hpp>
#include <gfcpp/InternalCacheTransactionManager2PC.hpp>
#include "TransactionId.hpp"
//#include "impl/NativeWrapper.hpp"
//#include "impl/TransactionWriter.hpp"
//#include "impl/TransactionListener.hpp"

using namespace System;
namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { 
			namespace Generic
    {
      
      /// <summary>
      /// CacheTransactionManager encapsulates the transactions for a cache
      /// </summary>
      public ref class CacheTransactionManager sealed
        : Internal::SBWrap<gemfire::InternalCacheTransactionManager2PC>
      {
      public:
        /// <summary>
        /// Creates a new transaction and associates it with the current thread.
        /// </summary>
        /// <exception cref="IllegalStateException">
        /// Throws exception if the thread is already associated with a transaction
        /// </exception>
        void Begin();

        /// <summary>
        /// Prepare the first message of two-phase-commit transaction associated
        /// with the current thread.
        /// </summary>
        /// <exception cref="IllegalStateException">
        /// if the thread is not associated with a transaction
        /// </exception>
        /// <exception cref="CommitConflictException">
        /// if the commit operation fails due to a write conflict.
        /// </exception>
        void Prepare();

        /// <summary>
        /// Commit the transaction associated with the current thread. If
        /// the commit operation fails due to a conflict it will destroy
        /// the transaction state and throw a <c>CommitConflictException</c>. 
        /// If the commit operation succeeds,it returns after the transaction 
        /// state has been merged with committed state.  When this method 
        /// completes, the thread is no longer associated with a transaction.
        /// </summary>
        /// <exception cref="IllegalStateException">
        /// if the thread is not associated with a transaction
        /// </exception>
        /// <exception cref="CommitConflictException">
        /// if the commit operation fails due to a write conflict.
        /// </exception>
        void Commit();
        
        /// <summary>
        /// Roll back the transaction associated with the current thread. When
        /// this method completes, the thread is no longer associated with a
        /// transaction and the transaction context is destroyed.
        /// </summary>
        /// <exception cref="IllegalStateException">
        /// if the thread is not associated with a transaction
        /// </exception>  
        void Rollback();
        
        /// <summary>
        /// Reports the existence of a Transaction for this thread
        /// </summary>
        /// <returns>true if a transaction exists, false otherwise</returns>
        bool Exists();

        /// <summary>
        /// Suspends the transaction on the current thread. All subsequent operations
        /// performed by this thread will be non-transactional. The suspended
        /// transaction can be resumed by calling <see cref="TransactionId"/>
        /// <para>
        /// Since 3.6.2
        /// </para>
        /// </summary>
        /// <returns>the transaction identifier of the suspended transaction or null if
        /// the thread was not associated with a transaction</returns>
        GemStone::GemFire::Cache::Generic::TransactionId^ Suspend();

        /// <summary>
        /// On the current thread, resumes a transaction that was previously suspended
        /// using <see cref="suspend"/>
        /// <para>
        /// Since 3.6.2
        /// </para>
        /// </summary>
        /// <param name="transactionId">the transaction to resume</param>
        /// <exception cref="IllegalStateException">if the thread is associated with a transaction or if
        /// would return false for the given transactionId</exception>
        /// <see cref="TransactionId"/> 
        void Resume(GemStone::GemFire::Cache::Generic::TransactionId^ transactionId);

        /// <summary>
        /// This method can be used to determine if a transaction with the given
        /// transaction identifier is currently suspended locally. This method does not
        ///  check other members for transaction status.
        /// <para>
        /// Since 3.6.2
        /// </para>
        /// </summary>
        /// <param name="transactionId"></param>
        /// <returns>true if the transaction is in suspended state, false otherwise</returns>
        /// <see cref="TransactionId"/>
        bool IsSuspended(GemStone::GemFire::Cache::Generic::TransactionId^ transactionId);


        /// <summary>
        /// On the current thread, resumes a transaction that was previously suspended
        /// using <see cref="suspend"/>.
        /// This method is equivalent to
        /// <code>
        /// if (isSuspended(txId)) {
        ///   resume(txId);
        /// }
        /// </code>
        /// except that this action is performed atomically
        /// <para>
        /// Since 3.6.2
        /// </para>
        /// </summary>
        /// <param name="transactionId">the transaction to resume</param>
        /// <returns>true if the transaction was resumed, false otherwise</returns>
        bool TryResume(GemStone::GemFire::Cache::Generic::TransactionId^ transactionId);


        /// <summary>
        /// On the current thread, resumes a transaction that was previously suspended
        /// using <see cref="suspend"/>, or waits for the specified timeout interval if
        /// the transaction has not been suspended. This method will return if:
        /// <para>
        /// Another thread suspends the transaction
        /// </para>
        /// <para>
        /// Another thread calls commit/rollback on the transaction
        /// </para>
        /// <para>
        /// This thread has waited for the specified timeout
        /// </para>
        /// This method returns immediately if <see cref="TransactionId"/> returns false.
        /// <para>
        /// Since 3.6.2
        /// </para>
        /// </summary>
        /// <param name="transactionId">the transaction to resume</param>
        /// <param name="waitTimeInMilliSec">the maximum milliseconds to wait </param>
        /// <returns>true if the transaction was resumed, false otherwise</returns>
        bool TryResume(GemStone::GemFire::Cache::Generic::TransactionId^ transactionId, int32_t waitTimeInMilliSec);



        /// <summary>
        /// Reports the existence of a transaction for the given transactionId. This
        /// method can be used to determine if a transaction with the given transaction
        /// identifier is currently in progress locally.
        /// <para>
        /// Since 3.6.2
        /// </para>
        /// </summary>
        /// <param name="transactionId">the given transaction identifier</param>
        /// <returns>true if the transaction is in progress, false otherwise.</returns>
        /// <see cref="isSuspended"/> 
        bool Exists(GemStone::GemFire::Cache::Generic::TransactionId^ transactionId);


        /// <summary>
        /// Returns the transaction identifier for the current thread
        /// <para>
        /// Since 3.6.2
        /// </para>
        /// </summary>
        /// <returns>the transaction identifier or null if no transaction exists</returns>
        property GemStone::GemFire::Cache::Generic::TransactionId^ TransactionId
        {
        //TODO::split
        GemStone::GemFire::Cache::Generic::TransactionId^ get( );
        }        

#ifdef CSTX_COMMENTED
          
 		    /// <summary>
        /// Returns the current transaction writer
        /// </summary>
        /// <returns>current transaction writer(<c>ITransactionWriter</c>)</returns>
        generic<class TKey, class TValue>
        ITransactionWriter<TKey, TValue>^ GetWriter ();
        
        /// <summary>
        /// Set the <c>ITransactionWriter</c> for the cache
        /// <param name="transactionWriter">transaction writer</param>
        generic<class TKey, class TValue>
        void SetWriter (ITransactionWriter<TKey, TValue>^ transactionWriter);

        /// <summary>
        /// Adds a transaction listener to the end of the list of transaction listeners 
        /// on this cache.
        /// </summary>
        /// <param name="aListener"> 
        /// the user defined transaction listener to add to the cache
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// If the parameter is null.
        /// </exception>
        generic<class TKey, class TValue>
        void AddListener(ITransactionListener<TKey, TValue>^ aListener);
        
        /// <summary>
        /// Removes a transaction listener from the list of transaction listeners on this cache.
        /// Does nothing if the specified listener has not been added.
        /// If the specified listener has been added then the close method of the listener will
        /// be called.
        /// </summary>
        /// <param name="aListener">the transaction listener to remove from the cache.</param>
        /// <exception cref="IllegalArgumentException">
        /// if the parameteris null
        /// </exception>
        generic<class TKey, class TValue>
        void RemoveListener(ITransactionListener<TKey, TValue>^ aListener);
        
#endif

      internal:

        inline static CacheTransactionManager^ Create( gemfire::InternalCacheTransactionManager2PC* nativeptr )
        {
          return ( nativeptr != nullptr ?
            gcnew CacheTransactionManager( nativeptr ) : nullptr );
        }


      private:

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline CacheTransactionManager( gemfire::InternalCacheTransactionManager2PC* nativeptr )
          : SBWrap( nativeptr ) { }
      };

    }
  }
}
 } //namespace 