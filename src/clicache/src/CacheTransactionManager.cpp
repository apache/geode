/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "gf_includes.hpp"
#include "impl/SafeConvert.hpp"
#include "impl/ManagedTransactionListener.hpp"
#include "impl/ManagedTransactionWriter.hpp"
#include "CacheTransactionManager.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache {
			namespace Generic
    {

      void CacheTransactionManager::Begin( )
      {
        _GF_MG_EXCEPTION_TRY2

          NativePtr->begin( );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      void CacheTransactionManager::Prepare( )
      {
        _GF_MG_EXCEPTION_TRY2

          NativePtr->prepare( );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      void CacheTransactionManager::Commit( )
      {
        _GF_MG_EXCEPTION_TRY2
          NativePtr->commit( );
        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      void CacheTransactionManager::Rollback( )
      {
        _GF_MG_EXCEPTION_TRY2
          NativePtr->rollback( );
        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      bool CacheTransactionManager::Exists( )
      {
        _GF_MG_EXCEPTION_TRY2

          return NativePtr->exists( );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      GemStone::GemFire::Cache::Generic::TransactionId^ CacheTransactionManager::Suspend( )
      {
        _GF_MG_EXCEPTION_TRY2
       
          return GemStone::GemFire::Cache::Generic::TransactionId::Create( NativePtr->suspend().ptr() );
       
        _GF_MG_EXCEPTION_CATCH_ALL2
      }
			GemStone::GemFire::Cache::Generic::TransactionId^ CacheTransactionManager::TransactionId::get( )
      {
        _GF_MG_EXCEPTION_TRY2

          return GemStone::GemFire::Cache::Generic::TransactionId::Create( NativePtr->getTransactionId().ptr() );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }
      void CacheTransactionManager::Resume(GemStone::GemFire::Cache::Generic::TransactionId^ transactionId)
      {
        _GF_MG_EXCEPTION_TRY2
        
          return NativePtr->resume( gemfire::TransactionIdPtr(transactionId->NativePtr()));

        _GF_MG_EXCEPTION_CATCH_ALL2
      }
      bool CacheTransactionManager::IsSuspended(GemStone::GemFire::Cache::Generic::TransactionId^ transactionId)
      {
        _GF_MG_EXCEPTION_TRY2

          return NativePtr->isSuspended( gemfire::TransactionIdPtr(transactionId->NativePtr()));

        _GF_MG_EXCEPTION_CATCH_ALL2
      }
      bool CacheTransactionManager::TryResume(GemStone::GemFire::Cache::Generic::TransactionId^ transactionId)
      {
        _GF_MG_EXCEPTION_TRY2

          return NativePtr->tryResume( gemfire::TransactionIdPtr(transactionId->NativePtr()));

        _GF_MG_EXCEPTION_CATCH_ALL2
      }
      bool CacheTransactionManager::TryResume(GemStone::GemFire::Cache::Generic::TransactionId^ transactionId, int32_t waitTimeInMilliSec)
      {
        _GF_MG_EXCEPTION_TRY2

          return NativePtr->tryResume( gemfire::TransactionIdPtr(transactionId->NativePtr()), waitTimeInMilliSec);

        _GF_MG_EXCEPTION_CATCH_ALL2
      }
      bool CacheTransactionManager::Exists(GemStone::GemFire::Cache::Generic::TransactionId^ transactionId)
      {
        _GF_MG_EXCEPTION_TRY2

          return NativePtr->exists( gemfire::TransactionIdPtr(transactionId->NativePtr()));

        _GF_MG_EXCEPTION_CATCH_ALL2
      }

#ifdef CSTX_COMMENTED
      generic<class TKey, class TValue>
      ITransactionWriter<TKey, TValue>^ CacheTransactionManager::GetWriter( )
      {
        _GF_MG_EXCEPTION_TRY2

          // Conver the unmanaged object to  managed generic object 
          gemfire::TransactionWriterPtr& writerPtr( NativePtr->getWriter( ) );
          gemfire::ManagedTransactionWriterGeneric* twg =
          dynamic_cast<gemfire::ManagedTransactionWriterGeneric*>( writerPtr.ptr( ) );

          if (twg != nullptr)
          {
            return (ITransactionWriter<TKey, TValue>^)twg->userptr( );
          }
        
        _GF_MG_EXCEPTION_CATCH_ALL2
        
        return nullptr;
      }
      
      generic<class TKey, class TValue>
      void CacheTransactionManager::SetWriter(ITransactionWriter<TKey, TValue>^ transactionWriter)
      {
        _GF_MG_EXCEPTION_TRY2
          // Create a unmanaged object using the ManagedTransactionWriterGeneric.
          // Set the generic object inside the TransactionWriterGeneric that is a non generic object
          gemfire::TransactionWriterPtr writerPtr;
          if ( transactionWriter != nullptr ) 
          {
            TransactionWriterGeneric<TKey, TValue>^ twg = gcnew TransactionWriterGeneric<TKey, TValue> ();
            twg->SetTransactionWriter(transactionWriter);
            writerPtr = new gemfire::ManagedTransactionWriterGeneric( transactionWriter );
            ((gemfire::ManagedTransactionWriterGeneric*)writerPtr.ptr())->setptr(twg);
          }
          NativePtr->setWriter( writerPtr );
          
        _GF_MG_EXCEPTION_CATCH_ALL2
      }

      generic<class TKey, class TValue>
      void CacheTransactionManager::AddListener(ITransactionListener<TKey, TValue>^ transactionListener)
      {
        _GF_MG_EXCEPTION_TRY2
          // Create a unmanaged object using the ManagedTransactionListenerGeneric.
          // Set the generic object inside the TransactionListenerGeneric that is a non generic object
          gemfire::TransactionListenerPtr listenerPtr;
          if ( transactionListener != nullptr ) 
          {
            TransactionListenerGeneric<TKey, TValue>^ twg = gcnew TransactionListenerGeneric<TKey, TValue> ();
            twg->SetTransactionListener(transactionListener);
            listenerPtr = new gemfire::ManagedTransactionListenerGeneric( transactionListener );
            ((gemfire::ManagedTransactionListenerGeneric*)listenerPtr.ptr())->setptr(twg);
          }
          NativePtr->addListener( listenerPtr );
          
        _GF_MG_EXCEPTION_CATCH_ALL2
      }
        
      generic<class TKey, class TValue>
      void CacheTransactionManager::RemoveListener(ITransactionListener<TKey, TValue>^ transactionListener)
      {
        _GF_MG_EXCEPTION_TRY2
          // Create an unmanaged non generic object using the managed generic object
          // use this to call the remove listener
          gemfire::TransactionListenerPtr listenerPtr;
          if ( transactionListener != nullptr ) 
          {
            TransactionListenerGeneric<TKey, TValue>^ twg = gcnew TransactionListenerGeneric<TKey, TValue> ();
            twg->SetTransactionListener(transactionListener);
            listenerPtr = new gemfire::ManagedTransactionListenerGeneric( transactionListener );
            ((gemfire::ManagedTransactionListenerGeneric*)listenerPtr.ptr())->setptr(twg);
          }
          NativePtr->removeListener( listenerPtr );

        _GF_MG_EXCEPTION_CATCH_ALL2
      }
#endif
    }
  }
}
 } //namespace 
