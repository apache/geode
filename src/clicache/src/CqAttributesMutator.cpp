/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*=========================================================================
*/

//#include "gf_includes.hpp"
#include "CqAttributesMutator.hpp"
#include "impl/ManagedCqListener.hpp"
#include "impl/ManagedCqStatusListener.hpp"

using namespace System;


namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      generic<class TKey, class TResult>
      void CqAttributesMutator<TKey, TResult>::AddCqListener( Generic::ICqListener<TKey, TResult>^ cqListener )
      {
        gemfire::CqListenerPtr listenerptr;
        if ( cqListener != nullptr ) {
          ICqStatusListener<TKey, TResult>^ cqStatusListener = 
            dynamic_cast<ICqStatusListener<TKey, TResult>^>(cqListener);
          if (cqStatusListener != nullptr) {
            CqStatusListenerGeneric<TKey, TResult>^ sLstr = gcnew CqStatusListenerGeneric<TKey, TResult>();
            sLstr->AddCqListener(cqListener);
            listenerptr = new gemfire::ManagedCqStatusListenerGeneric(cqListener);
            try {
              CqListenerHelper<TKey, TResult>::g_readerWriterLock->AcquireWriterLock(-1);
              if ( CqListenerHelper<TKey, TResult>::m_ManagedVsUnManagedCqLstrDict->ContainsKey(cqListener) ) {
                CqListenerHelper<TKey, TResult>::m_ManagedVsUnManagedCqLstrDict[cqListener] = (IntPtr)listenerptr.ptr();
              }
              else {
                CqListenerHelper<TKey, TResult>::m_ManagedVsUnManagedCqLstrDict->Add(cqListener, (IntPtr)listenerptr.ptr());
              }
            }
            finally {
              CqListenerHelper<TKey, TResult>::g_readerWriterLock->ReleaseWriterLock();
            }
            ((gemfire::ManagedCqStatusListenerGeneric*)listenerptr.ptr())->setptr(sLstr);
          }
          else {
            //TODO::split
            CqListenerGeneric<TKey, TResult>^ cqlg = gcnew CqListenerGeneric<TKey, TResult>();
            cqlg->AddCqListener(cqListener);
            //listenerptr = new gemfire::ManagedCqListenerGeneric((ICqListener<Object^, Object^>^)cqListener );
            listenerptr = new gemfire::ManagedCqListenerGeneric( /*clg,*/ cqListener );
            try {
              CqListenerHelper<TKey, TResult>::g_readerWriterLock->AcquireWriterLock(-1);
              if ( CqListenerHelper<TKey, TResult>::m_ManagedVsUnManagedCqLstrDict->ContainsKey(cqListener) ) {
                CqListenerHelper<TKey, TResult>::m_ManagedVsUnManagedCqLstrDict[cqListener] = (IntPtr)listenerptr.ptr(); 
              }
              else {
                CqListenerHelper<TKey, TResult>::m_ManagedVsUnManagedCqLstrDict->Add(cqListener, (IntPtr)listenerptr.ptr());
              }
            } finally {
                CqListenerHelper<TKey, TResult>::g_readerWriterLock->ReleaseWriterLock();
            }
            ((gemfire::ManagedCqListenerGeneric*)listenerptr.ptr())->setptr(cqlg);            
          }
        }
        NativePtr->addCqListener( listenerptr );
      }

      generic<class TKey, class TResult>
      void CqAttributesMutator<TKey, TResult>::RemoveCqListener( Generic::ICqListener<TKey, TResult>^ cqListener )
      {
        Generic::ICqStatusListener<TKey, TResult>^ lister = dynamic_cast<Generic::ICqStatusListener<TKey, TResult>^>(cqListener);
        if (lister != nullptr) {
          CqStatusListenerGeneric<TKey, TResult>^ cqlg = gcnew CqStatusListenerGeneric<TKey, TResult>();
          cqlg->AddCqListener(cqListener);
          gemfire::CqStatusListenerPtr lptr(new gemfire::ManagedCqStatusListenerGeneric(
          (Generic::ICqStatusListener<TKey, TResult>^) lister ));
          ((gemfire::ManagedCqStatusListenerGeneric*)lptr.ptr())->setptr(cqlg);
          try {
            IntPtr value;
            CqListenerHelper<TKey, TResult>::g_readerWriterLock->AcquireWriterLock(-1);
            if ( CqListenerHelper<TKey, TResult>::m_ManagedVsUnManagedCqLstrDict->TryGetValue(cqListener, value) ) {
              gemfire::CqStatusListenerPtr lptr((gemfire::CqStatusListener*)value.ToPointer());
              NativePtr->removeCqListener(lptr);
            }
          } finally {
              CqListenerHelper<TKey, TResult>::g_readerWriterLock->ReleaseWriterLock();
          }
        }
        else {
          CqListenerGeneric<TKey, TResult>^ cqlg = gcnew CqListenerGeneric<TKey, TResult>();
          cqlg->AddCqListener(cqListener);
          gemfire::CqListenerPtr lptr(new gemfire::ManagedCqListenerGeneric(
            (Generic::ICqListener<TKey, TResult>^) cqListener ));
          ((gemfire::ManagedCqListenerGeneric*)lptr.ptr())->setptr(cqlg);
          try {
            IntPtr value;
            CqListenerHelper<TKey, TResult>::g_readerWriterLock->AcquireWriterLock(-1);
            if ( CqListenerHelper<TKey, TResult>::m_ManagedVsUnManagedCqLstrDict->TryGetValue(cqListener, value) ) {
              gemfire::CqListenerPtr lptr((gemfire::CqListener*)value.ToPointer());
              NativePtr->removeCqListener(lptr);
            } 
          } finally {
              CqListenerHelper<TKey, TResult>::g_readerWriterLock->ReleaseWriterLock();            
          }
        }
      }

      generic<class TKey, class TResult>
      void CqAttributesMutator<TKey, TResult>::SetCqListeners(array<Generic::ICqListener<TKey, TResult>^>^ newListeners)
      {
        gemfire::VectorOfCqListener vrr;
        for( int i = 0; i < newListeners->Length; i++ )
        {
          Generic::ICqStatusListener<TKey, TResult>^ lister = dynamic_cast<Generic::ICqStatusListener<TKey, TResult>^>(newListeners[i]);
          if (lister != nullptr) {
            gemfire::CqStatusListenerPtr cptr(new gemfire::ManagedCqStatusListenerGeneric(
              (ICqStatusListener<TKey, TResult>^)lister ));
            vrr.push_back(cptr);
            CqStatusListenerGeneric<TKey, TResult>^ cqlg = gcnew CqStatusListenerGeneric<TKey, TResult>();
            cqlg->AddCqListener(newListeners[i]);
            try {
              CqListenerHelper<TKey, TResult>::g_readerWriterLock->AcquireWriterLock(-1);
              if ( CqListenerHelper<TKey, TResult>::m_ManagedVsUnManagedCqLstrDict->ContainsKey( newListeners[i]) ) {
                CqListenerHelper<TKey, TResult>::m_ManagedVsUnManagedCqLstrDict[newListeners[i]] = (IntPtr)cptr.ptr();
              }
              else {
                CqListenerHelper<TKey, TResult>::m_ManagedVsUnManagedCqLstrDict->Add(newListeners[i], (IntPtr)cptr.ptr());
              }
            } finally {
                CqListenerHelper<TKey, TResult>::g_readerWriterLock->ReleaseWriterLock();
            }
            ((gemfire::ManagedCqStatusListenerGeneric*)vrr[i].ptr())->setptr(cqlg);
          }
          else {
            Generic::ICqListener<TKey, TResult>^ lister = newListeners[i];
            gemfire::CqListenerPtr cptr(new gemfire::ManagedCqListenerGeneric(
              (ICqListener<TKey, TResult>^)lister ));
            vrr.push_back(cptr);
            CqListenerGeneric<TKey, TResult>^ cqlg = gcnew CqListenerGeneric<TKey, TResult>();
            cqlg->AddCqListener(newListeners[i]);
            try {
              CqListenerHelper<TKey, TResult>::g_readerWriterLock->AcquireWriterLock(-1);
              if ( CqListenerHelper<TKey, TResult>::m_ManagedVsUnManagedCqLstrDict->ContainsKey( newListeners[i]) ) {
                CqListenerHelper<TKey, TResult>::m_ManagedVsUnManagedCqLstrDict[newListeners[i]] = (IntPtr)cptr.ptr();
              }
              else {
                CqListenerHelper<TKey, TResult>::m_ManagedVsUnManagedCqLstrDict->Add(newListeners[i], (IntPtr)cptr.ptr());
              }
            } finally {
                CqListenerHelper<TKey, TResult>::g_readerWriterLock->ReleaseWriterLock();
            }
            ((gemfire::ManagedCqListenerGeneric*)vrr[i].ptr())->setptr(cqlg);
          }
        }

        NativePtr->setCqListeners( vrr );
      }

    }
    }
  }
} //namespace 
