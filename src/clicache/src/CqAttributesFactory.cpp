/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*=========================================================================
*/

//#include "gf_includes.hpp"
#include "CqAttributesFactory.hpp"
#include "impl/ManagedCqListener.hpp"
#include "ICqListener.hpp"
#include "impl/ManagedCqStatusListener.hpp"
#include "ICqStatusListener.hpp"
#include "CqAttributesMutator.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      generic<class TKey, class TResult>
      void CqAttributesFactory<TKey, TResult>::AddCqListener(Generic::ICqListener<TKey, TResult>^ cqListener )
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
              if ( CqListenerHelper<TKey, TResult>::m_ManagedVsUnManagedCqLstrDict->ContainsKey( cqListener) ) {
                CqListenerHelper<TKey, TResult>::m_ManagedVsUnManagedCqLstrDict[cqListener] = (IntPtr)listenerptr.ptr();
              }
              else {
                CqListenerHelper<TKey, TResult>::m_ManagedVsUnManagedCqLstrDict->Add(cqListener, (IntPtr)listenerptr.ptr());
              }
            } finally {
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
              if ( CqListenerHelper<TKey, TResult>::m_ManagedVsUnManagedCqLstrDict->ContainsKey( cqListener) ) {
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
      void CqAttributesFactory<TKey, TResult>::InitCqListeners(array<Generic::ICqListener<TKey, TResult>^>^ cqListeners)
      {
        gemfire::VectorOfCqListener vrr;
        for( int i = 0; i < cqListeners->Length; i++ )
        {
          ICqStatusListener<TKey, TResult>^ lister = dynamic_cast<ICqStatusListener<TKey, TResult>^>(cqListeners[i]);
          if (lister != nullptr) {
            gemfire::CqStatusListenerPtr cptr(new gemfire::ManagedCqStatusListenerGeneric(
              (ICqStatusListener<TKey, TResult>^)lister ));
            vrr.push_back(cptr);
            CqStatusListenerGeneric<TKey, TResult>^ cqlg = gcnew CqStatusListenerGeneric<TKey, TResult>();
            cqlg->AddCqListener(cqListeners[i]);
            try {
              CqListenerHelper<TKey, TResult>::g_readerWriterLock->AcquireWriterLock(-1);
              if ( CqListenerHelper<TKey, TResult>::m_ManagedVsUnManagedCqLstrDict->ContainsKey( cqListeners[i]) ) {
                CqListenerHelper<TKey, TResult>::m_ManagedVsUnManagedCqLstrDict[cqListeners[i]] = (IntPtr)cptr.ptr();
              }
              else {
                CqListenerHelper<TKey, TResult>::m_ManagedVsUnManagedCqLstrDict->Add(cqListeners[i], (IntPtr)cptr.ptr());
              }
            } finally {
                CqListenerHelper<TKey, TResult>::g_readerWriterLock->ReleaseWriterLock();
            }
            ((gemfire::ManagedCqStatusListenerGeneric*)vrr[i].ptr())->setptr(cqlg);
          }
          else {
            ICqListener<TKey, TResult>^ lister = cqListeners[i];
            gemfire::CqListenerPtr cptr(new gemfire::ManagedCqListenerGeneric(
              (ICqListener<TKey, TResult>^)lister ));
            vrr.push_back(cptr);
            CqListenerGeneric<TKey, TResult>^ cqlg = gcnew CqListenerGeneric<TKey, TResult>();
            cqlg->AddCqListener(cqListeners[i]);
            try {
              CqListenerHelper<TKey, TResult>::g_readerWriterLock->AcquireWriterLock(-1);
              if ( CqListenerHelper<TKey, TResult>::m_ManagedVsUnManagedCqLstrDict->ContainsKey( cqListeners[i]) ) {
                CqListenerHelper<TKey, TResult>::m_ManagedVsUnManagedCqLstrDict[cqListeners[i]] = (IntPtr)cptr.ptr();
              }
              else {
                CqListenerHelper<TKey, TResult>::m_ManagedVsUnManagedCqLstrDict->Add(cqListeners[i], (IntPtr)cptr.ptr());
              }
            } finally {
                CqListenerHelper<TKey, TResult>::g_readerWriterLock->ReleaseWriterLock();
            }
            ((gemfire::ManagedCqListenerGeneric*)vrr[i].ptr())->setptr(cqlg);
          }
        }

        NativePtr->initCqListeners( vrr );
      }

      generic<class TKey, class TResult>
      Generic::CqAttributes<TKey, TResult>^ CqAttributesFactory<TKey, TResult>::Create( )
      {
        return Generic::CqAttributes<TKey, TResult>::Create(NativePtr->create().ptr());
      }

    }
    }
  }
} //namespace 
