/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
