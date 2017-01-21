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


namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      generic<class TKey, class TResult>
      void CqAttributesMutator<TKey, TResult>::AddCqListener( Client::ICqListener<TKey, TResult>^ cqListener )
      {
        apache::geode::client::CqListenerPtr listenerptr;
        if ( cqListener != nullptr ) {
          ICqStatusListener<TKey, TResult>^ cqStatusListener = 
            dynamic_cast<ICqStatusListener<TKey, TResult>^>(cqListener);
          if (cqStatusListener != nullptr) {
            CqStatusListenerGeneric<TKey, TResult>^ sLstr = gcnew CqStatusListenerGeneric<TKey, TResult>();
            sLstr->AddCqListener(cqListener);
            listenerptr = new apache::geode::client::ManagedCqStatusListenerGeneric(cqListener);
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
            ((apache::geode::client::ManagedCqStatusListenerGeneric*)listenerptr.ptr())->setptr(sLstr);
          }
          else {
            //TODO::split
            CqListenerGeneric<TKey, TResult>^ cqlg = gcnew CqListenerGeneric<TKey, TResult>();
            cqlg->AddCqListener(cqListener);
            //listenerptr = new apache::geode::client::ManagedCqListenerGeneric((ICqListener<Object^, Object^>^)cqListener );
            listenerptr = new apache::geode::client::ManagedCqListenerGeneric( /*clg,*/ cqListener );
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
            ((apache::geode::client::ManagedCqListenerGeneric*)listenerptr.ptr())->setptr(cqlg);            
          }
        }
        NativePtr->addCqListener( listenerptr );
      }

      generic<class TKey, class TResult>
      void CqAttributesMutator<TKey, TResult>::RemoveCqListener( Client::ICqListener<TKey, TResult>^ cqListener )
      {
        Client::ICqStatusListener<TKey, TResult>^ lister = dynamic_cast<Client::ICqStatusListener<TKey, TResult>^>(cqListener);
        if (lister != nullptr) {
          CqStatusListenerGeneric<TKey, TResult>^ cqlg = gcnew CqStatusListenerGeneric<TKey, TResult>();
          cqlg->AddCqListener(cqListener);
          apache::geode::client::CqStatusListenerPtr lptr(new apache::geode::client::ManagedCqStatusListenerGeneric(
          (Client::ICqStatusListener<TKey, TResult>^) lister ));
          ((apache::geode::client::ManagedCqStatusListenerGeneric*)lptr.ptr())->setptr(cqlg);
          try {
            IntPtr value;
            CqListenerHelper<TKey, TResult>::g_readerWriterLock->AcquireWriterLock(-1);
            if ( CqListenerHelper<TKey, TResult>::m_ManagedVsUnManagedCqLstrDict->TryGetValue(cqListener, value) ) {
              apache::geode::client::CqStatusListenerPtr lptr((apache::geode::client::CqStatusListener*)value.ToPointer());
              NativePtr->removeCqListener(lptr);
            }
          } finally {
              CqListenerHelper<TKey, TResult>::g_readerWriterLock->ReleaseWriterLock();
          }
        }
        else {
          CqListenerGeneric<TKey, TResult>^ cqlg = gcnew CqListenerGeneric<TKey, TResult>();
          cqlg->AddCqListener(cqListener);
          apache::geode::client::CqListenerPtr lptr(new apache::geode::client::ManagedCqListenerGeneric(
            (Client::ICqListener<TKey, TResult>^) cqListener ));
          ((apache::geode::client::ManagedCqListenerGeneric*)lptr.ptr())->setptr(cqlg);
          try {
            IntPtr value;
            CqListenerHelper<TKey, TResult>::g_readerWriterLock->AcquireWriterLock(-1);
            if ( CqListenerHelper<TKey, TResult>::m_ManagedVsUnManagedCqLstrDict->TryGetValue(cqListener, value) ) {
              apache::geode::client::CqListenerPtr lptr((apache::geode::client::CqListener*)value.ToPointer());
              NativePtr->removeCqListener(lptr);
            } 
          } finally {
              CqListenerHelper<TKey, TResult>::g_readerWriterLock->ReleaseWriterLock();            
          }
        }
      }

      generic<class TKey, class TResult>
      void CqAttributesMutator<TKey, TResult>::SetCqListeners(array<Client::ICqListener<TKey, TResult>^>^ newListeners)
      {
        apache::geode::client::VectorOfCqListener vrr;
        for( int i = 0; i < newListeners->Length; i++ )
        {
          Client::ICqStatusListener<TKey, TResult>^ lister = dynamic_cast<Client::ICqStatusListener<TKey, TResult>^>(newListeners[i]);
          if (lister != nullptr) {
            apache::geode::client::CqStatusListenerPtr cptr(new apache::geode::client::ManagedCqStatusListenerGeneric(
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
            ((apache::geode::client::ManagedCqStatusListenerGeneric*)vrr[i].ptr())->setptr(cqlg);
          }
          else {
            Client::ICqListener<TKey, TResult>^ lister = newListeners[i];
            apache::geode::client::CqListenerPtr cptr(new apache::geode::client::ManagedCqListenerGeneric(
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
            ((apache::geode::client::ManagedCqListenerGeneric*)vrr[i].ptr())->setptr(cqlg);
          }
        }

        NativePtr->setCqListeners( vrr );
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache

} //namespace 
