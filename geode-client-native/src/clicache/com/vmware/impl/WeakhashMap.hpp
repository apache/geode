/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once


#pragma warning(disable:4091)
#include <msclr/lock.h>

using namespace System;
using namespace System::Collections::Generic;
using namespace System::Threading;
//using namespace System::WeakReference;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Generic
      {
      namespace Internal
      {
        public ref class EntryNode
        {
          System::WeakReference^     m_weakValue;
          EntryNode^  m_nextEntryNode;

        public:
          EntryNode(Object^ val, EntryNode^ nextNode)
          {
            m_weakValue = gcnew WeakReference(val);
            m_nextEntryNode = nextNode;
          }

          bool isValEquals(Object^ other)
          {
            if( other == nullptr)
              return false;
            Object^ val = m_weakValue->Target;

            if(val != nullptr)
            {
              if(val->GetHashCode() == other->GetHashCode() && val->Equals(other))
                return true;
            }

            return false;
          }

          property Object^ Value
          {
            Object^ get()
            {
              return m_weakValue->Target;
            }
            void set(Object^ val)
            {
              m_weakValue = gcnew WeakReference(val);
            }
          }

          property EntryNode^ NextNode
          {
            EntryNode^ get()
            {
              return m_nextEntryNode;
            }
          }

        };

        public ref class MapEntry
        {
          EntryNode^ m_entryNode;

        public:
          MapEntry(Object^ newVal)
          {
            m_entryNode = gcnew EntryNode(newVal, nullptr);
          }

          void AddEntry(Object^ newVal)
          {
            //lock at entry level
            msclr::lock lockInstance(this);
            //1. see if hashCode and equals matches if yes then replace.
            //2. store node if val is null(GC collected ) replace that node(ie reuse)
            //3. create new node
            
            EntryNode^ startNode = m_entryNode;
            EntryNode^ nullNode = nullptr; //to replace

            while(startNode != nullptr)
            {
              if(startNode->isValEquals(newVal))
              {
                //replace value from new value;; will we ever come acrosss here ???
                startNode->Value = newVal;
                return;
              }
              else if(nullNode == nullptr && startNode->Value == nullptr)
              {
                //reuse the node once it gced
                nullNode = startNode;
              }
              startNode = startNode->NextNode;
            }

            if(nullNode != nullptr)
            {
              nullNode->Value = newVal;
            }
            else
            {
              //create new node
              EntryNode^ newNode = gcnew EntryNode(newVal, m_entryNode);
              m_entryNode = newNode;//append new node as put can happen immediately for this 
            }
          }

          Object^ Get(Object^ val)
          {
            EntryNode^ startNode = m_entryNode;

            while(startNode != nullptr)
            {
              if(startNode->isValEquals(val))
              {
                return startNode->Value;
              }
              startNode = startNode->NextNode;
            }            

            return nullptr;
          }
        };

        public ref class WeakHashMap
        {
          //not thread safe 
          Dictionary<int, MapEntry^>^ m_dictionary;
          ReaderWriterLock^ m_readerWriterLock;

        public:
          WeakHashMap()
          {
            m_dictionary = gcnew Dictionary<int, MapEntry^>(1000);
            m_readerWriterLock = gcnew ReaderWriterLock();
          }

					property int Count
					{
						int get(){return m_dictionary->Count;}
					}

          void Put(Object^ val)
          {
            int hash = val->GetHashCode();

            MapEntry^ ret = GetEntry(val);    

            if(ret != nullptr)
            {
              // this will take lock at entry level
              //so once entry is created it will remain there and we don't need to take writer lock
              ret->AddEntry(val);
            }
            else
            {
              try
              {
                m_readerWriterLock->AcquireWriterLock(-1);
                
                m_dictionary->TryGetValue(hash,ret);

                if(ret != nullptr)
                {
                  ret->AddEntry(val);
                }
                else
                {
                  ret = gcnew MapEntry(val);
                  m_dictionary[hash] = ret;
                }
              }finally
              {
                m_readerWriterLock->ReleaseWriterLock();
              }
            }
          }

          MapEntry^ GetEntry(Object^ val)
          {
            int hash = val->GetHashCode();

            MapEntry^ ret = nullptr;

            try
            {
              m_readerWriterLock->AcquireReaderLock(-1);//infinite timeout
              m_dictionary->TryGetValue(hash,ret);
            }finally
            {
              m_readerWriterLock->ReleaseReaderLock();
            }
            return ret; 
          }

          Object^ Get(Object^ val)
          {
            if(m_dictionary->Count > 0)
            {
              int hash = val->GetHashCode();

              MapEntry^ ret = nullptr;

              try
              {
                m_readerWriterLock->AcquireReaderLock(-1);//infinite timeout
                m_dictionary->TryGetValue(hash,ret);

                if(ret != nullptr)
                {
                  return ret->Get(val);
                }
              }finally
              {
                m_readerWriterLock->ReleaseReaderLock();
              }
            }
            return nullptr;
          }

          void Clear()
          {
            m_dictionary->Clear();
          }
        };
      }
    }
  }
	}
}