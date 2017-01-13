/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once
#include "PdxTypeRegistry.hpp"
#include "../Serializable.hpp"
#include "PdxWrapper.hpp"
using namespace System;

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
				int PdxTypeRegistry::testGetNumberOfPdxIds()
				{
					return typeIdToPdxType->Count;
				}

				int PdxTypeRegistry::testNumberOfPreservedData()
				{
					return preserveData->Count;
				}

        Int32 PdxTypeRegistry::GetPDXIdForType(Type^ pdxType, const char* poolname, PdxType^ nType, bool checkIfThere)
        {
          if(checkIfThere)
          {
            PdxType^ lpdx = GetLocalPdxType(nType->PdxClassName);

            if(lpdx != nullptr)
              return lpdx->TypeId;
          }

          try
          {
            g_readerWriterLock->AcquireWriterLock(-1);
            {
              if(checkIfThere)
              {
                PdxType^ lpdx = GetLocalPdxType(nType->PdxClassName);

                if(lpdx != nullptr)
                  return lpdx->TypeId;
              } 
            }
            return Serializable::GetPDXIdForType(poolname, nType);            
          }
          finally
          {
            g_readerWriterLock->ReleaseWriterLock();
          }
            
        }

        Int32 PdxTypeRegistry::GetPDXIdForType(PdxType^ pType, const char* poolname)
        {
          IDictionary<PdxType^, Int32>^ tmp = pdxTypeToTypeId;
          Int32 typeId = 0;

          tmp->TryGetValue(pType, typeId);

          if(typeId != 0)
            return typeId;

          try
          {
            g_readerWriterLock->AcquireWriterLock(-1);
            {
              tmp = pdxTypeToTypeId;
              tmp->TryGetValue(pType, typeId);

              if(typeId != 0)
                return typeId;

            }
            typeId = Serializable::GetPDXIdForType(poolname, pType);            
            pType->TypeId = typeId;

            IDictionary<PdxType^, Int32>^ newDict = gcnew Dictionary<PdxType^, Int32>(pdxTypeToTypeId);
            newDict[pType] = typeId;
            pdxTypeToTypeId = newDict;            
          }
          finally
          {
            g_readerWriterLock->ReleaseWriterLock();
          }
          PdxTypeRegistry::AddPdxType(typeId, pType);
          return typeId;
        }

        void PdxTypeRegistry::clear()
        {
          try
          {    
            g_readerWriterLock->AcquireWriterLock(-1);
            typeIdToPdxType = gcnew Dictionary<Int32, PdxType^>();
            remoteTypeIdToMergedPdxType= gcnew Dictionary<Int32, PdxType^>();
            localTypeToPdxType = gcnew Dictionary<String^, PdxType^>();
            pdxTypeToTypeId = gcnew Dictionary<PdxType^, Int32>();
            preserveData = gcnew WeakHashMap();    
            intToEnum = gcnew Dictionary<Int32, EnumInfo^>();
            enumToInt = gcnew Dictionary<EnumInfo^, Int32>();
          }finally
          {
            g_readerWriterLock->ReleaseWriterLock();
          }
        }

        void PdxTypeRegistry::AddPdxType(Int32 typeId, PdxType^ pdxType)
        {
          try
          {
            g_readerWriterLock->AcquireWriterLock(-1);
            IDictionary<Int32, PdxType^>^ newDict = gcnew Dictionary<Int32, PdxType^>(typeIdToPdxType);
            newDict[typeId] = pdxType;
            typeIdToPdxType = newDict;
          }
          finally
          {
            g_readerWriterLock->ReleaseWriterLock();
          }
        }

        PdxType^ PdxTypeRegistry::GetPdxType(Int32 typeId)
        {
          try
          {
           // g_readerWriterLock->AcquireReaderLock(-1);
            IDictionary<Int32, PdxType^>^ tmpDict = typeIdToPdxType;
            PdxType^ retVal = nullptr;

            tmpDict->TryGetValue(typeId, retVal);

            return retVal;
          }
          finally
          {
           // g_readerWriterLock->ReleaseReaderLock();
          } 
        }

        void PdxTypeRegistry::AddLocalPdxType(String^ localType, PdxType^ pdxType)
        {
          try
          {
            g_readerWriterLock->AcquireWriterLock(-1);
            IDictionary<String^, PdxType^>^ tmp = gcnew Dictionary<String^, PdxType^>(localTypeToPdxType);
            tmp[localType] = pdxType;
            localTypeToPdxType = tmp;
          }
          finally
          {
            g_readerWriterLock->ReleaseWriterLock();
          }
        }

        PdxType^ PdxTypeRegistry::GetLocalPdxType(String^ localType)
        {
          try
          {
            //g_readerWriterLock->AcquireReaderLock(-1);
            IDictionary<String^, PdxType^>^ tmp = localTypeToPdxType;
            PdxType^ retVal = nullptr;

            tmp->TryGetValue(localType, retVal);

            return retVal;
          }
          finally
          {
            //g_readerWriterLock->ReleaseReaderLock();
          }
        }

        void PdxTypeRegistry::SetMergedType(Int32 remoteTypeId, PdxType^ mergedType)
        {
          try
          {
            g_readerWriterLock->AcquireWriterLock(-1);
            IDictionary<Int32, PdxType^>^ tmp = gcnew Dictionary<Int32, PdxType^>(remoteTypeIdToMergedPdxType);
            tmp[remoteTypeId] = mergedType;
            remoteTypeIdToMergedPdxType = tmp;
          }
          finally
          {
            g_readerWriterLock->ReleaseWriterLock();
          }
        }

        PdxType^ PdxTypeRegistry::GetMergedType(Int32 remoteTypeId)
        {
          try
          {
            //g_readerWriterLock->AcquireReaderLock(-1);
            IDictionary<Int32, PdxType^>^ tmp = remoteTypeIdToMergedPdxType;
            PdxType^ retVal = nullptr;

            tmp->TryGetValue(remoteTypeId, retVal);

            return retVal;
          }
          finally
          {
           // g_readerWriterLock->ReleaseReaderLock();
          }
        }

        void PdxTypeRegistry::SetPreserveData(IPdxSerializable^ obj, PdxRemotePreservedData^ pData)
        {
          //preserveData[obj] = pData;
          PdxWrapper^ pdxWrapper = dynamic_cast<PdxWrapper^>(obj);

          if(pdxWrapper != nullptr)
          {
            pData->Owner = pdxWrapper->GetObject();
          }
          else
            pData->Owner = obj;
          preserveData->Put(pData);
        }

        PdxRemotePreservedData^ PdxTypeRegistry::GetPreserveData(IPdxSerializable^ pdxobj)
        {
          Object^ obj = pdxobj;
          PdxWrapper^ pdxWrapper = dynamic_cast<PdxWrapper^>(obj);
          if(pdxWrapper != nullptr)
            obj = pdxWrapper->GetObject();
          //PdxRemotePreservedData^ ppd = nullptr;

          //preserveData->TryGetValue(obj, ppd);
          Object^ retVal = preserveData->Get(obj);

          if(retVal != nullptr)
            return (PdxRemotePreservedData^)retVal;

          return nullptr;
        }

        Int32 PdxTypeRegistry::GetEnumValue(EnumInfo^ ei)
        {
          IDictionary<EnumInfo^, Int32>^ tmp = enumToInt;
          if(tmp->ContainsKey(ei))
            return tmp[ei];
          try
          {
             g_readerWriterLock->AcquireWriterLock(-1);
             tmp = enumToInt;
             if(tmp->ContainsKey(ei))
              return tmp[ei];

             int val = Serializable::GetEnumValue(ei);
             tmp = gcnew Dictionary<EnumInfo^, Int32>(enumToInt);
             tmp[ei] = val;
             enumToInt = tmp;
             return val;
          }
          finally
          {
             g_readerWriterLock->ReleaseWriterLock();
          }
          return 0;
        }

        EnumInfo^ PdxTypeRegistry::GetEnum(Int32 enumVal)
        {
          IDictionary<Int32, EnumInfo^>^ tmp = intToEnum;
          EnumInfo^ ret = nullptr;
          tmp->TryGetValue(enumVal, ret);

          if(ret != nullptr)
            return ret;

           try
          {
             g_readerWriterLock->AcquireWriterLock(-1);
             tmp = intToEnum;
             tmp->TryGetValue(enumVal, ret);

            if(ret != nullptr)
              return ret;

             ret = Serializable::GetEnum(enumVal);
             tmp = gcnew Dictionary<Int32, EnumInfo^>(intToEnum);
             tmp[enumVal] = ret;
             intToEnum = tmp;
             return ret;
          }
          finally
          {
             g_readerWriterLock->ReleaseWriterLock();
          }
          return nullptr;
        }
      }
			}
    }
  }
}