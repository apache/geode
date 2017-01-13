/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "PdxHelper.hpp"
#include "PdxTypeRegistry.hpp"
#include "PdxWriterWithTypeCollector.hpp"
#include "PdxReaderWithTypeCollector.hpp"
#include "PdxRemoteReader.hpp"
#include "PdxRemoteWriter.hpp"
#include "../Serializable.hpp"
#include "PdxWrapper.hpp"
#include "../Log.hpp"
#include "PdxInstanceImpl.hpp"
#include <CacheImpl.hpp>
#include <CacheRegionHelper.hpp>
#include <gfcpp/Cache.hpp>
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

        CacheImpl* getCacheImpl()
        {
          CachePtr cache = CacheFactory::getAnyInstance();
          if (cache == NULLPTR)
          {
            throw gcnew IllegalStateException("cache has not been created yet.");;
          }
          if (cache->isClosed())
          {
            throw gcnew IllegalStateException("cache has been closed. ");
          }      
          return CacheRegionHelper::getCacheImpl(cache.ptr());
        }
        
        void PdxHelper::SerializePdx(DataOutput^ dataOutput, IPdxSerializable^ pdxObject)
        {          
          dataOutput->setPdxSerialization(true);
          String^ pdxClassname = nullptr;
          bool isPdxWrapper = false;
          //String^ className = nullptr;
          Type^ pdxType = nullptr;
          
          PdxWrapper^ pdxWrapper = dynamic_cast<PdxWrapper^>(pdxObject);

          if(pdxWrapper != nullptr)
          {
            //className = pdxWrapper->GetObject()->GetType()->FullName;
            isPdxWrapper = true;
            pdxType = pdxWrapper->GetObject()->GetType();
          }
          else
          {
            PdxInstanceImpl^ pdxII = dynamic_cast<PdxInstanceImpl^>(pdxObject);
            if(pdxII != nullptr)
            {
              PdxType^ piPt = pdxII->getPdxType();
              if(piPt != nullptr && piPt->TypeId == 0)//from pdxInstance factory need to get typeid from server
              {
                int typeId = PdxTypeRegistry::GetPDXIdForType(piPt, dataOutput->GetPoolName());
                pdxII->setPdxId(typeId);
              }
              PdxLocalWriter^ plw = gcnew PdxLocalWriter(dataOutput, piPt);  
              pdxII->ToData(plw);

              plw->EndObjectWriting();//now write typeid

              int len = 0;
              uint8_t* pdxStream = plw->GetPdxStream(len);
              pdxII->updatePdxStream( pdxStream, len);


              return;
            }
            //className = pdxObject->GetType()->FullName;
            pdxType = pdxObject->GetType();
          }
  
          pdxClassname = Serializable::GetPdxTypeName(pdxType->FullName);        
          PdxType^ localPdxType = PdxTypeRegistry::GetLocalPdxType(pdxClassname);         

          if(localPdxType == nullptr)
          {
            //need to grab type info, as fromdata is not called yet
            PdxWriterWithTypeCollector^ ptc = gcnew PdxWriterWithTypeCollector(dataOutput, pdxClassname);
            pdxObject->ToData(ptc);                      

            PdxType^ nType = ptc->PdxLocalType;//local type
            nType->InitializeType();//initialize it

						//get type id from server and then set it
            int nTypeId = PdxTypeRegistry::GetPDXIdForType(pdxType, 
																														dataOutput->GetPoolName(), nType, true);
            nType->TypeId = nTypeId;

            ptc->EndObjectWriting();//now write typeid

            PdxTypeRegistry::AddLocalPdxType(pdxClassname, nType);//add classname VS pdxType
            PdxTypeRegistry::AddPdxType(nTypeId, nType);//add typeid vs pdxtype
			//This is for pdx Statistics
            CacheImpl* cacheImpl = getCacheImpl();
            if (cacheImpl != NULL) {
              uint8_t* stPos = dataOutput->GetStartBufferPosition() + ptc->getStartPositionOffset();
              int pdxLen = PdxHelper::ReadInt32(stPos);       
              cacheImpl->m_cacheStats->incPdxSerialization(pdxLen + 1 + 2*4); //pdxLen + 93 DSID + len + typeID
            }
          }
          else//we know locasl type, need to see preerved data
          {
            //if object got from server than create instance of RemoteWriter otherwise local writer.
            PdxRemotePreservedData^ pd = PdxTypeRegistry::GetPreserveData(pdxObject);

            //now always remotewriter as we have API Read/WriteUnreadFields 
						//so we don't know whether user has used those or not;; Can we do some trick here?
            PdxRemoteWriter^ prw = nullptr;
            if(pd != nullptr)
            {
              PdxType^ mergedPdxType = PdxTypeRegistry::GetPdxType(pd->MergedTypeId);
            
              prw = gcnew PdxRemoteWriter(dataOutput, mergedPdxType, pd);
            }
            else
            {
              prw = gcnew PdxRemoteWriter(dataOutput, pdxClassname);                
            }

            pdxObject->ToData(prw);

            prw->EndObjectWriting();
			//This is for pdx Statistics
            CacheImpl* cacheImpl = getCacheImpl();
            if (cacheImpl != NULL) {
              uint8_t* stPos = dataOutput->GetStartBufferPosition() + prw->getStartPositionOffset();
              int pdxLen = PdxHelper::ReadInt32(stPos);       
              cacheImpl->m_cacheStats->incPdxSerialization(pdxLen + 1 + 2*4); //pdxLen + 93 DSID + len + typeID
            }
          }
        }


        IPdxSerializable^ PdxHelper::DeserializePdx(DataInput^ dataInput, bool forceDeserialize, int typeId, int len )
        {
          dataInput->setPdxdeserialization(true);
           String^ pdxClassname = nullptr;
           String^ pdxDomainClassname = nullptr; 
          IPdxSerializable^ pdxObject = nullptr;
            dataInput->AdvanceUMCursor();//it will increase the cursor in c++ layer
            dataInput->SetBuffer();//it will c++ buffer in cli layer

            PdxType^ pType = PdxTypeRegistry::GetPdxType(typeId);
            PdxType^ pdxLocalType = nullptr;

            if(pType != nullptr)//this may happen with PdxInstanceFactory
              pdxLocalType = PdxTypeRegistry::GetLocalPdxType(pType->PdxClassName);//this should be fine for IPdxTypeMapper

            if(pType != nullptr && pdxLocalType != nullptr)//type found 
            {
              pdxClassname = pType->PdxClassName;
              pdxDomainClassname = Serializable::GetLocalTypeName(pdxClassname);
              //Log::Debug("found type " + typeId + " " + pType->IsLocal);
              pdxObject = Serializable::GetPdxType(pdxDomainClassname);
              if(pType->IsLocal)//local type no need to read Unread data
              {
                PdxLocalReader^ plr = gcnew PdxLocalReader(dataInput, pType, len);
                pdxObject->FromData(plr);              
                plr->MoveStream();//it will set stream
              }
              else
              {
                PdxRemoteReader^ prr = gcnew PdxRemoteReader(dataInput, pType, len);              
                pdxObject->FromData(prr);

                PdxType^ mergedVersion = PdxTypeRegistry::GetMergedType(pType->TypeId);
                PdxRemotePreservedData^ preserveData = prr->GetPreservedData(mergedVersion, pdxObject);
                if(preserveData != nullptr)
                  PdxTypeRegistry::SetPreserveData(pdxObject, preserveData);//it will set data in weakhashmap
                prr->MoveStream();
              }
            }
            else//type not found; need to get from server
            {
              if(pType == nullptr)
              {
                pType = (PdxType^)(Serializable::GetPDXTypeById(dataInput->GetPoolName(), typeId));
                pdxLocalType = PdxTypeRegistry::GetLocalPdxType(pType->PdxClassName);//this should be fine for IPdxTypeMappers
              }
              
              pdxClassname = pType->PdxClassName;
              pdxDomainClassname = Serializable::GetLocalTypeName(pdxClassname);

              pdxObject = Serializable::GetPdxType(pdxDomainClassname);
              
              Object^ pdxRealObject = pdxObject;
              bool isPdxWrapper = false;
            
              PdxWrapper^ pdxWrapper = dynamic_cast<PdxWrapper^>(pdxObject);

              if(pdxWrapper != nullptr)
              {
                //pdxDomainType = pdxWrapper->GetObject()->GetType();
                isPdxWrapper = true;
              }
              else
              {
                //pdxDomainType = pdxObject->GetType();
              }              

              if(pdxLocalType == nullptr)//need to know local type
              {
                PdxReaderWithTypeCollector^ prtc = gcnew PdxReaderWithTypeCollector(dataInput,pType,len);
                pdxObject->FromData(prtc);          

                if(isPdxWrapper)
                  pdxRealObject = pdxWrapper->GetObject();

                pdxLocalType = prtc->LocalType;
              
                if(pType->Equals(pdxLocalType))//same
                {
                  PdxTypeRegistry::AddLocalPdxType(pdxClassname, pType);
                  PdxTypeRegistry::AddPdxType(pType->TypeId, pType); 
                  pType->IsLocal = true;
                }
                else
                {
                  //need to know local type and then merge type
                  pdxLocalType->InitializeType();
                  pdxLocalType->TypeId = PdxTypeRegistry::GetPDXIdForType(pdxObject->GetType(), 
																																				  dataInput->GetPoolName(), 
																																				  pdxLocalType, true);
                  pdxLocalType->IsLocal = true;
                  PdxTypeRegistry::AddLocalPdxType(pdxClassname, pdxLocalType);//added local type
                  PdxTypeRegistry::AddPdxType(pdxLocalType->TypeId, pdxLocalType); 
                  
                  pType->InitializeType();
                  PdxTypeRegistry::AddPdxType(pType->TypeId, pType); //adding remote type
                  //pdxLocalType->AddOtherVersion(pType);
                  //pdxLocalType->AddOtherVersion(pdxLocalType);//no need to add local type
                  
                  //need to create merge type
                  CreateMergedType(pdxLocalType, pType, dataInput);
                  
                  PdxType^ mergedVersion = PdxTypeRegistry::GetMergedType(pType->TypeId);
                  PdxRemotePreservedData^ preserveData = prtc->GetPreservedData(mergedVersion, pdxObject);
                  if(preserveData != nullptr)
                    PdxTypeRegistry::SetPreserveData(pdxObject, preserveData);
                }
                prtc->MoveStream();
              }
              else//remote reader will come here as local type is there
              {
                pType->InitializeType();
                //Log::Debug("Adding type " + pType->TypeId);
                PdxTypeRegistry::AddPdxType(pType->TypeId, pType); //adding remote type
                //pdxLocalType->AddOtherVersion(pType);
                
                PdxRemoteReader^ prr = gcnew PdxRemoteReader(dataInput, pType, len);

                pdxObject->FromData(prr); 

                if(isPdxWrapper)
                  pdxRealObject = pdxWrapper->GetObject();

                //need to create merge type
                CreateMergedType(pdxLocalType, pType, dataInput);

                PdxType^ mergedVersion = PdxTypeRegistry::GetMergedType(pType->TypeId);
                PdxRemotePreservedData^ preserveData = prr->GetPreservedData(mergedVersion, pdxObject);
                if(preserveData != nullptr)
                  PdxTypeRegistry::SetPreserveData(pdxObject, preserveData);
                prr->MoveStream();
              }
            }//end type not found
            return pdxObject;
        }

        IPdxSerializable^ PdxHelper::DeserializePdx(DataInput^ dataInput, bool forceDeserialize )
        {
          try
          {
            dataInput->setPdxdeserialization(true);
          if(PdxTypeRegistry::PdxReadSerialized == false || forceDeserialize ||dataInput->isRootObjectPdx())
          {
            
            //here we are reading length and typeId..Note; our internal typeid already read in c++ layer
            int len = dataInput->ReadInt32();
            int typeId= dataInput->ReadInt32();

			//This is for pdx Statistics
            CacheImpl* cacheImpl = getCacheImpl();
            if (cacheImpl != NULL) {        
              cacheImpl->m_cacheStats->incPdxDeSerialization(len + 9);//pdxLen + 1 + 2*4
            }

            return DeserializePdx(dataInput, forceDeserialize, typeId, len);
          }//create PdxInstance
          else
          {
            IPdxSerializable^ pdxObject = nullptr;
            //here we are reading length and typeId..Note; our internal typeid already read in c++ layer
           int len = dataInput->ReadInt32();
           int typeId= dataInput->ReadInt32();

//            Log::Debug(" len " + len + " " + typeId + " readbytes " + dataInput->BytesReadInternally);

            PdxType^ pType = PdxTypeRegistry::GetPdxType(typeId);

            if(pType == nullptr)
            {
              PdxType^ pType = (PdxType^)(Serializable::GetPDXTypeById(dataInput->GetPoolName(), typeId));
              //this should be fine for IPdxTypeMapper
              PdxTypeRegistry::AddLocalPdxType(pType->PdxClassName, pType);
              PdxTypeRegistry::AddPdxType(pType->TypeId, pType); 
              //pType->IsLocal = true; ?????
            }

           // pdxObject = gcnew PdxInstanceImpl(gcnew DataInput(dataInput->GetBytes(dataInput->GetCursor(), len  + 8 ), len  + 8));
             pdxObject = gcnew PdxInstanceImpl(dataInput->GetBytes(dataInput->GetCursor(), len ), len, typeId, true );

            dataInput->AdvanceCursorPdx(len );
            
            dataInput->AdvanceUMCursor();
            
            dataInput->SetBuffer();

            //This is for pdxinstance Statistics            
            CacheImpl* cacheImpl = getCacheImpl();
            if (cacheImpl != NULL) {
              cacheImpl->m_cacheStats->incPdxInstanceCreations();		
            }
            return pdxObject;
          }
          }finally
          {
            dataInput->setPdxdeserialization(false);
          }
        }

        Int32 PdxHelper::GetEnumValue(String^ enumClassName, String^ enumName, int hashcode)
        {
          //in case app want different name
          enumClassName = Serializable::GetPdxTypeName(enumClassName);
          EnumInfo^ ei = gcnew EnumInfo(enumClassName, enumName, hashcode);
          return PdxTypeRegistry::GetEnumValue(ei);        
        }

        Object^ PdxHelper::GetEnum(int enumId)
        {
          EnumInfo^ ei = PdxTypeRegistry::GetEnum(enumId);
          return ei->GetEnum();
        }

        void PdxHelper::CreateMergedType(PdxType^ localType, PdxType^ remoteType, DataInput^ dataInput)
        {
          PdxType^ mergedVersion = localType->MergeVersion(remoteType);
                
          if(mergedVersion->Equals(localType))
          {
            PdxTypeRegistry::SetMergedType(remoteType->TypeId, localType); 
          }
          else if(mergedVersion->Equals(remoteType))
          {
            PdxTypeRegistry::SetMergedType(remoteType->TypeId, remoteType); 
          }
          else
          {//need to create new version            
            mergedVersion->InitializeType();
            if(mergedVersion->TypeId == 0)
              mergedVersion->TypeId = Serializable::GetPDXIdForType(dataInput->GetPoolName(), mergedVersion);              
            
           // PdxTypeRegistry::AddPdxType(remoteType->TypeId, mergedVersion);
            PdxTypeRegistry::AddPdxType(mergedVersion->TypeId, mergedVersion);  
            PdxTypeRegistry::SetMergedType(remoteType->TypeId, mergedVersion); 
            PdxTypeRegistry::SetMergedType(mergedVersion->TypeId, mergedVersion); 
          }
        }

        Int32 PdxHelper::ReadInt32(uint8_t* offsetPosition)
        {
          Int32 data = offsetPosition[0];
          data = (data << 8) | offsetPosition[1];
          data = (data << 8) | offsetPosition[2];
          data = (data << 8) | offsetPosition[3];

          return data;
        }

        Int32 PdxHelper::ReadInt16(uint8_t* offsetPosition)
        {
          int16_t data = offsetPosition[0];
          data = (data << 8) | offsetPosition[1];
          return (Int32)data;
        }

				Int32 PdxHelper::ReadUInt16(uint8_t* offsetPosition)
        {
					UInt16 data = offsetPosition[0];
          data = (data << 8) | offsetPosition[1];
          return (Int32)data;
        }

        Int32 PdxHelper::ReadByte(uint8_t* offsetPosition)
        {
          return (Int32)offsetPosition[0];
        }

        void PdxHelper::WriteInt32(uint8_t* offsetPosition, Int32 value)
        {
          offsetPosition[0] = (uint8_t)(value >> 24);
          offsetPosition[1] = (uint8_t)(value >> 16);
          offsetPosition[2] = (uint8_t)(value >> 8);
          offsetPosition[3] = (uint8_t)value;
        }

        void PdxHelper::WriteInt16(uint8_t* offsetPosition, Int32 value)
        {
          Int16 val = (Int16)value;
          offsetPosition[0] = (uint8_t)(val >> 8);
          offsetPosition[1] = (uint8_t)val;
        }

        void PdxHelper::WriteByte(uint8_t* offsetPosition, Int32 value)
        {
          offsetPosition[0] = (Byte)value;
        }

        Int32 PdxHelper::ReadInt(uint8_t* offsetPosition, int size)
        {
          switch(size)
          {
          case 1:
            return ReadByte(offsetPosition);
          case 2:
            return ReadUInt16(offsetPosition);
          case 4:
            return ReadInt32(offsetPosition);
          }
          throw gcnew System::ArgumentException("Size should be 1,2 or 4 in PdxHelper::ReadInt.");
        }
			}
      }
    }
  }
}