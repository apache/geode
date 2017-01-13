/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "PdxType.hpp"
#include "PdxHelper.hpp"
#include "PdxTypeRegistry.hpp"
#include "../Log.hpp"
#include <msclr/lock.h>
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
        void PdxType::AddFixedLengthTypeField(String^ fieldName, String^ className, 
                                                Byte typeId, Int32 size)
        {
          int current = m_pdxFieldTypes->Count;
          PdxFieldType^ pfType = gcnew PdxFieldType(fieldName, className, (Byte)typeId,
																										current/*field index*/, 
																										false, size, 0/*var len field idx*/);
          m_pdxFieldTypes->Add(pfType);
					//this will make sure one can't add same field name
          m_fieldNameVsPdxType->Add(fieldName, pfType);
        }

        void PdxType::AddVariableLengthTypeField(String^ fieldName, String^ className, 
                                                  Byte typeId)
        {
					//we don't store offset of first var len field, this is the purpose of following check
          if(m_isVarLenFieldAdded)
            m_varLenFieldIdx++;//it initial value is zero so variable length field idx start with zero
          
          m_numberOfVarLenFields++;

          m_isVarLenFieldAdded = true;

          int current = m_pdxFieldTypes->Count;
          PdxFieldType^ pfType = gcnew PdxFieldType(fieldName, className, (Byte)typeId, 
																										current, true, -1, m_varLenFieldIdx);
          m_pdxFieldTypes->Add(pfType);
					//this will make sure one can't add same field name
          m_fieldNameVsPdxType->Add(fieldName, pfType);
        }

        void PdxType::ToData( DataOutput^ output )
        {
					//defaulf java Dataserializable require this
          output->WriteByte(GemFireClassIds::DATA_SERIALIZABLE);
          output->WriteByte(GemFireClassIds::JAVA_CLASS);
          output->WriteObject((Object^)m_javaPdxClass);

					//pdx type
          output->WriteString(m_className);
          output->WriteBoolean(m_noJavaClass);
          output->WriteInt32(m_gemfireTypeId);
          output->WriteInt32(m_varLenFieldIdx);

          output->WriteArrayLen(m_pdxFieldTypes->Count);

          for(int i =0; i < m_pdxFieldTypes->Count; i++)
          {
            m_pdxFieldTypes[i]->ToData(output);
          }
        }
        
        IGFSerializable^ PdxType::FromData( DataInput^ input )
        {
					//defaulf java Dataserializable require this
          Byte val = input->ReadByte();//DS
          Byte val1 = input->ReadByte();//class
          input->ReadObject();//classname

          m_className = input->ReadString();
          //Object^ pdxObject = Serializable::GetPdxType(m_className);
         // m_pdxDomainType = pdxObject->GetType();

          /*PdxWrapper^ pdxWrapper = dynamic_cast<PdxWrapper^>(pdxObject);
          if(pdxWrapper == nullptr)
            m_pdxDomainType = pdxObject->GetType();
          else
            m_pdxDomainType = pdxWrapper->GetObject()->GetType();*/
          //Log::Debug("PdxType::FromData " + m_className);
          m_noJavaClass = input->ReadBoolean();
          m_gemfireTypeId = input->ReadInt32();
          m_varLenFieldIdx = input->ReadInt32();

          int len = input->ReadArrayLen();
          
					bool foundVarLenType = false;
          for(int i = 0; i < len; i++)
          {
            PdxFieldType^ pft = gcnew PdxFieldType();
            pft->FromData(input);

            m_pdxFieldTypes->Add(pft);

						if(pft->IsVariableLengthType == true)
							foundVarLenType = true;
          }

					//as m_varLenFieldIdx starts with 0
					if(m_varLenFieldIdx != 0)
						m_numberOfVarLenFields = m_varLenFieldIdx  + 1;
					else if(foundVarLenType)
						m_numberOfVarLenFields = 1;
          
					InitializeType();

          return this;
        }

        void PdxType::InitializeType()
        {
          initRemoteToLocal();//for writing
          initLocalToRemote();//for reading

          generatePositionMap();

        }

        Int32 PdxType::GetFieldPosition(String^ fieldName, uint8_t* offsetPosition, Int32 offsetSize, Int32 pdxStreamlen)
        {
          PdxFieldType^ pft = nullptr;
          m_fieldNameVsPdxType->TryGetValue(fieldName, pft);

          if(pft != nullptr)
          {
            if(pft->IsVariableLengthType)
              return variableLengthFieldPosition(pft, offsetPosition, offsetSize, pdxStreamlen);
            else
              return fixedLengthFieldPosition(pft, offsetPosition, offsetSize, pdxStreamlen);
          }

          return -1;
        }

        Int32 PdxType::GetFieldPosition(Int32 fieldIdx, uint8_t* offsetPosition, Int32 offsetSize, Int32 pdxStreamlen)
        {
          PdxFieldType^ pft = m_pdxFieldTypes[fieldIdx];
          
          if(pft != nullptr)
          {
            if(pft->IsVariableLengthType)
              return variableLengthFieldPosition(pft, offsetPosition, offsetSize, pdxStreamlen);
            else
              return fixedLengthFieldPosition(pft, offsetPosition, offsetSize, pdxStreamlen);
          }

          return -1;
        }

        Int32 PdxType::variableLengthFieldPosition(PdxFieldType^ varLenField, uint8_t* offsetPosition, Int32 offsetSize, Int32 pdxStreamlen)
        {
          int seqId = varLenField->SequenceId;          

          int offset = varLenField->VarLenOffsetIndex ;

          if(offset == -1)
            return /*first var len field*/ varLenField->RelativeOffset;
          else
          {
						//we write offset from behind
            return PdxHelper::ReadInt(offsetPosition + (m_numberOfVarLenFields - offset - 1)*offsetSize ,offsetSize);
          }

        }

        Int32 PdxType::fixedLengthFieldPosition(PdxFieldType^ fixLenField, uint8_t* offsetPosition, Int32 offsetSize, Int32 pdxStreamlen)
        {
          int seqId = fixLenField->SequenceId;          

          int offset = fixLenField->VarLenOffsetIndex;

          if(fixLenField->RelativeOffset >= 0)
          {
						//starting fields
            return fixLenField->RelativeOffset ;
          }
          else if(offset == -1) //Pdx length     
          {
						//there is no var len field so just subtracts relative offset from behind
            return pdxStreamlen + fixLenField->RelativeOffset ;
          }
          else
          {
						//need to read offset and then subtract relative offset
            return PdxHelper::ReadInt(offsetPosition + 
																			(m_numberOfVarLenFields - offset -1)*offsetSize,
                                       offsetSize)
																			 + fixLenField->RelativeOffset ;
          }
        }

        PdxType^ PdxType::isLocalTypeContains(PdxType^ otherType)
        {
          if(m_pdxFieldTypes->Count >= otherType->m_pdxFieldTypes->Count)
          {
            return isContains(this, otherType);
          }
          return nullptr;
        }

        PdxType^ PdxType::isRemoteTypeContains(PdxType^ remoteType)
        {
          if(m_pdxFieldTypes->Count <= remoteType->m_pdxFieldTypes->Count)
          {
            return isContains(remoteType, this);
          }
          return nullptr;
        }

        PdxType^ PdxType::MergeVersion(PdxType^ otherVersion)
        {
          int nTotalFields = otherVersion->m_pdxFieldTypes->Count;
          PdxType^ contains = nullptr;

          if(isLocalTypeContains(otherVersion) != nullptr)
            return this;

          if(isRemoteTypeContains(otherVersion) != nullptr)
            return otherVersion;

          //need to create new one, clone of local
          PdxType^ newone = clone();

          int varLenFields = newone->m_numberOfVarLenFields;
          
          for each(PdxFieldType^ tmp in otherVersion->m_pdxFieldTypes)
          {
            bool found = false;
            for each(PdxFieldType^ tmpNew in newone->m_pdxFieldTypes)
            {
              if(tmpNew->Equals(tmp))
              {
                found = true;
                break;
              }
            }
            if(!found)
            {
              PdxFieldType^ newFt = gcnew PdxFieldType(tmp->FieldName, 
                                                        tmp->ClassName,
                                                        tmp->TypeId,
                                                        newone->m_pdxFieldTypes->Count,//sequence id
                                                        tmp->IsVariableLengthType, 
                                                        tmp->Size,
                                                        (tmp->IsVariableLengthType? varLenFields++/*it increase after that*/: 0));
              newone->m_pdxFieldTypes->Add(newFt);//fieldnameVsPFT will happen after that							
            }
          }

          newone->m_numberOfVarLenFields = varLenFields;
          if(varLenFields >0)
            newone->m_varLenFieldIdx = varLenFields;

          //need to keep all versions in local version
          //m_otherVersions->Add(newone);
          return newone;
        }

        PdxType^ PdxType::isContains(PdxType^ first, PdxType^ second)
        {
          int j = 0;
          for(int i =0; i< second->m_pdxFieldTypes->Count; i++)
          {   
            PdxFieldType^ secondPdt = second->m_pdxFieldTypes[i];
            bool matched = false;
            for(; j< first->m_pdxFieldTypes->Count; j++)
            {
              PdxFieldType^ firstPdt = first->m_pdxFieldTypes[j];

              if(firstPdt->Equals(secondPdt))
              {
                matched = true;
                break;
              }
            }
            if(!matched)
              return nullptr;
          }
          return first;
        }

        PdxType^ PdxType::clone()
        {
          PdxType^ newone = gcnew PdxType(m_className, false);
          newone->m_gemfireTypeId = 0;
          newone->m_numberOfVarLenFields = m_numberOfVarLenFields;

          for each(PdxFieldType^ tmp in m_pdxFieldTypes)
          {
            newone->m_pdxFieldTypes->Add(tmp);
          }        
          return newone;
        }

         array<int>^ PdxType::GetLocalToRemoteMap()
         {
           if(m_localToRemoteFieldMap != nullptr)
            return m_localToRemoteFieldMap;
           
           msclr::lock lockInstance(m_lockObj);
           if(m_localToRemoteFieldMap != nullptr)
            return m_localToRemoteFieldMap;
           initLocalToRemote();

           return m_localToRemoteFieldMap;

         }
         array<int>^ PdxType::GetRemoteToLocalMap()
         {
           //return m_remoteToLocalFieldMap;

           if(m_remoteToLocalFieldMap != nullptr)
            return m_remoteToLocalFieldMap;
           
           msclr::lock lockInstance(m_lockObj);
           if(m_remoteToLocalFieldMap != nullptr)
            return m_remoteToLocalFieldMap;
           initRemoteToLocal();

           return m_remoteToLocalFieldMap;
         }

        /*
         * this is write data on remote type(this will always have all fields)
         */ 
        void PdxType::initRemoteToLocal()
        {
          //get local type from type Registry and then map local fields.
          //need to generate static map
          PdxType^ localPdxType = PdxTypeRegistry::GetLocalPdxType(m_className);
          m_numberOfFieldsExtra = 0;

          if(localPdxType != nullptr)
          {
            IList<PdxFieldType^>^ localPdxFields = localPdxType->m_pdxFieldTypes;      
            Int32 fieldIdx = 0;
            
						m_remoteToLocalFieldMap = gcnew array<Int32>(m_pdxFieldTypes->Count);

            for each(PdxFieldType^ remotePdxField in m_pdxFieldTypes)
            {
              bool found  = false;
              
              for each(PdxFieldType^ localPdxfield in localPdxFields)
              {
                if(localPdxfield->Equals(remotePdxField))
                {
                  found = true;
                  m_remoteToLocalFieldMap[fieldIdx++] = 1;//field there in remote type
                  break;
                }
              }

              if(!found)
              {
                //while writing take this from weakhashmap
                //local field is not in remote type
                if(remotePdxField->IsVariableLengthType)
                  m_remoteToLocalFieldMap[fieldIdx++] = -1;//local type don't have this fields
                else
                  m_remoteToLocalFieldMap[fieldIdx++] = -2;//local type don't have this fields
                m_numberOfFieldsExtra++;
              }
            }
          }
        }
        
        void PdxType::initLocalToRemote()
        {
          PdxType^ localPdxType = PdxTypeRegistry::GetLocalPdxType(m_className);

          if(localPdxType != nullptr)
          {
            //Log::Debug("In initLocalToRemote: " + m_gemfireTypeId);
            IList<PdxFieldType^>^ localPdxFields = localPdxType->m_pdxFieldTypes;
      
            Int32 fieldIdx = 0;
            //type which need to read/write should control local type
            m_localToRemoteFieldMap = gcnew array<Int32>(localPdxType->m_pdxFieldTypes->Count);

            for(int i = 0; i < m_localToRemoteFieldMap->Length && i < m_pdxFieldTypes->Count;i++)
            {
              if(localPdxFields[fieldIdx]->Equals(m_pdxFieldTypes[i]))
              {
								//fields are in same order, we can read as it is
                m_localToRemoteFieldMap[fieldIdx++] = -2;
              }
              else
                break;
            }
            
						for(;fieldIdx < m_localToRemoteFieldMap->Length;)
            {
              PdxFieldType^ localPdxField  = localPdxType->m_pdxFieldTypes[fieldIdx];
              bool found  = false;
              
              for each(PdxFieldType^ remotePdxfield in m_pdxFieldTypes)
              {
                if(localPdxField->Equals(remotePdxfield))
                {
                  found = true;
									//store pdxfield type position to get the offset quickly
                  m_localToRemoteFieldMap[fieldIdx++] = remotePdxfield->SequenceId;
                  break;
                }
              }

              if(!found)
              {
                //local field is not in remote type
                m_localToRemoteFieldMap[fieldIdx++] = -1;
              }
            }
          }
        }

        void PdxType::generatePositionMap()
        {
          //1. all static fields offsets
          //--read next var len offset and subtract offset
          //--if no var len then take length of stream - (len of offsets)
          //2. there is no offser for first var len field
          
          bool foundVarLen = false;          
          int lastVarLenSeqId = 0;
          int prevFixedSizeOffsets = 0;
          //set offsets from back first
          PdxFieldType^ previousField = nullptr;
          for(int i = m_pdxFieldTypes->Count -1; i >= 0 ; i--)
          {
            PdxFieldType^ tmpft = m_pdxFieldTypes[i];
            m_fieldNameVsPdxType[tmpft->FieldName] = tmpft;

            if(tmpft->IsVariableLengthType)
            {
              tmpft->VarLenOffsetIndex = tmpft->VarLenFieldIdx;
              tmpft->RelativeOffset = 0;
              foundVarLen = true;
              lastVarLenSeqId = tmpft->VarLenFieldIdx;
            }
            else
            {
              if(foundVarLen)
              {
                tmpft->VarLenOffsetIndex = lastVarLenSeqId;
								//relative offset is subtracted from var len offsets
                tmpft->RelativeOffset = (-tmpft->Size + previousField->RelativeOffset) ;
              }
              else
              {
                tmpft->VarLenOffsetIndex = -1;//Pdx header length
								//relative offset is subtracted from var len offsets
                tmpft->RelativeOffset = -tmpft->Size;
                if(previousField != nullptr)//boundary condition
                  tmpft->RelativeOffset = (-tmpft->Size + previousField->RelativeOffset) ;
              }
            }

            previousField = tmpft;
          }

          foundVarLen = false;  
          prevFixedSizeOffsets = 0;
          //now do optimization till you don't fine var len
          for(int i = 0; i< m_pdxFieldTypes->Count && !foundVarLen; i++)
          {
            PdxFieldType^ tmpft = m_pdxFieldTypes[i];

            if(tmpft->IsVariableLengthType)
            {
              tmpft->VarLenOffsetIndex = -1;//first var len field
              tmpft->RelativeOffset = prevFixedSizeOffsets;
              foundVarLen = true;
            }
            else
            {
              tmpft->VarLenOffsetIndex = 0;//no need to read offset
              tmpft->RelativeOffset = prevFixedSizeOffsets;
              prevFixedSizeOffsets += tmpft->Size;              
            }
          }
        }

        bool PdxType::Equals(Object^ otherObj)
        {
          if(otherObj == nullptr)
            return false;

          PdxType^ ot = dynamic_cast<PdxType^>(otherObj);

          if(ot == nullptr)
            return false;

          if(ot == this)
            return true;

          if(ot->m_pdxFieldTypes->Count != m_pdxFieldTypes->Count)
            return false;

          for(int i = 0; i < m_pdxFieldTypes->Count; i++)
          {
            if(!ot->m_pdxFieldTypes[i]->Equals(m_pdxFieldTypes[i]))
              return false;
          }
          return true;
        }

        Int32 PdxType::GetHashCode()
        {
          int hash = m_cachedHashcode;
          if(hash == 0) 
          {
            hash = 1;
            hash = hash * 31 + m_className->GetHashCode();
            for(int i = 0; i < m_pdxFieldTypes->Count; i++) 
            {
              hash = hash * 31 + m_pdxFieldTypes[i]->GetHashCode();
            }
            if(hash == 0) 
            {
              hash = 1;
            }
            m_cachedHashcode = hash;      
          }
          return m_cachedHashcode;
        }
      }
			}
    }
  }
}