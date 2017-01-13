#include <iostream>

#include "gtest/gtest.h"

#include <TcrMessage.hpp>
#include "ByteArrayFixture.hpp"

using namespace gemfire;

#define EXPECT_MESSAGE_EQ(e, a) EXPECT_PRED_FORMAT2(assertMessageEqual, e, a)

class TcrMessageTest : public ::testing::Test, protected ByteArrayFixture {
 public:
  ::testing::AssertionResult assertMessageEqual(
      const char *expectedStr, const char *bytesStr, const char *expected,
      const gemfire::TcrMessage &msg) {
    gemfire::ByteArray bytes(
        reinterpret_cast<const uint8_t *>(msg.getMsgData()),
        static_cast<const std::size_t>(msg.getMsgLength()));
    return ByteArrayFixture::assertByteArrayEqual(expectedStr, bytesStr,
                                                  expected, bytes);
  }
};

TEST_F(TcrMessageTest, intializeDefaultConstructor) {
  TcrMessageReply message(true, NULL);

  EXPECT_EQ(TcrMessage::INVALID, message.getMessageType());
}

TEST_F(TcrMessageTest, testConstructor1MessageDataContentWithDESTROY_REGION) {
  const Region *region = NULL;
  const UserDataPtr aCallbackArgument = NULL;
  int messageResponseTimeout = 1000;
  ThinClientBaseDM *connectionDM = NULL;

  TcrMessageDestroyRegion message(region, aCallbackArgument,
                                  messageResponseTimeout, connectionDM);

  EXPECT_EQ(TcrMessage::DESTROY_REGION, message.getMessageType());

  EXPECT_MESSAGE_EQ(
      "0000000B0000003800000003\\h{8}"
      "000000001300494E56414C49445F524547494F4E5F4E414D450000001200030000000000"
      "0000010300000000000000\\h{2}0000000400000003E8",
      message);
}

TEST_F(TcrMessageTest, testConstructor1MessageDataContentWithCLEAR_REGION) {
  const Region *region = NULL;
  const UserDataPtr aCallbackArgument = NULL;
  int messageResponseTimeout = 1000;
  ThinClientBaseDM *connectionDM = NULL;

  TcrMessageClearRegion message(region, aCallbackArgument,
                                messageResponseTimeout, connectionDM);

  EXPECT_MESSAGE_EQ(
      "000000240000003800000003\\h{8}"
      "000000001300494E56414C49445F524547494F4E5F4E414D450000001200030000000000"
      "0000010300000000000000\\h{2}0000000400000003E8",
      message);
}

TEST_F(TcrMessageTest, testQueryConstructorMessageDataCotent) {
  int messageResponseTimeout = 1000;
  ThinClientBaseDM *connectionDM = NULL;

  TcrMessageCloseCQ message("myRegionName", messageResponseTimeout,
                            connectionDM);

  EXPECT_EQ(TcrMessage::CLOSECQ_MSG_TYPE, message.getMessageType());

  EXPECT_MESSAGE_EQ(
      "0000002D0000003100000003FFFFFFFF000000000C006D79526567696F6E4E616D650000"
      "0012000300000000000000010300000000000000\\h{2}0000000400000003E8",
      message);
}

TEST_F(TcrMessageTest, testQueryConstructorWithQUERY) {
  int messageResponseTimeout = 1000;
  ThinClientBaseDM *connectionDM = NULL;

  TcrMessageQuery message("aRegionName", messageResponseTimeout, connectionDM);

  EXPECT_EQ(TcrMessage::QUERY, message.getMessageType());

  EXPECT_MESSAGE_EQ(
      "000000220000003000000003FFFFFFFF000000000B0061526567696F6E4E616D65000000"
      "12000300000000000000010300000000000000\\h{2}0000000400000003E8",
      message);
}

TEST_F(TcrMessageTest, testQueryConstructorWithSTOPCQ_MSG_TYPE) {
  int messageResponseTimeout = 1000;
  ThinClientBaseDM *connectionDM = NULL;

  TcrMessageStopCQ message("aRegionName", messageResponseTimeout, connectionDM);

  EXPECT_EQ(TcrMessage::STOPCQ_MSG_TYPE, message.getMessageType());

  EXPECT_MESSAGE_EQ(
      "0000002C0000003000000003FFFFFFFF000000000B0061526567696F6E4E616D65000000"
      "12000300000000000000010300000000000000\\h{2}0000000400000003E8",
      message);
}

TEST_F(TcrMessageTest, testQueryConstructorWithCLOSECQ_MSG_TYPE) {
  int messageResponseTimeout = 1000;
  ThinClientBaseDM *connectionDM = NULL;

  TcrMessageCloseCQ message("aRegionName", messageResponseTimeout,
                            connectionDM);

  EXPECT_EQ(TcrMessage::CLOSECQ_MSG_TYPE, message.getMessageType());

  EXPECT_MESSAGE_EQ(
      "0000002D0000003000000003FFFFFFFF000000000B0061526567696F6E4E616D65000000"
      "12000300000000000000010300000000000000\\h{2}0000000400000003E8",
      message);
}

TEST_F(TcrMessageTest,
       testParameterizedQueryConstructorWithQUERY_WITH_PARAMETERS) {
  int messageResponseTimeout = 1000;
  ThinClientBaseDM *connectionDM = NULL;
  const UserDataPtr aCallbackArgument = NULL;
  CacheableVectorPtr paramList = CacheableVector::create();

  TcrMessageQueryWithParameters message("aRegionName", aCallbackArgument,
                                        paramList, messageResponseTimeout,
                                        connectionDM);

  EXPECT_EQ(TcrMessage::QUERY_WITH_PARAMETERS, message.getMessageType());

  EXPECT_MESSAGE_EQ(
      "000000500000002B00000004FFFFFFFF000000000B0061526567696F6E4E616D65000000"
      "04000000000000000004000000000F0000000400000003E8",
      message);
}

TEST_F(TcrMessageTest, testConstructorWithCONTAINS_KEY) {
  TcrMessageContainsKey message(
      static_cast<const Region *>(NULL),
      CacheableString::create(
          "mykey"),  // static_cast<const CacheableKeyPtr>(NULLPTR),
      static_cast<const UserDataPtr>(NULLPTR),
      true,  // isContainsKey
      static_cast<ThinClientBaseDM *>(NULL));
  EXPECT_EQ(TcrMessage::CONTAINS_KEY, message.getMessageType());

  EXPECT_MESSAGE_EQ(
      "000000260000002E00000003FFFFFFFF000000001300494E56414C49445F524547494F4E"
      "5F4E414D4500000008015700056D796B6579000000040000000000",
      message);
}

TEST_F(TcrMessageTest, testConstructorWithGETDURABLECQS_MSG_TYPE) {
  TcrMessageGetDurableCqs message(static_cast<ThinClientBaseDM *>(NULL));

  EXPECT_EQ(TcrMessage::GETDURABLECQS_MSG_TYPE, message.getMessageType());

  EXPECT_MESSAGE_EQ("000000690000000600000001FFFFFFFF00000000010000", message);
}

TEST_F(TcrMessageTest, testConstructor2WithREQUEST) {
  TcrMessageRequest message(
      static_cast<const Region *>(NULL),
      CacheableString::create(
          "mykey"),  // static_cast<const CacheableKeyPtr>(NULLPTR),
      static_cast<const UserDataPtr>(NULLPTR),
      static_cast<ThinClientBaseDM *>(NULL));

  EXPECT_EQ(TcrMessage::REQUEST, message.getMessageType());

  EXPECT_MESSAGE_EQ(
      "000000000000002500000002FFFFFFFF000000001300494E56414C49445F524547494F4E"
      "5F4E414D4500000008015700056D796B6579",
      message);
}

TEST_F(TcrMessageTest, testConstructor2WithDESTROY) {
  TcrMessageDestroy message(static_cast<const Region *>(NULL),
                            CacheableString::create("mykey"),
                            static_cast<const CacheableKeyPtr>(NULLPTR),
                            static_cast<const UserDataPtr>(NULLPTR),
                            static_cast<ThinClientBaseDM *>(NULL));

  EXPECT_EQ(TcrMessage::DESTROY, message.getMessageType());

  EXPECT_MESSAGE_EQ(
      "000000090000004800000005FFFFFFFF000000001300494E56414C49445F524547494F4E"
      "5F4E414D4500000008015700056D796B6579000000010129000000010129000000120003"
      "000000000000000103000000000000000\\h{1}",
      message);
}

TEST_F(TcrMessageTest, testConstructor2WithINVALIDATE) {
  TcrMessageInvalidate message(
      static_cast<const Region *>(NULL),
      CacheableString::create(
          "mykey"),  // static_cast<const CacheableKeyPtr>(NULLPTR),
      static_cast<const UserDataPtr>(NULLPTR),
      static_cast<ThinClientBaseDM *>(NULL));

  EXPECT_EQ(TcrMessage::INVALIDATE, message.getMessageType());

  EXPECT_MESSAGE_EQ(
      "000000530000003C00000003FFFFFFFF000000001300494E56414C49445F524547494F4E"
      "5F4E414D4500000008015700056D796B6579000000120003000000000000000103000000"
      "000000000\\h{1}",
      message);
}

TEST_F(TcrMessageTest, testConstructor3WithPUT) {
  TcrMessagePut message(static_cast<const Region *>(NULL),
                        CacheableString::create("mykey"),
                        CacheableString::create("myvalue"),
                        static_cast<const UserDataPtr>(NULLPTR),
                        false,  // isDelta
                        static_cast<ThinClientBaseDM *>(NULL),
                        false,  // isMetaRegion
                        false,  // fullValueAfterDeltaFail
                        "myRegionName");

  EXPECT_EQ(TcrMessage::PUT, message.getMessageType());

  EXPECT_MESSAGE_EQ(
      "000000070000005A00000007FFFFFFFF000000000C006D79526567696F6E4E616D650000"
      "0001012900000004000000000000000008015700056D796B657900000002013500000000"
      "0A015700076D7976616C7565000000120003000000000000000103000000000000000\\h"
      "{1}",
      message);
}

TEST_F(TcrMessageTest, testConstructor4) {
  TcrMessageReply message(false,  // decodeAll
                          static_cast<ThinClientBaseDM *>(NULL));

  EXPECT_EQ(TcrMessage::INVALID, message.getMessageType());
}

TEST_F(TcrMessageTest, testConstructor5WithREGISTER_INTERST_LIST) {
  VectorOfCacheableKey keys;
  keys.push_back(CacheableString::create("mykey"));
  TcrMessageRegisterInterestList message(
      static_cast<const Region *>(NULL), keys,
      false,  // isDurable
      false,  // isCacheingEnabled
      false,  // receiveValues
      InterestResultPolicy::NONE, static_cast<ThinClientBaseDM *>(NULL));

  EXPECT_EQ(TcrMessage::REGISTER_INTEREST_LIST, message.getMessageType());

  EXPECT_MESSAGE_EQ(
      "000000180000004200000006FFFFFFFF000000001300494E56414C49445F524547494F4E"
      "5F4E414D4500000003010125000000000100000000000A0141015700056D796B65790000"
      "0001000100000002000000",
      message);
}

TEST_F(TcrMessageTest, testConstructor5WithUNREGISTER_INTERST_LIST) {
  VectorOfCacheableKey keys;
  keys.push_back(CacheableString::create("mykey"));
  TcrMessageUnregisterInterestList message(
      static_cast<const Region *>(NULL), keys,
      false,  // isDurable
      false,  // isCacheingEnabled
      false,  // receiveValues
      InterestResultPolicy::NONE, static_cast<ThinClientBaseDM *>(NULL));

  EXPECT_EQ(TcrMessage::UNREGISTER_INTEREST_LIST, message.getMessageType());

  EXPECT_MESSAGE_EQ(
      "000000190000003A00000005FFFFFFFF000000001300494E56414C49445F524547494F4E"
      "5F4E414D4500000001000000000001000000000004000000000100000008015700056D79"
      "6B6579",
      message);
}

TEST_F(TcrMessageTest, testConstructorGET_FUNCTION_ATTRIBUTES) {
  TcrMessageGetFunctionAttributes message(
      std::string("myFunction"), static_cast<ThinClientBaseDM *>(NULL));

  EXPECT_EQ(TcrMessage::GET_FUNCTION_ATTRIBUTES, message.getMessageType());

  EXPECT_MESSAGE_EQ(
      "0000005B0000000F00000001FFFFFFFF000000000A006D7946756E6374696F6E",
      message);
}

TEST_F(TcrMessageTest, testConstructorKEY_SET) {
  TcrMessageKeySet message(std::string("myFunctionKeySet"),
                           static_cast<ThinClientBaseDM *>(NULL));

  EXPECT_EQ(TcrMessage::KEY_SET, message.getMessageType());

  EXPECT_MESSAGE_EQ(
      "000000280000001500000001FFFFFFFF0000000010006D7946756E6374696F6E4B657953"
      "6574",
      message);
}

TEST_F(TcrMessageTest, testConstructor6WithCREATE_REGION) {
  TcrMessageCreateRegion message("str1",  // TODO: what does this parameter do?!
                                 "str2",  // TODO: what does this parameter do?!
                                 InterestResultPolicy::NONE,
                                 false,  // isDurable
                                 false,  // isCacheingEnabled
                                 false,  // receiveValues
                                 static_cast<ThinClientBaseDM *>(NULL));

  EXPECT_EQ(TcrMessage::CREATE_REGION, message.getMessageType());

  EXPECT_MESSAGE_EQ(
      "0000001D0000001200000002FFFFFFFF00000000040073747231000000040073747232",
      message);
}

TEST_F(TcrMessageTest, testConstructor6WithREGISTER_INTEREST) {
  TcrMessageRegisterInterest message(
      "str1",  // TODO: what does this parameter do?!
      "str2",  // TODO: what does this parameter do?!
      InterestResultPolicy::NONE,
      false,  // isDurable
      false,  // isCacheingEnabled
      false,  // receiveValues
      static_cast<ThinClientBaseDM *>(NULL));

  EXPECT_EQ(TcrMessage::REGISTER_INTEREST, message.getMessageType());

  EXPECT_MESSAGE_EQ(
      "000000140000003600000007FFFFFFFF0000000004007374723100000004000000000100"
      "0000030101250000000001000000000004007374723200000001000100000002000000",
      message);
}

TEST_F(TcrMessageTest, testConstructor6WithUNREGISTER_INTEREST) {
  TcrMessageUnregisterInterest message(
      "str1",  // TODO: what does this parameter do?!
      "str2",  // TODO: what does this parameter do?!
      InterestResultPolicy::NONE,
      false,  // isDurable
      false,  // isCacheingEnabled
      false,  // receiveValues
      static_cast<ThinClientBaseDM *>(NULL));

  EXPECT_EQ(TcrMessage::UNREGISTER_INTEREST, message.getMessageType());

  EXPECT_MESSAGE_EQ(
      "000000160000002700000005FFFFFFFF0000000004007374723100000004000000000100"
      "0000040073747232000000010000000000010000",
      message);
}

TEST_F(TcrMessageTest, testConstructorGET_PDX_TYPE_BY_ID) {
  TcrMessageGetPdxTypeById message(42, static_cast<ThinClientBaseDM *>(NULL));

  EXPECT_EQ(TcrMessage::GET_PDX_TYPE_BY_ID, message.getMessageType());

  EXPECT_MESSAGE_EQ("0000005C0000000900000001FFFFFFFF0000000004000000002A",
                    message);
}

TEST_F(TcrMessageTest, testConstructorGET_PDX_ENUM_BY_ID) {
  TcrMessageGetPdxEnumById message(42, static_cast<ThinClientBaseDM *>(NULL));

  EXPECT_EQ(TcrMessage::GET_PDX_ENUM_BY_ID, message.getMessageType());

  EXPECT_MESSAGE_EQ("000000620000000900000001FFFFFFFF0000000004000000002A",
                    message);
}

TEST_F(TcrMessageTest, testConstructorGET_PDX_ID_FOR_TYPE) {
  CacheablePtr myPtr(CacheableString::createDeserializable());
  TcrMessageGetPdxIdForType message(myPtr,
                                    static_cast<ThinClientBaseDM *>(NULL), 42);

  EXPECT_EQ(TcrMessage::GET_PDX_ID_FOR_TYPE, message.getMessageType());

  EXPECT_MESSAGE_EQ("0000005D0000000700000001FFFFFFFF0000000002010000",
                    message);
}

TEST_F(TcrMessageTest, testConstructorADD_PDX_TYPE) {
  CacheablePtr myPtr(CacheableString::createDeserializable());
  TcrMessageAddPdxType message(myPtr, static_cast<ThinClientBaseDM *>(NULL),
                               42);

  EXPECT_EQ(TcrMessage::ADD_PDX_TYPE, message.getMessageType());

  EXPECT_MESSAGE_EQ(
      "0000005E0000001000000002FFFFFFFF000000000201000000000004000000002A",
      message);
}

TEST_F(TcrMessageTest, testConstructorGET_PDX_ID_FOR_ENUM) {
  TcrMessageGetPdxIdForEnum message(static_cast<CacheablePtr>(NULLPTR),
                                    static_cast<ThinClientBaseDM *>(NULL), 42);

  EXPECT_EQ(TcrMessage::GET_PDX_ID_FOR_ENUM, message.getMessageType());

  EXPECT_MESSAGE_EQ("000000610000000600000001FFFFFFFF00000000010129", message);
}

TEST_F(TcrMessageTest, testConstructorADD_PDX_ENUM) {
  CacheablePtr myPtr(CacheableString::createDeserializable());
  TcrMessageAddPdxEnum message(static_cast<CacheablePtr>(NULLPTR),
                               static_cast<ThinClientBaseDM *>(NULL), 42);

  EXPECT_EQ(TcrMessage::ADD_PDX_ENUM, message.getMessageType());

  EXPECT_MESSAGE_EQ(
      "000000600000000F00000002FFFFFFFF0000000001012900000004000000002A",
      message);
}

TEST_F(TcrMessageTest, testConstructorEventId) {
  TcrMessageRequestEventValue message(static_cast<EventIdPtr>(NULLPTR));

  EXPECT_EQ(TcrMessage::REQUEST_EVENT_VALUE, message.getMessageType());

  EXPECT_MESSAGE_EQ("000000440000000600000001FFFFFFFF00000000010129", message);
}

TEST_F(TcrMessageTest, testConstructorREMOVE_USER_AUTH) {
  TcrMessageRemoveUserAuth message(true, static_cast<ThinClientBaseDM *>(NULL));

  EXPECT_EQ(TcrMessage::REMOVE_USER_AUTH, message.getMessageType());

  EXPECT_MESSAGE_EQ("0000004E0000000600000001FFFFFFFF00000000010001", message);

  TcrMessageRemoveUserAuth message2(false,
                                    static_cast<ThinClientBaseDM *>(NULL));

  EXPECT_EQ(TcrMessage::REMOVE_USER_AUTH, message2.getMessageType());

  EXPECT_MESSAGE_EQ("0000004E0000000600000001FFFFFFFF00000000010000", message2);
}

TEST_F(TcrMessageTest, testConstructorUSER_CREDENTIAL_MESSAGE) {
  TcrMessageUserCredential message(static_cast<PropertiesPtr>(NULLPTR),
                                   static_cast<ThinClientBaseDM *>(NULL));

  EXPECT_EQ(TcrMessage::USER_CREDENTIAL_MESSAGE, message.getMessageType());
  // this message is currently blank so this should change it if the impl
  // changes
  EXPECT_MESSAGE_EQ("", message);
}

TEST_F(TcrMessageTest, testConstructorGET_CLIENT_PARTITION_ATTRIBUTES) {
  TcrMessageGetClientPartitionAttributes message("testClientRegion");

  EXPECT_EQ(TcrMessage::GET_CLIENT_PARTITION_ATTRIBUTES,
            message.getMessageType());

  EXPECT_MESSAGE_EQ(
      "000000490000001500000001FFFFFFFF00000000100074657374436C69656E7452656769"
      "6F6E",
      message);
}

TEST_F(TcrMessageTest, testConstructorGET_CLIENT_PR_METADATA) {
  TcrMessageGetClientPrMetadata message("testClientRegionPRMETA");

  EXPECT_EQ(TcrMessage::GET_CLIENT_PR_METADATA, message.getMessageType());

  EXPECT_MESSAGE_EQ(
      "000000470000001B00000001FFFFFFFF00000000160074657374436C69656E7452656769"
      "6F6E50524D455441",
      message);
}
TEST_F(TcrMessageTest, testConstructorSIZE) {
  TcrMessageSize message("testClientRegionSIZE");

  EXPECT_EQ(TcrMessage::SIZE, message.getMessageType());

  EXPECT_MESSAGE_EQ(
      "000000510000001900000001FFFFFFFF00000000140074657374436C69656E7452656769"
      "6F6E53495A45",
      message);
}

TEST_F(TcrMessageTest, testConstructorEXECUTE_REGION_FUNCTION_SINGLE_HOP) {
  const Region *region = NULL;

  CacheableHashSetPtr myHashCachePtr = CacheableHashSet::create();

  CacheablePtr myPtr(CacheableString::createDeserializable());
  TcrMessageExecuteRegionFunctionSingleHop message(
      "myFuncName", region, myPtr, myHashCachePtr, 0, myHashCachePtr,
      false,  // allBuckets
      1, static_cast<ThinClientBaseDM *>(NULL));

  EXPECT_EQ(TcrMessage::EXECUTE_REGION_FUNCTION_SINGLE_HOP,
            message.getMessageType());

  EXPECT_MESSAGE_EQ(
      "0000004F0000005E00000009FFFFFFFF00000000050000000003E80000001300494E5641"
      "4C49445F524547494F4E5F4E414D450000000A006D7946756E634E616D65000000030157"
      "000000000001012900000001000000000004000000000000000004000000000000000002"
      "014200",
      message);
}

TEST_F(TcrMessageTest, testConstructorEXECUTE_REGION_FUNCTION) {
  const Region *region = NULL;

  CacheableHashSetPtr myHashCachePtr = CacheableHashSet::create();
  CacheablePtr myCacheablePtr(CacheableString::createDeserializable());
  CacheableVectorPtr myVectPtr = CacheableVector::create();

  TcrMessageExecuteRegionFunction testMessage(
      "ExecuteRegion", region, myCacheablePtr, myVectPtr, 1, myHashCachePtr, 10,
      static_cast<ThinClientBaseDM *>(NULL), 10);

  EXPECT_EQ(TcrMessage::EXECUTE_REGION_FUNCTION, testMessage.getMessageType());
  // this message is currently blank so this should change it if the impl
  // changes

  EXPECT_MESSAGE_EQ(
      "0000003B0000006100000009FFFFFFFF00000000050001000027100000001300494E5641"
      "4C49445F524547494F4E5F4E414D450000000D0045786563757465526567696F6E000000"
      "030157000000000001012900000001000A00000004000000000000000004000000000000"
      "000002014200",
      testMessage);
}

TEST_F(TcrMessageTest, testConstructorEXECUTE_FUNCTION) {
  CacheablePtr myCacheablePtr(CacheableString::createDeserializable());

  TcrMessageExecuteFunction testMessage("ExecuteFunction", myCacheablePtr, 1,
                                        static_cast<ThinClientBaseDM *>(NULL),
                                        10);

  EXPECT_EQ(TcrMessage::EXECUTE_FUNCTION, testMessage.getMessageType());

  EXPECT_TRUE(testMessage.hasResult());

  EXPECT_MESSAGE_EQ(
      "0000003E0000002600000003FFFFFFFF00000000050001000027100000000F0045786563"
      "75746546756E6374696F6E0000000301570000",
      testMessage);
}
