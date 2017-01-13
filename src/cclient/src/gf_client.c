#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <errno.h>
#include<pthread.h>
#include "data_io.h"
#include "gf_client.h"


#define RECIEVE_DATA(context, request, reply, len, resultcode) \
    recieveData(context, &reply, len, resultcode);\
if(*resultcode != NO_ERROR)\
{\
    clearBuf(&request);\
    clearBuf(&reply);\
    return;\
}

#define SEND_DATA(context, request, reply, resultcode) \
    sendData(context, getBuffer(&request), getBufferLength(&request), resultcode);\
if(*resultcode != NO_ERROR)\
{\
    clearBuf(&request);\
    clearBuf(&reply);\
    return;\
}


#define CLIENT_TO_SERVER 100
#define REPLY_OK          59
#define SECURITY_CREDENTIALS_NONE      0
#define VERSION_ORDINAL_651   7 // since NC 3510
#define CONFLATEBYTE 0
#define ADDRSIZE 4
#define DCPORT 12334
#define VMKIND 13
#define ROLEARRLENGTH 0
#define REGION_NAME "/ROOT-REGION"
#define TIMEOUT 5
#define RAND_STRING_LEN 10
const int32_t HEADER_LENGTH = 17;
static int32_t synch_counter = 2;
//static int32_t transactioID = 0;


#define MAXBUF          1024

enum TypeIdValues {
    // Do not use IDs 5 and 6 which are used by .NET
    // ManagedObject and ManagedObjectXml. If those are
    // required then change those in GemfireTypeIdsM.hpp

    // keep the following in alphabetical order please.
    Properties = 11,
    BooleanArray = 26,
    CharArray = 27,
    RegionAttributes = 30, // because there's no equivalence in java
    CacheableUndefined = 31,
    Struct = 32,
    NullObj = 41,
    CacheableString = 42,
    CacheableBytes = 46,
    CacheableInt16Array = 47,
    CacheableInt32Array = 48,
    CacheableInt64Array = 49,
    CacheableFloatArray = 50,
    CacheableDoubleArray = 51,
    CacheableObjectArray = 52,
    CacheableBoolean = 53,
    CacheableWideChar = 54,
    CacheableByte = 55,
    CacheableInt16 = 56,
    CacheableInt32 = 57,
    CacheableInt64 = 58,
    CacheableFloat = 59,
    CacheableDouble = 60,
    CacheableDate = 61,
    CacheableFileName = 63,
    CacheableStringArray = 64,
    CacheableArrayList = 65,
    CacheableHashSet = 66,
    CacheableHashMap = 67,
    CacheableTimeUnit = 68,
    CacheableNullString = 69,
    CacheableHashTable = 70,
    CacheableVector = 71,
    CacheableIdentityHashMap = 72,
    CacheableLinkedHashSet = 73,
    CacheableStack = 74,
    CacheableASCIIString = 87,
    CacheableASCIIStringHuge = 88,
    CacheableStringHuge = 89
};
enum IdValues {
    // keep the following in alphabetical order please.
    ObjectTypeImpl = -61,
    StructTypeImpl = -60,
    CollectionTypeImpl = -59,
    FixedIDDefault = 0,
    FixedIDByte = 1,
    FixedIDShort = 2,
    FixedIDInt = 3,
    FixedIDNone = 4,
    CacheableToken = 14, // because there's no equivalence in java
    CacheableObjectPartList = 25,
    EventId = 36,
    InterestResultPolicy = 37,
    ClientProxyMembershipId = 38,
    CacheableUserData4 = 37,
    CacheableUserData2 = 38,
    CacheableUserData = 39,
    CacheableUserClass = 40,
    Class = 43,
    JavaSerializable = 44,
    DataSerializable = 45,
    InternalDistributedMember = 92,
    EntryEventImpl = 105,
    RegionEventImpl = 108,
    ClientHealthStats = -126,
    GatewayEventCallbackArgument = -56, // 0xC8
    ClientConnectionRequest = -53,
    ClientConnectionResponse = -50,
    QueueConnectionRequest = -52,
    QueueConnectionResponse = -49,
    LocatorListRequest = -54,
    LocatorListResponse = -51,
    GetAllServersRequest = -43,
    GetAllServersResponse = -42,
    ClientReplacementRequest= -48
};
typedef enum {
    /* Server couldn't read message; handle it like a server side
       exception that needs retries */
    INVALID = -1,
    REQUEST = 0,
    RESPONSE /* 1 */,
    EXCEPTION /* 2 */,
    REQUEST_DATA_ERROR /* 3 */,
    DATA_NOT_FOUND_ERROR /* 4 Not in use */,
    PING /* 5 */,
    REPLY /* 6 */,
    PUT /* 7 */,
    PUT_DATA_ERROR /* 8 */,
    DESTROY /* 9 */,
    DESTROY_DATA_ERROR /* 10 */,
    DESTROY_REGION /* 11 */,
    DESTROY_REGION_DATA_ERROR /* 12 */,
    CLIENT_NOTIFICATION /* 13 */,
    UPDATE_CLIENT_NOTIFICATION /* 14 */,
    LOCAL_INVALIDATE /* 15 */,
    LOCAL_DESTROY /* 16 */,
    LOCAL_DESTROY_REGION /* 17 */,
    CLOSE_CONNECTION /* 18 */,
    PROCESS_BATCH /* 19 */,
    REGISTER_INTEREST /* 20 */,
    REGISTER_INTEREST_DATA_ERROR /* 21 */,
    UNREGISTER_INTEREST /* 22 */,
    UNREGISTER_INTEREST_DATA_ERROR /* 23 */,
    REGISTER_INTEREST_LIST /* 24 */,
    UNREGISTER_INTEREST_LIST /* 25 */,
    UNKNOWN_MESSAGE_TYPE_ERROR /* 26 */,
    LOCAL_CREATE /* 27 */,
    LOCAL_UPDATE /* 28 */,
    CREATE_REGION /* 29 */,
    CREATE_REGION_DATA_ERROR /* 30 */,
    MAKE_PRIMARY /* 31 */,
    RESPONSE_FROM_PRIMARY /* 32 */,
    RESPONSE_FROM_SECONDARY /* 33 */,
    QUERY /* 34 */,
    QUERY_DATA_ERROR /* 35 */,
    CLEAR_REGION /* 36 */,
    CLEAR_REGION_DATA_ERROR /* 37 */,
    CONTAINS_KEY /* 38 */,
    CONTAINS_KEY_DATA_ERROR /* 39 */,
    KEY_SET /* 40 */,
    KEY_SET_DATA_ERROR /* 41 */,
    EXECUTECQ_MSG_TYPE /* 42 */,
    EXECUTECQ_WITH_IR_MSG_TYPE /*43 */,
    STOPCQ_MSG_TYPE /*44*/,
    CLOSECQ_MSG_TYPE /*45 */,
    CLOSECLIENTCQS_MSG_TYPE /*46*/,
    CQDATAERROR_MSG_TYPE /*47 */,
    GETCQSTATS_MSG_TYPE /*48 */,
    MONITORCQ_MSG_TYPE /*49 */,
    CQ_EXCEPTION_TYPE /*50 */,
    REGISTER_INSTANTIATORS = 51 /* 51 */,
    PERIODIC_ACK = 52 /* 52 */,
    CLIENT_READY /* 53 */,
    CLIENT_MARKER /* 54 */,
    INVALIDATE_REGION /* 55 */,
    PUTALL /* 56 */,
    GET_ALL = 57 /* 57 */,
    GET_ALL_DATA_ERROR /* 58 */,
    EXECUTE_REGION_FUNCTION = 59 /* 59 */,
    EXECUTE_REGION_FUNCTION_RESULT /* 60 */,
    EXECUTE_REGION_FUNCTION_ERROR /* 61 */,
    EXECUTE_FUNCTION /* 62 */,
    EXECUTE_FUNCTION_RESULT /* 63 */,
    EXECUTE_FUNCTION_ERROR /* 64 */,
    CLIENT_REGISTER_INTEREST = 65 /* 65 */,
    CLIENT_UNREGISTER_INTEREST = 66,
    REGISTER_DATASERIALIZERS = 67,
    REQUEST_EVENT_VALUE = 68,
    REQUEST_EVENT_VALUE_ERROR = 69, /*69*/
    PUT_DELTA_ERROR = 70, /*70*/
    GET_CLIENT_PR_METADATA = 71, /*71*/
    RESPONSE_CLIENT_PR_METADATA = 72, /*72*/
    GET_CLIENT_PARTITION_ATTRIBUTES = 73, /*73*/
    RESPONSE_CLIENT_PARTITION_ATTRIBUTES =74, /*74*/
    GET_CLIENT_PR_METADATA_ERROR = 75, /*75*/
    GET_CLIENT_PARTITION_ATTRIBUTES_ERROR = 76, /*76*/
    USER_CREDENTIAL_MESSAGE = 77,
    REMOVE_USER_AUTH = 78, 
    QUERY_WITH_PARAMETERS = 80

} MsgType;
/*
int32_t incTransactionID()
{
    return 1;//transactioID++;
}
*/
int64_t getAndIncEid(CONTEXT* context)
{
    return context->eidSeq++;
}

void printData(uint8_t* buf, int32_t len)
{
    int32_t i;
    printf("\n");
    for(i = 0; i < len; i++)
    {
        printf("%d ", buf[i]);
    }
    printf("\n");
}

void recieveData(CONTEXT* context, Buffer* buf, uint32_t len, int32_t* resultcode)
{
    time_t startTime = time(NULL); 
    uint32_t origLen = len;

    while(len > 0 && (time(NULL) - startTime) < TIMEOUT )
    {
        int8_t buffer[MAXBUF];
        bzero(buffer, MAXBUF);
        int32_t recLen = recv(context->sockfd, buffer, len > MAXBUF?MAXBUF:len, 0);
        if(recLen > 0)
        {
            len -= recLen;
            writeBytesOnly(buf, buffer, recLen);
        } else if(recLen == 0)
        {
            *resultcode = CONNECTION_ERROR;
            //printf("closed connection");
            return;
        }
    }

    //printf("recieve");
    //printData(getBuffer(buf), getBufferLength(buf));

    if(len == 0)
        rewindCursor(buf, origLen);
    *resultcode =  (len > 0)?CONNECTION_ERROR:NO_ERROR;
}



void sendData(CONTEXT* context, uint8_t* buf, int32_t len, int32_t* resultcode)
{
    time_t startTime = time(NULL); 
    uint32_t sentLen = 0;
    //printf("send[%d]", len);
    //printData(buf, len);
    while(len > 0 && (time(NULL) - startTime) < TIMEOUT )
    {
        int32_t sendLen = send(context->sockfd, (buf + sentLen), len, 0);
        //printf("sent %d bytes\n", sendLen);
        if(sendLen > 0)
        {
            len -= sendLen;
            sentLen += sendLen;
        } else if(sendLen == 0)
        {
            *resultcode = CONNECTION_ERROR;
            //printf("closed connection");
            return;
        }

    }

    *resultcode = (len > 0)?CONNECTION_ERROR:NO_ERROR;
}

CONTEXT* createContext(char* host, char* port)
{

    struct addrinfo hints;
    struct addrinfo *result, *rp;
    int sfd, s;

    /* Obtain address(es) matching host/port */

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = 0;
    hints.ai_protocol = 0;

    s = getaddrinfo(host, port, &hints, &result);
    if (s != 0) {
        return NULL;
    }

    /* getaddrinfo() returns a list of address structures.
       Try each address until we successfully connect(2).
       If socket(2) (or connect(2)) fails, we (close the socket
       and) try the next address. */

    for (rp = result; rp != NULL; rp = rp->ai_next) {
        sfd = socket(rp->ai_family, rp->ai_socktype,
                rp->ai_protocol);
        struct timeval tv;
        int32_t timeout=1000;

        tv.tv_sec = timeout / 1000 ;
        tv.tv_usec = ( timeout % 1000) * 1000  ;

        setsockopt (sfd, SOL_SOCKET, SO_RCVTIMEO, (char 
                    *)&tv, sizeof tv);
        if (sfd == -1)
            continue;

        if (connect(sfd, rp->ai_addr, rp->ai_addrlen) != -1)
            break;                  /* Success */

        close(sfd);
    }

    if (rp == NULL) {               /* No address succeeded */
        //fprintf(stderr, "Could not connect\n");
        //exit(EXIT_FAILURE);
        return NULL;
    }

    CONTEXT* context = (CONTEXT*)malloc(sizeof(CONTEXT));
    context->sockfd = sfd;
    context->eidSeq = 0;

    return context;
}

void createRandString(char *randString)
{
    const char selectChars[] =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";
    const uint32_t numChars = (sizeof(selectChars) / sizeof(char)) - 1;
    strcpy(randString, "GFNative_");
    uint32_t startIndex = strlen(randString);
    uint32_t seed = getpid() + time(NULL);
    srand(seed);
    uint32_t index;
    for (index = 0; index < RAND_STRING_LEN; ++index) {
        randString[startIndex + index] = selectChars[rand() % numChars];
    }
    randString[startIndex + RAND_STRING_LEN] = '\0';
}

void writeClientProxyMembershipId(Buffer* buf)
{
    struct hostent *host;     /* host information */
    //struct in_addr h_addr;    /* internet address */
    char hostName[1024];
    gethostname(hostName, 1023);
    host = gethostbyname(hostName);
    char randString[1024];
    createRandString(randString);

    Buffer memId;
    initBuf(&memId);

    writeByte(&memId, (int8_t)FixedIDByte);
    writeByte(&memId, (int8_t)InternalDistributedMember);
    writeArrayLen(&memId, ADDRSIZE);
    writeInt(&memId, (int32_t)(*(host->h_addr_list[0])));
    //m_memID.writeInt((int32_t)hostPort);
    writeInt(&memId, (int32_t)synch_counter);
    writeByte(&memId, (int8_t)CacheableASCIIString);
    writeASCII(&memId, host->h_name  );
    writeByte(&memId, (int8_t)0); // splitbrain flags

    writeInt(&memId, (int32_t)DCPORT);

    writeInt(&memId, (int32_t)getpid());
    writeByte(&memId, (int8_t)VMKIND);
    writeArrayLen(&memId, ROLEARRLENGTH);
    writeByte(&memId, (int8_t)CacheableASCIIString);
    writeASCII(&memId, "default_GemfireDS");
    writeByte(&memId, (int8_t)CacheableASCIIString);
    writeASCII(&memId, randString);
    writeByte(&memId, (int8_t)CacheableASCIIString);
    writeASCII(&memId, "");
    writeInt(&memId, (int32_t)300);
    writeUnsignedBytes(buf, getBuffer(&memId), getBufferLength(&memId));


    clearBuf(&memId);
}

void doHandshake(CONTEXT* context, int32_t* resultcode)
{
    Buffer request;
    initBuf(&request);
    Buffer reply;
    initBuf(&reply);
    *resultcode = NO_ERROR;

    writeByte(&request, (int8_t)CLIENT_TO_SERVER );
    writeByte(&request, (int8_t)VERSION_ORDINAL_651);
    writeByte(&request, (int8_t)REPLY_OK);
    writeInt(&request, (int32_t)0x7fffffff - 10000 );
    // Write header for byte FixedID since GFE 5.7
    writeByte(&request, (int8_t)FixedIDByte);
    //Writing byte for ClientProxyMembershipID class id=38 as registered on the java server.
    writeByte(&request, (int8_t)ClientProxyMembershipId);
    /* calc memId */
    writeClientProxyMembershipId(&request);
    // writeBytes(&request, (int8_t *)memIdBuffer, memIdBufferLength);
    writeInt(&request,(int32_t)1);
    writeByte(&request, (int8_t)CONFLATEBYTE);
    writeUnsigned(&request, (uint8_t)SECURITY_CREDENTIALS_NONE);

    SEND_DATA(context, request, reply, resultcode)

        //sendData(context, getBuffer(&buf), getBufferLength(&buf));
        /*---Get "Hello?"---*/
        //int recLen = 1;
        //    recieveData(context, &reply, 7);
        RECIEVE_DATA(context, request, reply, 7, resultcode)

        int8_t acceptance_code;
    int8_t serverQueueStatus;
    int8_t recvMsgLenByte;
    int32_t queueSize;
    readByte(&reply, &acceptance_code);
    readByte(&reply, &serverQueueStatus);
    readInt(&reply, &queueSize);
    readByte(&reply, &recvMsgLenByte);
    int32_t recvMsgLen = recvMsgLenByte;
    if (recvMsgLen== -2) {
        int16_t recvMsgLenShort = 0;
        //        recieveData(context, &reply, 2);
        RECIEVE_DATA(context, request, reply, 2, resultcode)
            readShort(&reply, &recvMsgLenShort);
        recvMsgLen = recvMsgLenShort;
    }
    else if (recvMsgLen == -3) {
        //        recieveData(context, &reply, 4);
        RECIEVE_DATA(context, request, reply, 4, resultcode)
            readInt(&reply, &recvMsgLen);
    }
    //recieveData(context, &reply, recvMsgLen);
    RECIEVE_DATA(context, request, reply, recvMsgLen, resultcode)
        advanceCursor(&reply, recvMsgLen);
    uint16_t recvMsgLen2 = 0;
    //recieveData(context, &reply, 2);
    RECIEVE_DATA(context, request, reply,2, resultcode)
        readUnsignedShort(&reply, &recvMsgLen2);
    //recieveData(context, &reply, recvMsgLen2);
    RECIEVE_DATA(context, request, reply, recvMsgLen2, resultcode)
        advanceCursor(&reply, recvMsgLen2);
    int8_t isDeltaEnabledOnServer;
    //recieveData(context, &reply, 1);
    RECIEVE_DATA(context, request, reply, 1, resultcode)
        readByte(&reply, &isDeltaEnabledOnServer);
    if(acceptance_code != REPLY_OK)
    {
        *resultcode = HANDSHAKE_ERROR;
    }
    clearBuf(&request);
    clearBuf(&reply);
}


void gf_write_header(Buffer* buf, uint32_t msgType, uint32_t numOfParts)
{

    writeInt(buf, (int32_t)msgType);
    writeInt(buf, (int32_t)0); // write a dummy message len('0' here). At the end write the length at the (buffer + 4) offset.
    writeInt(buf, (int32_t)numOfParts);
    writeInt(buf, (int32_t)1);
    writeByte(buf, (int8_t)0x0);
}

void write_region_part(Buffer* buf, char* regionName)
{
    int32_t len = strlen(regionName);
    writeInt(buf, len);
    writeByte(buf, (int8_t)0); // isObject = 0
    writeBytesOnly(buf, (int8_t *)regionName, len);
}

void write_null_object(Buffer* buf)
{
    //write size
    writeInt(buf, (int32_t)1);
    //write isobject
    writeByte(buf, (int8_t)1);
    //write actual object
    writeByte(buf, (int8_t)NullObj);
}

void write_int_part(Buffer* buf,  int32_t intValue)
{
    writeInt(buf, (int32_t) 4);
    writeByte(buf, (int8_t) 0);
    writeInt(buf, intValue);
}

void write_string_part(Buffer* buf, const char* strValue)
{
    //write size
    writeInt(buf, (int32_t)0);
    //write isobject
    writeByte(buf, (int8_t)1);
    int32_t before = getBufferLength(buf);
    writeByte(buf, (int8_t)CacheableASCIIString);
    writeASCII(buf, strValue);
    int32_t after = getBufferLength(buf);
    int32_t sizeOfObj = after - before;
    rewindCursor(buf, sizeOfObj + 1 + 4);
    writeInt(buf, sizeOfObj);
    advanceCursor(buf, sizeOfObj + 1);
}

void write_bool_part(Buffer* buf, int8_t boolValue)
{
    //write size
    writeInt(buf, (int32_t)2);
    //write isobject
    writeByte(buf, (int8_t)1);
    writeByte(buf, (int8_t)CacheableBoolean);
    writeByte(buf, (int8_t)boolValue);
}

void write_bytes_part(Buffer* buf, const int8_t* bytes, int32_t len)
{
    //write size
    writeInt(buf, (int32_t)1);
    //write isobject
    writeByte(buf, (int8_t)0);
    int32_t before = getBufferLength(buf);
    writeBytesOnly(buf, bytes, len);
    int32_t after = getBufferLength(buf);
    int32_t sizeOfObj = after - before;
    rewindCursor(buf, sizeOfObj + 1 + 4);
    writeInt(buf, sizeOfObj);
    advanceCursor(buf, sizeOfObj + 1);
}

void write_id_part(CONTEXT* context, Buffer* buf)
{
    //  Write EventId threadid and seqno.
    int32_t idsBufferLength = 18;
    writeInt(buf, idsBufferLength);
    writeUnsigned(buf, (uint8_t) 0);
    char longCode = 3;
    writeUnsigned(buf, (uint8_t) longCode);
    writeLong(buf, pthread_self());
    writeUnsigned(buf, (uint8_t) longCode);
    writeLong(buf, getAndIncEid(context));
}

void write_message_length(Buffer* buf)
{
    uint32_t totalLen = getBufferLength(buf);
    uint32_t g_headerLen = 17;
    uint32_t msgLen = totalLen - g_headerLen;
    //printf("msglen: %d\n", msgLen);
    rewindCursor(buf, totalLen - 4); // msg len is written after the msg type which is of 4 bytes ...
    writeInt(buf, (int32_t)msgLen);
    advanceCursor(buf, totalLen - 8); // after writing 4 bytes for msg len you are already 8 bytes ahead from the beginning.
}

void gf_put(CONTEXT * context, const char * uuid, const int8_t * data, int32_t len, int32_t * resultcode)
{
    Buffer request;
    initBuf(&request);
    Buffer reply;
    initBuf(&reply);
    *resultcode = NO_ERROR;

    gf_write_header(&request, PUT, 7);
    write_region_part(&request, REGION_NAME);
    write_null_object(&request);
    write_int_part(&request, 0);
    write_string_part(&request, uuid);
    write_bool_part(&request, 0);
    write_bytes_part(&request, data, len);
    write_id_part(context, &request);
    write_message_length(&request);

    SEND_DATA(context, request, reply, resultcode)
        // int err = sendData(context, getBuffer(&buf), getBufferLength(&buf));
        /*---Get "Hello?"---*/
        //int recLen = 1;

        RECIEVE_DATA(context, request, reply, HEADER_LENGTH, resultcode)
        //err = recieveData(context, &reply, HEADER_LENGTH);
        //printf("put error: %d\n", *resultcode);
    int32_t msgType;
    int32_t msgLen;

    readInt(&reply, &msgType);
    readInt(&reply, &msgLen);
    advanceCursor(&reply, HEADER_LENGTH - 8);

    RECIEVE_DATA(context, request, reply, msgLen, resultcode)
        if(msgType != REPLY)
        {
            *resultcode = OPERATION_ERROR;
            clearBuf(&request);
            clearBuf(&reply);
        }
    //err = recieveData(context, &reply, msgLen);
}

void gf_get(CONTEXT * context, const char * uuid, int8_t* data, uint32_t len, int32_t * resultcode)
{
    Buffer request;
    initBuf(&request);
    Buffer reply;
    initBuf(&reply);
    *resultcode = NO_ERROR;

    gf_write_header(&request, REQUEST, 2);
    write_region_part(&request, REGION_NAME);
    write_string_part(&request, uuid);
    write_message_length(&request);

    SEND_DATA(context, request, reply, resultcode)

        //int err = sendData(context, getBuffer(&buf), getBufferLength(&buf));

        RECIEVE_DATA(context, request, reply, HEADER_LENGTH, resultcode)
        //err = recieveData(context, &reply, HEADER_LENGTH);
        int32_t msgType;
    int32_t msgLen;

    readInt(&reply, &msgType);
    readInt(&reply, &msgLen);
    advanceCursor(&reply, HEADER_LENGTH - 8);

    //err = recieveData(context, &reply, msgLen);
    RECIEVE_DATA(context, request, reply, msgLen, resultcode)
        if(msgType != RESPONSE)
        {
            *resultcode = OPERATION_ERROR;
            clearBuf(&request);
            clearBuf(&reply);
            return;
        }
    uint32_t dataLen;
    readUnsignedInt(&reply, &dataLen);
    int8_t isObj;
    readByte(&reply, &isObj);
    memcpy(data, getCursor(&reply), dataLen);
}

void gf_destroy(CONTEXT * context, const char * uuid, int32_t * resultcode)
{
    Buffer request;
    initBuf(&request);
    Buffer reply;
    initBuf(&reply);
    *resultcode = NO_ERROR;

    gf_write_header(&request, DESTROY, 5);
    write_region_part(&request, REGION_NAME);
    write_string_part(&request, uuid);
    write_null_object(&request);
    write_null_object(&request);
    write_id_part(context, &request);
    write_message_length(&request);

    SEND_DATA(context, request, reply, resultcode)

        //    int err = sendData(context, getBuffer(&buf), getBufferLength(&buf));
        /*---Get "Hello?"---*/
        //int recLen = 1;

        RECIEVE_DATA(context, request, reply, HEADER_LENGTH, resultcode)

        //    err = recieveData(context, &reply, HEADER_LENGTH);
        //printf("destroy error: %d\n", *resultcode);
    int32_t msgType;
    int32_t msgLen;

    readInt(&reply, &msgType);
    readInt(&reply, &msgLen);
    advanceCursor(&reply, HEADER_LENGTH - 8);

    //    err = recieveData(context, &reply, msgLen);
    RECIEVE_DATA(context, request, reply, msgLen, resultcode)
        if(msgType != REPLY)
        {
            *resultcode = OPERATION_ERROR;
            clearBuf(&request);
            clearBuf(&reply);
        }
}

void gf_ping(CONTEXT* context, int32_t* resultcode)
{
    Buffer request;
    initBuf(&request);
    Buffer reply;
    initBuf(&reply);
    *resultcode = NO_ERROR;

    writeInt(&request, (int32_t)PING);
    writeInt(&request, (int32_t)0);// 17 is fixed message len ...  PING only has a header.
    writeInt(&request, (int32_t)0);// Number of parts.
    writeInt(&request, (int32_t)0);
    writeByte(&request, (int8_t)0);// Early ack is '0'.
    SEND_DATA(context, request, reply, resultcode)
        RECIEVE_DATA(context, request, reply, HEADER_LENGTH, resultcode)

        int32_t msgType;
    int32_t msgLen;

    readInt(&reply, &msgType);
    readInt(&reply, &msgLen);
    advanceCursor(&reply, HEADER_LENGTH - 8);

    //    err = recieveData(context, &reply, msgLen);
    RECIEVE_DATA(context, request, reply, msgLen, resultcode)
        if(msgType != REPLY)
        {
            *resultcode = OPERATION_ERROR;
            clearBuf(&request);
            clearBuf(&reply);
        }
}

void gf_disconnect(CONTEXT * context, int32_t * resultcode)
{
    Buffer request;
    initBuf(&request);
    Buffer reply;
    initBuf(&reply);
    *resultcode = NO_ERROR;

    writeInt(&request, (int32_t)CLOSE_CONNECTION);
    writeInt(&request,(int32_t)6);
    writeInt(&request,(int32_t)1);// Number of parts.
    //int32_t txId = TcrMessage::m_transactionId++;
    writeInt(&request,(int32_t)0);
    writeByte(&request, (int8_t)0);// Early ack is '0'.
    // last two parts are not used ... setting zero in both the parts.
    writeInt(&request,(int32_t)1); // len is 1
    writeByte(&request,(int8_t)0);// is obj is '0'.
    // cast away constness here since we want to modify this
    writeByte(&request,(int8_t)0);// keepalive is '0'. 
    SEND_DATA(context, request, reply, resultcode)

        int32_t error = close(context->sockfd);
    if(error == 0) {
        *resultcode = NO_ERROR;
    } else {
        *resultcode = CONNECTION_ERROR;
    }

    free(context);
}

CONTEXT* gf_connect(char* host, char* port, int32_t* resultcode)
{
    CONTEXT* context = createContext(host, port);

    if(context == NULL)
    {
        *resultcode = CONNECTION_ERROR;
        //printf("CONNECTION ERROR");
    } else {
        doHandshake(context, resultcode);
        if(*resultcode != NO_ERROR)
        {
            int32_t error;
            //printf("HANDSHAKE ERROR");
            *resultcode = HANDSHAKE_ERROR;
            gf_disconnect(context, &error);
            context = NULL;
        }
    }

    return context;
}

