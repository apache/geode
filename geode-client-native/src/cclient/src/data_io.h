//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

#ifndef __C_GEMFIRE_DATAOUTPUT_H__
#define __C_GEMFIRE_DATAOUTPUT_H__

#include <string.h>
#include <stdlib.h>
#include <stdint.h>

/**
 * @file
 */

/**
 * C style memory allocation that throws OutOfMemoryException
 * if it fails
 */
#define GF_ALLOC(v,t,s) \
{ \
    v = (t*)malloc((s) * sizeof(t)); \
}

/**
 * C style memory re-allocation that throws OutOfMemoryException
 * if it fails
 */
#define GF_RESIZE(v,t,s) \
{ \
    v = (t*)realloc(v, (s) * sizeof(t)); \
}

#define GF_FREE(v) free(v)

typedef struct {
  // memory m_buffer to encode to.
  uint8_t* m_bytes;
  // cursor.
  uint8_t* m_buf;
  // size of m_bytes.
  uint32_t m_size;
} Buffer;

inline void writeUnsigned(Buffer* buf, uint8_t value);
inline void writeByte(Buffer* buf, int8_t value);
inline void writeUnsignedBytes(Buffer* buf,  const uint8_t* bytes, int32_t len );
inline void writeBytes(Buffer* buf,  const int8_t* bytes, int32_t len );
inline void writeBytesOnly(Buffer* buf,  const int8_t* bytes, int32_t len );
inline void writeUnsignedInt( Buffer* buf,uint32_t value );
inline void writeInt(Buffer* buf,  int32_t value );
inline void writeUnsignedLong( Buffer* buf,uint64_t value );
inline void writeLong(Buffer* buf,  int64_t value );
inline void writeUnsignedShort( Buffer* buf,uint16_t value );
inline void writeShort(Buffer* buf,  int16_t value );
inline void writeArrayLen(Buffer* buf,  int32_t len );
inline void writeASCII(Buffer* buf, const char* value);
inline void writeNoCheck(Buffer* buf, uint8_t value);

inline void readByte(Buffer* buf, int8_t* value );
inline void readShort(Buffer* buf, int16_t* value );
inline void readUnsignedShort(Buffer* buf, uint16_t* value );
inline void readInt(Buffer* buf, int32_t* value );
inline void readUnsignedInt(Buffer* buf, uint32_t* value );

void advanceCursor(Buffer* buf, uint32_t offset);
void rewindCursor(Buffer* buf, uint32_t offset);

inline void initBuf(Buffer* buf);
inline void clearBuf(Buffer* buf);
inline uint32_t getBufferLength(Buffer* buf) ;
inline uint8_t* getBuffer(Buffer* buf) ;
inline uint8_t* getCursor(Buffer* buf) ;

inline void ensureCapacity(Buffer* buf,  uint32_t size );

#endif // __C_GEMFIRE_DATAOUTPUT_H__
