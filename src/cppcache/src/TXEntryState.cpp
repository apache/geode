/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * TXEntryState.cpp
 *
 *  Created on: 16-Feb-2011
 *      Author: ankurs
 */

#include "TXEntryState.hpp"
#include <gfcpp/ExceptionTypes.hpp>

namespace gemfire {

TXEntryState::TXEntryState()
    : /* adongre
       * CID 28946: Uninitialized scalar field (UNINIT_CTOR)
       */
      // UNUSED m_modSerialNum(0),
      m_op(OP_NULL)
// UNUSED , m_bulkOp(false)
{}

TXEntryState::~TXEntryState() {}

int8_t TXEntryState::adviseOp(int8_t requestedOpCode) {
  int8_t advisedOpCode = OP_NULL;
  // Determine new operation based on
  // the requested operation 'requestedOpCode' and
  // the previous operation 'm_op'
  switch (requestedOpCode) {
    case OP_L_DESTROY:
      switch (m_op) {
        case OP_NULL:
          advisedOpCode = requestedOpCode;
          break;
        case OP_L_DESTROY:
        case OP_CREATE_LD:
        case OP_LLOAD_CREATE_LD:
        case OP_NLOAD_CREATE_LD:
        case OP_PUT_LD:
        case OP_LLOAD_PUT_LD:
        case OP_NLOAD_PUT_LD:
        case OP_D_INVALIDATE_LD:
        case OP_D_DESTROY:
          GfErrTypeThrowException(
              "Unexpected current op  {0}  for requested op  {1}",
              GF_CACHE_ILLEGAL_STATE_EXCEPTION);
          // not expected to be reached
          break;
        case OP_L_INVALIDATE:
          advisedOpCode = requestedOpCode;
          break;
        case OP_PUT_LI:
          advisedOpCode = OP_PUT_LD;
          break;
        case OP_LLOAD_PUT_LI:
          advisedOpCode = OP_LLOAD_PUT_LD;
          break;
        case OP_NLOAD_PUT_LI:
          advisedOpCode = OP_NLOAD_PUT_LD;
          break;
        case OP_D_INVALIDATE:
          advisedOpCode = OP_D_INVALIDATE_LD;
          break;
        case OP_CREATE_LI:
          advisedOpCode = OP_CREATE_LD;
          break;
        case OP_LLOAD_CREATE_LI:
          advisedOpCode = OP_LLOAD_CREATE_LD;
          break;
        case OP_NLOAD_CREATE_LI:
          advisedOpCode = OP_NLOAD_CREATE_LD;
          break;
        case OP_CREATE:
          advisedOpCode = OP_CREATE_LD;
          break;
        case OP_SEARCH_CREATE:
          advisedOpCode = requestedOpCode;
          break;
        case OP_LLOAD_CREATE:
          advisedOpCode = OP_LLOAD_CREATE_LD;
          break;
        case OP_NLOAD_CREATE:
          advisedOpCode = OP_NLOAD_CREATE_LD;
          break;
        case OP_LOCAL_CREATE:
          advisedOpCode = requestedOpCode;
          break;
        case OP_PUT:
          advisedOpCode = OP_PUT_LD;
          break;
        case OP_SEARCH_PUT:
          advisedOpCode = requestedOpCode;
          break;
        case OP_LLOAD_PUT:
          advisedOpCode = OP_LLOAD_PUT_LD;
          break;
        case OP_NLOAD_PUT:
          advisedOpCode = OP_NLOAD_PUT_LD;
          break;
        default:
          GfErrTypeThrowException("Unhandled  {0}",
                                  GF_CACHE_ILLEGAL_STATE_EXCEPTION);
      }
      break;
    case OP_D_DESTROY:
      // Assert.assertTrue(!isOpDestroy(),
      //                  "Transactional destroy assertion op=" + m_op);
      advisedOpCode = requestedOpCode;
      break;
    case OP_L_INVALIDATE:
      switch (m_op) {
        case OP_NULL:
          advisedOpCode = requestedOpCode;
          break;
        case OP_L_DESTROY:
        case OP_CREATE_LD:
        case OP_LLOAD_CREATE_LD:
        case OP_NLOAD_CREATE_LD:
        case OP_PUT_LD:
        case OP_LLOAD_PUT_LD:
        case OP_NLOAD_PUT_LD:
        case OP_D_DESTROY:
        case OP_D_INVALIDATE_LD:
          GfErrTypeThrowException(
              "Unexpected current op  {0}  for requested op  {1}",
              GF_CACHE_ILLEGAL_STATE_EXCEPTION);
          // not expected to be reached
          break;
        case OP_L_INVALIDATE:
          advisedOpCode = requestedOpCode;
          break;
        case OP_LLOAD_PUT_LI:
        case OP_NLOAD_PUT_LI:
        case OP_LLOAD_CREATE_LI:
        case OP_NLOAD_CREATE_LI:
          advisedOpCode = m_op;
          break;
        case OP_PUT_LI:
          advisedOpCode = OP_PUT_LI;
          break;
        case OP_CREATE_LI:
          advisedOpCode = OP_CREATE_LI;
          break;
        case OP_D_INVALIDATE:
          advisedOpCode = OP_D_INVALIDATE;
          break;
        case OP_CREATE:
          advisedOpCode = OP_CREATE_LI;
          break;
        case OP_SEARCH_CREATE:
          advisedOpCode = OP_LOCAL_CREATE;
          // pendingValue will be set to LOCAL_INVALID
          break;
        case OP_LLOAD_CREATE:
          advisedOpCode = OP_LLOAD_CREATE_LI;
          break;
        case OP_NLOAD_CREATE:
          advisedOpCode = OP_NLOAD_CREATE_LI;
          break;
        case OP_LOCAL_CREATE:
          advisedOpCode = OP_LOCAL_CREATE;
          break;
        case OP_PUT:
          advisedOpCode = OP_PUT_LI;
          break;
        case OP_SEARCH_PUT:
          advisedOpCode = requestedOpCode;
          break;
        case OP_LLOAD_PUT:
          advisedOpCode = OP_LLOAD_PUT_LI;
          break;
        case OP_NLOAD_PUT:
          advisedOpCode = OP_NLOAD_PUT_LI;
          break;
        default:
          GfErrTypeThrowException("Unhandled  {0}",
                                  GF_CACHE_ILLEGAL_STATE_EXCEPTION);
      }
      break;
    case OP_D_INVALIDATE:
      switch (m_op) {
        case OP_NULL:
          advisedOpCode = requestedOpCode;
          break;
        case OP_L_DESTROY:
        case OP_CREATE_LD:
        case OP_LLOAD_CREATE_LD:
        case OP_NLOAD_CREATE_LD:
        case OP_PUT_LD:
        case OP_LLOAD_PUT_LD:
        case OP_NLOAD_PUT_LD:
        case OP_D_INVALIDATE_LD:
        case OP_D_DESTROY:
          GfErrTypeThrowException(
              "Unexpected current op  {0}  for requested op  {1}",
              GF_CACHE_ILLEGAL_STATE_EXCEPTION);
          // not expected to be reached
          break;
        case OP_D_INVALIDATE:
        case OP_L_INVALIDATE:
          advisedOpCode = OP_D_INVALIDATE;
          break;

        case OP_PUT_LI:
        case OP_LLOAD_PUT_LI:
        case OP_NLOAD_PUT_LI:
        case OP_CREATE_LI:
        case OP_LLOAD_CREATE_LI:
        case OP_NLOAD_CREATE_LI:
          /*
           * No change, keep it how it was.
           */
          advisedOpCode = m_op;
          break;
        case OP_CREATE:
          advisedOpCode = OP_CREATE;
          // pendingValue will be set to INVALID turning it into create invalid
          break;
        case OP_SEARCH_CREATE:
          advisedOpCode = OP_LOCAL_CREATE;
          // pendingValue will be set to INVALID to indicate dinvalidate
          break;
        case OP_LLOAD_CREATE:
          advisedOpCode = OP_CREATE;
          // pendingValue will be set to INVALID turning it into create invalid
          break;
        case OP_NLOAD_CREATE:
          advisedOpCode = OP_CREATE;
          // pendingValue will be set to INVALID turning it into create invalid
          break;
        case OP_LOCAL_CREATE:
          advisedOpCode = OP_LOCAL_CREATE;
          // pendingValue will be set to INVALID to indicate dinvalidate
          break;
        case OP_PUT:
        case OP_SEARCH_PUT:
        case OP_LLOAD_PUT:
        case OP_NLOAD_PUT:
          advisedOpCode = requestedOpCode;
          break;
        default:
          GfErrTypeThrowException("Unhandled  {0}",
                                  GF_CACHE_ILLEGAL_STATE_EXCEPTION);
      }
      break;
    case OP_CREATE:
    case OP_SEARCH_CREATE:
    case OP_LLOAD_CREATE:
    case OP_NLOAD_CREATE:
      advisedOpCode = requestedOpCode;
      break;
    case OP_PUT:
      switch (m_op) {
        case OP_CREATE:
        case OP_SEARCH_CREATE:
        case OP_LLOAD_CREATE:
        case OP_NLOAD_CREATE:
        case OP_LOCAL_CREATE:
        case OP_CREATE_LI:
        case OP_LLOAD_CREATE_LI:
        case OP_NLOAD_CREATE_LI:
        case OP_CREATE_LD:
        case OP_LLOAD_CREATE_LD:
        case OP_NLOAD_CREATE_LD:
        case OP_PUT_LD:
        case OP_LLOAD_PUT_LD:
        case OP_NLOAD_PUT_LD:
        case OP_D_INVALIDATE_LD:
        case OP_L_DESTROY:
        case OP_D_DESTROY:
          advisedOpCode = OP_CREATE;
          break;
        default:
          advisedOpCode = requestedOpCode;
          break;
      }
      break;
    case OP_SEARCH_PUT:
      switch (m_op) {
        case OP_NULL:
          advisedOpCode = requestedOpCode;
          break;
        case OP_L_INVALIDATE:
          advisedOpCode = requestedOpCode;
          break;
        // The incoming search put value should match
        // the pendingValue from the previous tx operation.
        // So it is ok to simply drop the _LI from the op
        case OP_PUT_LI:
          advisedOpCode = OP_PUT;
          break;
        case OP_LLOAD_PUT_LI:
          advisedOpCode = OP_LLOAD_PUT;
          break;
        case OP_NLOAD_PUT_LI:
          advisedOpCode = OP_NLOAD_PUT;
          break;
        case OP_CREATE_LI:
          advisedOpCode = OP_CREATE;
          break;
        case OP_LLOAD_CREATE_LI:
          advisedOpCode = OP_LLOAD_CREATE;
          break;
        case OP_NLOAD_CREATE_LI:
          advisedOpCode = OP_NLOAD_CREATE;
          break;
        default:
          // Note that OP_LOCAL_CREATE and OP_CREATE with invalid values
          // are not possible because they would cause the netsearch to
          // fail and we would do a load or a total miss.
          // Note that OP_D_INVALIDATE followed by OP_SEARCH_PUT is not
          // possible since the netsearch will alwsys "miss" in this case.
          GfErrTypeThrowException(
              "Unexpected current op  {0}  for requested op  {1}",
              GF_CACHE_ILLEGAL_STATE_EXCEPTION);
      }
      break;
    case OP_LLOAD_PUT:
    case OP_NLOAD_PUT:
      switch (m_op) {
        case OP_NULL:
        case OP_L_INVALIDATE:
        case OP_PUT_LI:
        case OP_LLOAD_PUT_LI:
        case OP_NLOAD_PUT_LI:
        case OP_D_INVALIDATE:
          advisedOpCode = requestedOpCode;
          break;
        case OP_CREATE:
        case OP_LOCAL_CREATE:
        case OP_CREATE_LI:
        case OP_LLOAD_CREATE_LI:
        case OP_NLOAD_CREATE_LI:
          if (requestedOpCode == OP_LLOAD_PUT) {
            advisedOpCode = OP_LLOAD_CREATE;
          } else {
            advisedOpCode = OP_NLOAD_CREATE;
          }
          break;
        default:
          // note that other invalid states are covered by this default
          // case because they should have caused a OP_SEARCH_PUT
          // to be requested.
          GfErrTypeThrowException(
              "Unexpected current op  {0}  for requested op  {1}",
              GF_CACHE_ILLEGAL_STATE_EXCEPTION);
      }
      break;
    default:
      GfErrTypeThrowException("OpCode  {0}  should never be requested",
                              GF_CACHE_ILLEGAL_STATE_EXCEPTION);
  }
  return advisedOpCode;
}
}  // namespace gemfire
