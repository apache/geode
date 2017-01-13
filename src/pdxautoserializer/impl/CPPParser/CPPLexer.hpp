/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef INC_CPPLexer_hpp_
#define INC_CPPLexer_hpp_

#include <antlr/config.hpp>
/* $ANTLR 2.7.7 (20130409): "CPP_parser.g" -> "CPPLexer.hpp"$ */
#include <antlr/CommonToken.hpp>
#include <antlr/InputBuffer.hpp>
#include <antlr/BitSet.hpp>
#include "STDCTokenTypes.hpp"
#include <antlr/CharScanner.hpp>
#line 79 "CPP_parser.g"

// File generated from CPP_parser.g
// Version 3.1 November 2005
// This file is best viewed in courier font with tabs set to 4 spaces
//
// The statements in this block appear in both CPPLexer.hpp and CPPParser.hpp
#include <stdio.h>
#include <string.h>
#include "antlr/CharScanner.hpp"
#include "CPPDictionary.hpp"

// Defined in main.cpp
extern void process_line_directive(const char*, const char*);

// Declared in main.cpp
extern int deferredLineCount;

#line 30 "CPPLexer.hpp"
class CUSTOM_API CPPLexer : public ANTLR_USE_NAMESPACE(antlr) CharScanner,
                            public STDCTokenTypes {
#line 2303 "CPP_parser.g"

  void deferredNewline() {
    deferredLineCount++;  // Declared and initialised in main.cpp
  }

  void newline() {
    for (; deferredLineCount > 0; deferredLineCount--) {
      CharScanner::newline();
    }
    CharScanner::newline();
  }
#line 34 "CPPLexer.hpp"
 private:
  void initLiterals();

 public:
  bool getCaseSensitiveLiterals() const { return true; }

 public:
  CPPLexer(ANTLR_USE_NAMESPACE(std) istream& in);
  CPPLexer(ANTLR_USE_NAMESPACE(antlr) InputBuffer& ib);
  CPPLexer(const ANTLR_USE_NAMESPACE(antlr) LexerSharedInputState& state);
  ANTLR_USE_NAMESPACE(antlr) RefToken nextToken();

 public:
  void mASSIGNEQUAL(bool _createToken);

 public:
  void mCOLON(bool _createToken);

 public:
  void mCOMMA(bool _createToken);

 public:
  void mQUESTIONMARK(bool _createToken);

 public:
  void mSEMICOLON(bool _createToken);

 public:
  void mPOINTERTO(bool _createToken);

 public:
  void mLPAREN(bool _createToken);

 public:
  void mRPAREN(bool _createToken);

 public:
  void mLSQUARE(bool _createToken);

 public:
  void mRSQUARE(bool _createToken);

 public:
  void mLCURLY(bool _createToken);

 public:
  void mRCURLY(bool _createToken);

 public:
  void mEQUAL(bool _createToken);

 public:
  void mNOTEQUAL(bool _createToken);

 public:
  void mLESSTHANOREQUALTO(bool _createToken);

 public:
  void mLESSTHAN(bool _createToken);

 public:
  void mGREATERTHANOREQUALTO(bool _createToken);

 public:
  void mGREATERTHAN(bool _createToken);

 public:
  void mDIVIDE(bool _createToken);

 public:
  void mDIVIDEEQUAL(bool _createToken);

 public:
  void mPLUS(bool _createToken);

 public:
  void mPLUSEQUAL(bool _createToken);

 public:
  void mPLUSPLUS(bool _createToken);

 public:
  void mMINUS(bool _createToken);

 public:
  void mMINUSEQUAL(bool _createToken);

 public:
  void mMINUSMINUS(bool _createToken);

 public:
  void mSTAR(bool _createToken);

 public:
  void mTIMESEQUAL(bool _createToken);

 public:
  void mMOD(bool _createToken);

 public:
  void mMODEQUAL(bool _createToken);

 public:
  void mSHIFTRIGHT(bool _createToken);

 public:
  void mSHIFTRIGHTEQUAL(bool _createToken);

 public:
  void mSHIFTLEFT(bool _createToken);

 public:
  void mSHIFTLEFTEQUAL(bool _createToken);

 public:
  void mAND(bool _createToken);

 public:
  void mNOT(bool _createToken);

 public:
  void mOR(bool _createToken);

 public:
  void mAMPERSAND(bool _createToken);

 public:
  void mBITWISEANDEQUAL(bool _createToken);

 public:
  void mTILDE(bool _createToken);

 public:
  void mBITWISEOR(bool _createToken);

 public:
  void mBITWISEOREQUAL(bool _createToken);

 public:
  void mBITWISEXOR(bool _createToken);

 public:
  void mBITWISEXOREQUAL(bool _createToken);

 public:
  void mPOINTERTOMBR(bool _createToken);

 public:
  void mDOTMBR(bool _createToken);

 public:
  void mSCOPE(bool _createToken);

 public:
  void mWhitespace(bool _createToken);

 public:
  void mComment(bool _createToken);

 protected:
  void mEndOfLine(bool _createToken);

 public:
  void mCPPComment(bool _createToken);

 public:
  void mPREPROC_DIRECTIVE(bool _createToken);

 protected:
  void mLineDirective(bool _createToken);

 protected:
  void mSpace(bool _createToken);

 protected:
  void mDecimal(bool _createToken);

 public:
  void mStringLiteral(bool _createToken);

 protected:
  void mPragma(bool _createToken);

 protected:
  void mError(bool _createToken);

 public:
  void mCharLiteral(bool _createToken);

 protected:
  void mEscape(bool _createToken);

 public:
  void mWCharLiteral(bool _createToken);

 public:
  void mWStringLiteral(bool _createToken);

 protected:
  void mDigit(bool _createToken);

 protected:
  void mLongSuffix(bool _createToken);

 protected:
  void mUnsignedSuffix(bool _createToken);

 protected:
  void mFloatSuffix(bool _createToken);

 protected:
  void mExponent(bool _createToken);

 protected:
  void mVocabulary(bool _createToken);

 public:
  void mNumber(bool _createToken);

 public:
  void mID(bool _createToken);

 private:
  static const unsigned long _tokenSet_0_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_0;
  static const unsigned long _tokenSet_1_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_1;
  static const unsigned long _tokenSet_2_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_2;
  static const unsigned long _tokenSet_3_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_3;
  static const unsigned long _tokenSet_4_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_4;
  static const unsigned long _tokenSet_5_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_5;
  static const unsigned long _tokenSet_6_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_6;
  static const unsigned long _tokenSet_7_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_7;
  static const unsigned long _tokenSet_8_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_8;
};

#endif /*INC_CPPLexer_hpp_*/
