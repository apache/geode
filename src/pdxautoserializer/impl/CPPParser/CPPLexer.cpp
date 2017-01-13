/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/* $ANTLR 2.7.7 (20130409): "CPP_parser.g" -> "CPPLexer.cpp"$ */
#ifndef _WIN32
#include <stdio.h>
#include <strings.h>
#endif
#include "CPPLexer.hpp"
#include <antlr/CharBuffer.hpp>
#include <antlr/TokenStreamException.hpp>
#include <antlr/TokenStreamIOException.hpp>
#include <antlr/TokenStreamRecognitionException.hpp>
#include <antlr/CharStreamException.hpp>
#include <antlr/CharStreamIOException.hpp>
#include <antlr/NoViableAltForCharException.hpp>

#line 1 "CPP_parser.g"
#line 13 "CPPLexer.cpp"
CPPLexer::CPPLexer(ANTLR_USE_NAMESPACE(std) istream& in)
    : ANTLR_USE_NAMESPACE(antlr)
          CharScanner(new ANTLR_USE_NAMESPACE(antlr) CharBuffer(in), true) {
  initLiterals();
}

CPPLexer::CPPLexer(ANTLR_USE_NAMESPACE(antlr) InputBuffer& ib)
    : ANTLR_USE_NAMESPACE(antlr) CharScanner(ib, true) {
  initLiterals();
}

CPPLexer::CPPLexer(const ANTLR_USE_NAMESPACE(antlr)
                       LexerSharedInputState& state)
    : ANTLR_USE_NAMESPACE(antlr) CharScanner(state, true) {
  initLiterals();
}

void CPPLexer::initLiterals() {
  literals["_cdecl"] = 164;
  literals["__stdcall"] = 26;
  literals["extern"] = 20;
  literals["public"] = 82;
  literals["namespace"] = 14;
  literals["case"] = 98;
  literals["short"] = 48;
  literals["break"] = 108;
  literals["while"] = 103;
  literals["__int8"] = 51;
  literals["delete"] = 174;
  literals["new"] = 163;
  literals["__declspec"] = 75;
  literals["uint32_t"] = 64;
  literals["int16_t"] = 55;
  literals["_int32"] = 56;
  literals["GFID"] = 29;
  literals["GFARRAYSIZE"] = 31;
  literals["__pascal"] = 173;
  literals["template"] = 4;
  literals["GFEXCLUDE"] = 27;
  literals["reinterpret_cast"] = 152;
  literals["inline"] = 12;
  literals["unsigned"] = 70;
  literals["const"] = 76;
  literals["float"] = 71;
  literals["_int8"] = 50;
  literals["__int16"] = 54;
  literals["static_cast"] = 151;
  literals["return"] = 109;
  literals["throw"] = 96;
  literals["int64_t"] = 61;
  literals["typename"] = 44;
  literals["using"] = 97;
  literals["operator"] = 85;
  literals["__far"] = 169;
  literals["sizeof"] = 145;
  literals["protected"] = 83;
  literals["class"] = 24;
  literals["_stdcall"] = 25;
  literals["friend"] = 13;
  literals["do"] = 104;
  literals["__interrupt"] = 170;
  literals["_far"] = 168;
  literals["typeid"] = 154;
  literals["__volatile__"] = 79;
  literals["__alignof__"] = 146;
  literals["__cdecl"] = 165;
  literals["_int64"] = 59;
  literals["__asm"] = 114;
  literals["pascal"] = 171;
  literals["typedef"] = 7;
  literals["__const"] = 77;
  literals["const_cast"] = 153;
  literals["__asm__"] = 115;
  literals["uint16_t"] = 63;
  literals["explicit"] = 43;
  literals["_asm"] = 113;
  literals["if"] = 100;
  literals["__int32"] = 57;
  literals["double"] = 72;
  literals["volatile"] = 78;
  literals["catch"] = 111;
  literals["dynamic_cast"] = 150;
  literals["union"] = 23;
  literals["try"] = 110;
  literals["register"] = 37;
  literals["_inline"] = 40;
  literals["auto"] = 36;
  literals["GFINCLUDE"] = 28;
  literals["goto"] = 106;
  literals["enum"] = 9;
  literals["int"] = 49;
  literals["for"] = 105;
  literals["int32_t"] = 58;
  literals["uint64_t"] = 65;
  literals["char"] = 45;
  literals["__near"] = 167;
  literals["private"] = 84;
  literals["GFARRAYELEMSIZE"] = 35;
  literals["default"] = 99;
  literals["false"] = 88;
  literals["this"] = 86;
  literals["static"] = 38;
  literals["mutable"] = 39;
  literals["int8_t"] = 52;
  literals["GFARRAYSIZES"] = 34;
  literals["uint8_t"] = 62;
  literals["_int16"] = 53;
  literals["continue"] = 107;
  literals["bool"] = 47;
  literals["struct"] = 22;
  literals["GFUNREAD"] = 30;
  literals["_near"] = 166;
  literals["__int64"] = 60;
  literals["signed"] = 69;
  literals["GFIGNORE"] = 80;
  literals["else"] = 101;
  literals["_declspec"] = 74;
  literals["_pascal"] = 172;
  literals["__w64"] = 67;
  literals["antlrTrace_on"] = 15;
  literals["void"] = 73;
  literals["antlrTrace_off"] = 16;
  literals["wchar_t"] = 46;
  literals["switch"] = 102;
  literals["__inline"] = 41;
  literals["true"] = 87;
  literals["long"] = 68;
  literals["asm"] = 112;
  literals["virtual"] = 42;
  literals["_w64"] = 66;
}

ANTLR_USE_NAMESPACE(antlr) RefToken CPPLexer::nextToken() {
  ANTLR_USE_NAMESPACE(antlr) RefToken theRetToken;
  for (;;) {
    ANTLR_USE_NAMESPACE(antlr) RefToken theRetToken;
    int _ttype = ANTLR_USE_NAMESPACE(antlr) Token::INVALID_TYPE;
    resetText();
    try {  // for lexical and char stream error handling
      switch (LA(1)) {
        case 0x2c /* ',' */: {
          mCOMMA(true);
          theRetToken = _returnToken;
          break;
        }
        case 0x3f /* '?' */: {
          mQUESTIONMARK(true);
          theRetToken = _returnToken;
          break;
        }
        case 0x3b /* ';' */: {
          mSEMICOLON(true);
          theRetToken = _returnToken;
          break;
        }
        case 0x28 /* '(' */: {
          mLPAREN(true);
          theRetToken = _returnToken;
          break;
        }
        case 0x29 /* ')' */: {
          mRPAREN(true);
          theRetToken = _returnToken;
          break;
        }
        case 0x5b /* '[' */: {
          mLSQUARE(true);
          theRetToken = _returnToken;
          break;
        }
        case 0x5d /* ']' */: {
          mRSQUARE(true);
          theRetToken = _returnToken;
          break;
        }
        case 0x7b /* '{' */: {
          mLCURLY(true);
          theRetToken = _returnToken;
          break;
        }
        case 0x7d /* '}' */: {
          mRCURLY(true);
          theRetToken = _returnToken;
          break;
        }
        case 0x7e /* '~' */: {
          mTILDE(true);
          theRetToken = _returnToken;
          break;
        }
        case 0x9 /* '\t' */:
        case 0xa /* '\n' */:
        case 0xc /* '\14' */:
        case 0xd /* '\r' */:
        case 0x20 /* ' ' */:
        case 0x5c /* '\\' */: {
          mWhitespace(true);
          theRetToken = _returnToken;
          break;
        }
        case 0x23 /* '#' */: {
          mPREPROC_DIRECTIVE(true);
          theRetToken = _returnToken;
          break;
        }
        case 0x22 /* '\"' */: {
          mStringLiteral(true);
          theRetToken = _returnToken;
          break;
        }
        case 0x27 /* '\'' */: {
          mCharLiteral(true);
          theRetToken = _returnToken;
          break;
        }
        default:
          if ((LA(1) == 0x3e /* '>' */) && (LA(2) == 0x3e /* '>' */) &&
              (LA(3) == 0x3d /* '=' */)) {
            mSHIFTRIGHTEQUAL(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x3c /* '<' */) && (LA(2) == 0x3c /* '<' */) &&
                     (LA(3) == 0x3d /* '=' */)) {
            mSHIFTLEFTEQUAL(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x2d /* '-' */) && (LA(2) == 0x3e /* '>' */) &&
                     (LA(3) == 0x2a /* '*' */)) {
            mPOINTERTOMBR(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x2d /* '-' */) && (LA(2) == 0x3e /* '>' */) &&
                     (true)) {
            mPOINTERTO(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x3d /* '=' */) && (LA(2) == 0x3d /* '=' */)) {
            mEQUAL(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x21 /* '!' */) && (LA(2) == 0x3d /* '=' */)) {
            mNOTEQUAL(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x3c /* '<' */) && (LA(2) == 0x3d /* '=' */)) {
            mLESSTHANOREQUALTO(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x3e /* '>' */) && (LA(2) == 0x3d /* '=' */)) {
            mGREATERTHANOREQUALTO(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x2f /* '/' */) && (LA(2) == 0x3d /* '=' */)) {
            mDIVIDEEQUAL(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x2b /* '+' */) && (LA(2) == 0x3d /* '=' */)) {
            mPLUSEQUAL(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x2b /* '+' */) && (LA(2) == 0x2b /* '+' */)) {
            mPLUSPLUS(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x2d /* '-' */) && (LA(2) == 0x3d /* '=' */)) {
            mMINUSEQUAL(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x2d /* '-' */) && (LA(2) == 0x2d /* '-' */)) {
            mMINUSMINUS(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x2a /* '*' */) && (LA(2) == 0x3d /* '=' */)) {
            mTIMESEQUAL(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x25 /* '%' */) && (LA(2) == 0x3d /* '=' */)) {
            mMODEQUAL(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x3e /* '>' */) && (LA(2) == 0x3e /* '>' */) &&
                     (true)) {
            mSHIFTRIGHT(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x3c /* '<' */) && (LA(2) == 0x3c /* '<' */) &&
                     (true)) {
            mSHIFTLEFT(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x26 /* '&' */) && (LA(2) == 0x26 /* '&' */)) {
            mAND(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x7c /* '|' */) && (LA(2) == 0x7c /* '|' */)) {
            mOR(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x26 /* '&' */) && (LA(2) == 0x3d /* '=' */)) {
            mBITWISEANDEQUAL(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x7c /* '|' */) && (LA(2) == 0x3d /* '=' */)) {
            mBITWISEOREQUAL(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x5e /* '^' */) && (LA(2) == 0x3d /* '=' */)) {
            mBITWISEXOREQUAL(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x2e /* '.' */) && (LA(2) == 0x2a /* '*' */)) {
            mDOTMBR(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x3a /* ':' */) && (LA(2) == 0x3a /* ':' */)) {
            mSCOPE(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x2f /* '/' */) && (LA(2) == 0x2a /* '*' */)) {
            mComment(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x2f /* '/' */) && (LA(2) == 0x2f /* '/' */)) {
            mCPPComment(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x4c /* 'L' */) && (LA(2) == 0x27 /* '\'' */)) {
            mWCharLiteral(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x4c /* 'L' */) && (LA(2) == 0x22 /* '\"' */)) {
            mWStringLiteral(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x3d /* '=' */) && (true)) {
            mASSIGNEQUAL(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x3a /* ':' */) && (true)) {
            mCOLON(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x3c /* '<' */) && (true)) {
            mLESSTHAN(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x3e /* '>' */) && (true)) {
            mGREATERTHAN(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x2f /* '/' */) && (true)) {
            mDIVIDE(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x2b /* '+' */) && (true)) {
            mPLUS(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x2d /* '-' */) && (true)) {
            mMINUS(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x2a /* '*' */) && (true)) {
            mSTAR(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x25 /* '%' */) && (true)) {
            mMOD(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x21 /* '!' */) && (true)) {
            mNOT(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x26 /* '&' */) && (true)) {
            mAMPERSAND(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x7c /* '|' */) && (true)) {
            mBITWISEOR(true);
            theRetToken = _returnToken;
          } else if ((LA(1) == 0x5e /* '^' */) && (true)) {
            mBITWISEXOR(true);
            theRetToken = _returnToken;
          } else if ((_tokenSet_0.member(LA(1))) && (true)) {
            mNumber(true);
            theRetToken = _returnToken;
          } else if ((_tokenSet_1.member(LA(1))) && (true)) {
            mID(true);
            theRetToken = _returnToken;
          } else {
            if (LA(1) == EOF_CHAR) {
              uponEOF();
              _returnToken =
                  makeToken(ANTLR_USE_NAMESPACE(antlr) Token::EOF_TYPE);
            } else {
              throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
                  LA(1), getFilename(), getLine(), getColumn());
            }
          }
      }
      if (!_returnToken) goto tryAgain;  // found SKIP token

      _ttype = _returnToken->getType();
      _ttype = testLiteralsTable(_ttype);
      _returnToken->setType(_ttype);
      return _returnToken;
    } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& e) {
      throw ANTLR_USE_NAMESPACE(antlr) TokenStreamRecognitionException(e);
    } catch (ANTLR_USE_NAMESPACE(antlr) CharStreamIOException& csie) {
      throw ANTLR_USE_NAMESPACE(antlr) TokenStreamIOException(csie.io);
    } catch (ANTLR_USE_NAMESPACE(antlr) CharStreamException& cse) {
      throw ANTLR_USE_NAMESPACE(antlr) TokenStreamException(cse.getMessage());
    }
  tryAgain:;
  }
}

void CPPLexer::mASSIGNEQUAL(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = ASSIGNEQUAL;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match('=' /* charlit */);
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mCOLON(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = COLON;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match(':' /* charlit */);
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mCOMMA(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = COMMA;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match(',' /* charlit */);
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mQUESTIONMARK(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = QUESTIONMARK;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match('?' /* charlit */);
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mSEMICOLON(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = SEMICOLON;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match(';' /* charlit */);
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mPOINTERTO(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = POINTERTO;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match("->");
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mLPAREN(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = LPAREN;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match('(' /* charlit */);
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mRPAREN(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = RPAREN;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match(')' /* charlit */);
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mLSQUARE(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = LSQUARE;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match('[' /* charlit */);
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mRSQUARE(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = RSQUARE;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match(']' /* charlit */);
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mLCURLY(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = LCURLY;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match('{' /* charlit */);
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mRCURLY(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = RCURLY;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match('}' /* charlit */);
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mEQUAL(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = EQUAL;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match("==");
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mNOTEQUAL(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = NOTEQUAL;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match("!=");
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mLESSTHANOREQUALTO(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = LESSTHANOREQUALTO;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match("<=");
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mLESSTHAN(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = LESSTHAN;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match("<");
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mGREATERTHANOREQUALTO(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = GREATERTHANOREQUALTO;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match(">=");
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mGREATERTHAN(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = GREATERTHAN;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match(">");
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mDIVIDE(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = DIVIDE;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match('/' /* charlit */);
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mDIVIDEEQUAL(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = DIVIDEEQUAL;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match("/=");
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mPLUS(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = PLUS;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match('+' /* charlit */);
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mPLUSEQUAL(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = PLUSEQUAL;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match("+=");
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mPLUSPLUS(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = PLUSPLUS;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match("++");
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mMINUS(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = MINUS;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match('-' /* charlit */);
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mMINUSEQUAL(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = MINUSEQUAL;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match("-=");
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mMINUSMINUS(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = MINUSMINUS;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match("--");
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mSTAR(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = STAR;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match('*' /* charlit */);
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mTIMESEQUAL(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = TIMESEQUAL;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match("*=");
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mMOD(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = MOD;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match('%' /* charlit */);
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mMODEQUAL(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = MODEQUAL;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match("%=");
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mSHIFTRIGHT(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = SHIFTRIGHT;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match(">>");
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mSHIFTRIGHTEQUAL(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = SHIFTRIGHTEQUAL;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match(">>=");
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mSHIFTLEFT(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = SHIFTLEFT;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match("<<");
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mSHIFTLEFTEQUAL(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = SHIFTLEFTEQUAL;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match("<<=");
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mAND(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = AND;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match("&&");
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mNOT(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = NOT;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match('!' /* charlit */);
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mOR(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = OR;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match("||");
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mAMPERSAND(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = AMPERSAND;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match('&' /* charlit */);
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mBITWISEANDEQUAL(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = BITWISEANDEQUAL;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match("&=");
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mTILDE(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = TILDE;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match('~' /* charlit */);
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mBITWISEOR(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = BITWISEOR;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match('|' /* charlit */);
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mBITWISEOREQUAL(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = BITWISEOREQUAL;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match("|=");
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mBITWISEXOR(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = BITWISEXOR;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match('^' /* charlit */);
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mBITWISEXOREQUAL(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = BITWISEXOREQUAL;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match("^=");
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mPOINTERTOMBR(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = POINTERTOMBR;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match("->*");
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mDOTMBR(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = DOTMBR;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match(".*");
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mSCOPE(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = SCOPE;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match("::");
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mWhitespace(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = Whitespace;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  {
    switch (LA(1)) {
      case 0x9 /* '\t' */:
      case 0xc /* '\14' */:
      case 0x20 /* ' ' */: {
        {
          switch (LA(1)) {
            case 0x20 /* ' ' */: {
              match(' ' /* charlit */);
              break;
            }
            case 0x9 /* '\t' */: {
              match('\t' /* charlit */);
              break;
            }
            case 0xc /* '\14' */: {
              match('\14' /* charlit */);
              break;
            }
            default: {
              throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
                  LA(1), getFilename(), getLine(), getColumn());
            }
          }
        }
        break;
      }
      case 0xa /* '\n' */:
      case 0xd /* '\r' */: {
        {
          if ((LA(1) == 0xd /* '\r' */) && (LA(2) == 0xa /* '\n' */)) {
            match('\r' /* charlit */);
            match('\n' /* charlit */);
          } else if ((LA(1) == 0xd /* '\r' */) && (true)) {
            match('\r' /* charlit */);
          } else if ((LA(1) == 0xa /* '\n' */)) {
            match('\n' /* charlit */);
          } else {
            throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
                LA(1), getFilename(), getLine(), getColumn());
          }
        }
        if (inputState->guessing == 0) {
#line 2391 "CPP_parser.g"
          newline();
#line 1165 "CPPLexer.cpp"
        }
        break;
      }
      case 0x5c /* '\\' */: {
        {
          if ((LA(1) == 0x5c /* '\\' */) && (LA(2) == 0xd /* '\r' */) &&
              (LA(3) == 0xa /* '\n' */)) {
            match('\\' /* charlit */);
            match('\r' /* charlit */);
            match('\n' /* charlit */);
          } else if ((LA(1) == 0x5c /* '\\' */) && (LA(2) == 0xd /* '\r' */) &&
                     (true)) {
            match('\\' /* charlit */);
            match('\r' /* charlit */);
          } else if ((LA(1) == 0x5c /* '\\' */) && (LA(2) == 0xa /* '\n' */)) {
            match('\\' /* charlit */);
            match('\n' /* charlit */);
          } else {
            throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
                LA(1), getFilename(), getLine(), getColumn());
          }
        }
        if (inputState->guessing == 0) {
#line 2396 "CPP_parser.g"
          printf("CPP_parser.g continuation line detected\n");
          deferredNewline();
#line 1194 "CPPLexer.cpp"
        }
        break;
      }
      default: {
        throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
            LA(1), getFilename(), getLine(), getColumn());
      }
    }
  }
  if (inputState->guessing == 0) {
#line 2399 "CPP_parser.g"
    _ttype = ANTLR_USE_NAMESPACE(antlr) Token::SKIP;
#line 1207 "CPPLexer.cpp"
  }
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mComment(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = Comment;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match("/*");
  {  // ( ... )*
    for (;;) {
      if (((LA(1) == 0x2a /* '*' */) &&
           ((LA(2) >= 0x0 /* '\0' */ && LA(2) <= 0xff)) &&
           ((LA(3) >= 0x0 /* '\0' */ && LA(3) <= 0xff))) &&
          (LA(2) != '/')) {
        match('*' /* charlit */);
      } else if ((LA(1) == 0xa /* '\n' */ || LA(1) == 0xd /* '\r' */)) {
        mEndOfLine(false);
        if (inputState->guessing == 0) {
#line 2406 "CPP_parser.g"
          deferredNewline();
#line 1233 "CPPLexer.cpp"
        }
      } else if ((_tokenSet_2.member(LA(1)))) {
        { match(_tokenSet_2); }
      } else {
        goto _loop653;
      }
    }
  _loop653:;
  }  // ( ... )*
  match("*/");
  if (inputState->guessing == 0) {
#line 2409 "CPP_parser.g"
    _ttype = ANTLR_USE_NAMESPACE(antlr) Token::SKIP;
#line 1252 "CPPLexer.cpp"
  }
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mEndOfLine(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = EndOfLine;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  {
    if ((LA(1) == 0xd /* '\r' */) && (LA(2) == 0xa /* '\n' */) && (true)) {
      match("\r\n");
    } else if ((LA(1) == 0xd /* '\r' */) && (true) && (true)) {
      match('\r' /* charlit */);
    } else if ((LA(1) == 0xa /* '\n' */)) {
      match('\n' /* charlit */);
    } else {
      throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
          LA(1), getFilename(), getLine(), getColumn());
    }
  }
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mCPPComment(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = CPPComment;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match("//");
  {  // ( ... )*
    for (;;) {
      if ((_tokenSet_3.member(LA(1)))) {
        { match(_tokenSet_3); }
      } else {
        goto _loop657;
      }
    }
  _loop657:;
  }  // ( ... )*
  mEndOfLine(false);
  if (inputState->guessing == 0) {
#line 2415 "CPP_parser.g"
    _ttype = ANTLR_USE_NAMESPACE(antlr) Token::SKIP;
    newline();
#line 1314 "CPPLexer.cpp"
  }
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mPREPROC_DIRECTIVE(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = PREPROC_DIRECTIVE;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  {
    match('#' /* charlit */);
    {  // ( ... )*
      for (;;) {
        if ((_tokenSet_3.member(LA(1)))) {
          { match(_tokenSet_3); }
        } else {
          goto _loop662;
        }
      }
    _loop662:;
    }  // ( ... )*
    mEndOfLine(false);
  }
  if (inputState->guessing == 0) {
#line 2422 "CPP_parser.g"
    _ttype = ANTLR_USE_NAMESPACE(antlr) Token::SKIP;
    newline();
#line 1350 "CPPLexer.cpp"
  }
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mLineDirective(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = LineDirective;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;
  ANTLR_USE_NAMESPACE(antlr) RefToken n;
  ANTLR_USE_NAMESPACE(antlr) RefToken sl;

  {
    switch (LA(1)) {
      case 0x6c /* 'l' */: {
        match("line");
        break;
      }
      case 0x9 /* '\t' */:
      case 0xc /* '\14' */:
      case 0x20 /* ' ' */: {
        break;
      }
      default: {
        throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
            LA(1), getFilename(), getLine(), getColumn());
      }
    }
  }
  {  // ( ... )+
    int _cnt666 = 0;
    for (;;) {
      if ((LA(1) == 0x9 /* '\t' */ || LA(1) == 0xc /* '\14' */ ||
           LA(1) == 0x20 /* ' ' */)) {
        mSpace(false);
      } else {
        if (_cnt666 >= 1) {
          goto _loop666;
        } else {
          throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
              LA(1), getFilename(), getLine(), getColumn());
        }
      }

      _cnt666++;
    }
  _loop666:;
  }  // ( ... )+
  mDecimal(true);
  n = _returnToken;
  {  // ( ... )+
    int _cnt668 = 0;
    for (;;) {
      if ((LA(1) == 0x9 /* '\t' */ || LA(1) == 0xc /* '\14' */ ||
           LA(1) == 0x20 /* ' ' */)) {
        mSpace(false);
      } else {
        if (_cnt668 >= 1) {
          goto _loop668;
        } else {
          throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
              LA(1), getFilename(), getLine(), getColumn());
        }
      }

      _cnt668++;
    }
  _loop668:;
  }  // ( ... )+
  {
    mStringLiteral(true);
    sl = _returnToken;
  }
  {  // ( ... )*
    for (;;) {
      if ((LA(1) == 0x9 /* '\t' */ || LA(1) == 0xc /* '\14' */ ||
           LA(1) == 0x20 /* ' ' */)) {
        {  // ( ... )+
          int _cnt672 = 0;
          for (;;) {
            if ((LA(1) == 0x9 /* '\t' */ || LA(1) == 0xc /* '\14' */ ||
                 LA(1) == 0x20 /* ' ' */)) {
              mSpace(false);
            } else {
              if (_cnt672 >= 1) {
                goto _loop672;
              } else {
                throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
                    LA(1), getFilename(), getLine(), getColumn());
              }
            }

            _cnt672++;
          }
        _loop672:;
        }  // ( ... )+
        mDecimal(false);
      } else {
        goto _loop673;
      }
    }
  _loop673:;
  }  // ( ... )*
  if (inputState->guessing == 0) {
#line 2434 "CPP_parser.g"

    process_line_directive((sl->getText()).data(),
                           (n->getText()).data());  // see main()

#line 1451 "CPPLexer.cpp"
  }
  mEndOfLine(false);
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mSpace(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = Space;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  {
    switch (LA(1)) {
      case 0x20 /* ' ' */: {
        match(' ' /* charlit */);
        break;
      }
      case 0x9 /* '\t' */: {
        match('\t' /* charlit */);
        break;
      }
      case 0xc /* '\14' */: {
        match('\14' /* charlit */);
        break;
      }
      default: {
        throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
            LA(1), getFilename(), getLine(), getColumn());
      }
    }
  }
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mDecimal(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = Decimal;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  {  // ( ... )+
    int _cnt710 = 0;
    for (;;) {
      if (((LA(1) >= 0x30 /* '0' */ && LA(1) <= 0x39 /* '9' */))) {
        matchRange('0', '9');
      } else {
        if (_cnt710 >= 1) {
          goto _loop710;
        } else {
          throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
              LA(1), getFilename(), getLine(), getColumn());
        }
      }

      _cnt710++;
    }
  _loop710:;
  }  // ( ... )+
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mStringLiteral(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = StringLiteral;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match('\"' /* charlit */);
  {  // ( ... )*
    for (;;) {
      if ((LA(1) == 0x5c /* '\\' */) && (_tokenSet_4.member(LA(2)))) {
        mEscape(false);
      } else if ((LA(1) == 0x5c /* '\\' */) &&
                 (LA(2) == 0xa /* '\n' */ || LA(2) == 0xd /* '\r' */)) {
        {
          if ((LA(1) == 0x5c /* '\\' */) && (LA(2) == 0xd /* '\r' */) &&
              (LA(3) == 0xa /* '\n' */)) {
            match("\\\r\n");
          } else if ((LA(1) == 0x5c /* '\\' */) && (LA(2) == 0xd /* '\r' */) &&
                     (_tokenSet_3.member(LA(3)))) {
            match("\\\r");
          } else if ((LA(1) == 0x5c /* '\\' */) && (LA(2) == 0xa /* '\n' */)) {
            match("\\\n");
          } else {
            throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
                LA(1), getFilename(), getLine(), getColumn());
          }
        }
        if (inputState->guessing == 0) {
#line 2496 "CPP_parser.g"
          deferredNewline();
#line 1555 "CPPLexer.cpp"
        }
      } else if ((_tokenSet_5.member(LA(1)))) {
        { match(_tokenSet_5); }
      } else {
        goto _loop694;
      }
    }
  _loop694:;
  }  // ( ... )*
  match('\"' /* charlit */);
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mPragma(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = Pragma;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  {
    match('#' /* charlit */);
    match("pragma");
    {  // ( ... )*
      for (;;) {
        if ((_tokenSet_3.member(LA(1)))) {
          { match(_tokenSet_3); }
        } else {
          goto _loop680;
        }
      }
    _loop680:;
    }  // ( ... )*
    mEndOfLine(false);
  }
  if (inputState->guessing == 0) {
#line 2451 "CPP_parser.g"
    _ttype = ANTLR_USE_NAMESPACE(antlr) Token::SKIP;
    newline();
#line 1606 "CPPLexer.cpp"
  }
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mError(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = Error;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  {
    match('#' /* charlit */);
    match("error");
    {  // ( ... )*
      for (;;) {
        if ((_tokenSet_3.member(LA(1)))) {
          { match(_tokenSet_3); }
        } else {
          goto _loop685;
        }
      }
    _loop685:;
    }  // ( ... )*
    mEndOfLine(false);
  }
  if (inputState->guessing == 0) {
#line 2458 "CPP_parser.g"
    _ttype = ANTLR_USE_NAMESPACE(antlr) Token::SKIP;
    newline();
#line 1643 "CPPLexer.cpp"
  }
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mCharLiteral(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = CharLiteral;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match('\'' /* charlit */);
  {
    if ((LA(1) == 0x5c /* '\\' */) && (_tokenSet_4.member(LA(2))) &&
        (_tokenSet_6.member(LA(3)))) {
      mEscape(false);
    } else if ((_tokenSet_7.member(LA(1))) && (LA(2) == 0x27 /* '\'' */) &&
               (true)) {
      { match(_tokenSet_7); }
    } else {
      throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
          LA(1), getFilename(), getLine(), getColumn());
    }
  }
  match('\'' /* charlit */);
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mEscape(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = Escape;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match('\\' /* charlit */);
  {
    switch (LA(1)) {
      case 0x61 /* 'a' */: {
        match('a' /* charlit */);
        break;
      }
      case 0x62 /* 'b' */: {
        match('b' /* charlit */);
        break;
      }
      case 0x66 /* 'f' */: {
        match('f' /* charlit */);
        break;
      }
      case 0x6e /* 'n' */: {
        match('n' /* charlit */);
        break;
      }
      case 0x72 /* 'r' */: {
        match('r' /* charlit */);
        break;
      }
      case 0x74 /* 't' */: {
        match('t' /* charlit */);
        break;
      }
      case 0x76 /* 'v' */: {
        match('v' /* charlit */);
        break;
      }
      case 0x22 /* '\"' */: {
        match('\"' /* charlit */);
        break;
      }
      case 0x27 /* '\'' */: {
        match('\'' /* charlit */);
        break;
      }
      case 0x5c /* '\\' */: {
        match('\\' /* charlit */);
        break;
      }
      case 0x3f /* '?' */: {
        match('?' /* charlit */);
        break;
      }
      case 0x30 /* '0' */:
      case 0x31 /* '1' */:
      case 0x32 /* '2' */:
      case 0x33 /* '3' */: {
        { matchRange('0', '3'); }
        {
          if (((LA(1) >= 0x30 /* '0' */ && LA(1) <= 0x39 /* '9' */)) &&
              (_tokenSet_3.member(LA(2))) && (true)) {
            mDigit(false);
            {
              if (((LA(1) >= 0x30 /* '0' */ && LA(1) <= 0x39 /* '9' */)) &&
                  (_tokenSet_3.member(LA(2))) && (true)) {
                mDigit(false);
              } else if ((_tokenSet_3.member(LA(1))) && (true) && (true)) {
              } else {
                throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
                    LA(1), getFilename(), getLine(), getColumn());
              }
            }
          } else if ((_tokenSet_3.member(LA(1))) && (true) && (true)) {
          } else {
            throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
                LA(1), getFilename(), getLine(), getColumn());
          }
        }
        break;
      }
      case 0x34 /* '4' */:
      case 0x35 /* '5' */:
      case 0x36 /* '6' */:
      case 0x37 /* '7' */: {
        { matchRange('4', '7'); }
        {
          if (((LA(1) >= 0x30 /* '0' */ && LA(1) <= 0x39 /* '9' */)) &&
              (_tokenSet_3.member(LA(2))) && (true)) {
            mDigit(false);
          } else if ((_tokenSet_3.member(LA(1))) && (true) && (true)) {
          } else {
            throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
                LA(1), getFilename(), getLine(), getColumn());
          }
        }
        break;
      }
      case 0x78 /* 'x' */: {
        match('x' /* charlit */);
        {  // ( ... )+
          int _cnt706 = 0;
          for (;;) {
            if (((LA(1) >= 0x30 /* '0' */ && LA(1) <= 0x39 /* '9' */)) &&
                (_tokenSet_3.member(LA(2))) && (true)) {
              mDigit(false);
            } else if (((LA(1) >= 0x61 /* 'a' */ && LA(1) <= 0x66 /* 'f' */)) &&
                       (_tokenSet_3.member(LA(2))) && (true)) {
              matchRange('a', 'f');
            } else if (((LA(1) >= 0x41 /* 'A' */ && LA(1) <= 0x46 /* 'F' */)) &&
                       (_tokenSet_3.member(LA(2))) && (true)) {
              matchRange('A', 'F');
            } else {
              if (_cnt706 >= 1) {
                goto _loop706;
              } else {
                throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
                    LA(1), getFilename(), getLine(), getColumn());
              }
            }

            _cnt706++;
          }
        _loop706:;
        }  // ( ... )+
        break;
      }
      default: {
        throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
            LA(1), getFilename(), getLine(), getColumn());
      }
    }
  }
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mWCharLiteral(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = WCharLiteral;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match('L' /* charlit */);
  mCharLiteral(false);
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mWStringLiteral(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = WStringLiteral;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  match('L' /* charlit */);
  mStringLiteral(false);
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mDigit(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = Digit;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  matchRange('0', '9');
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mLongSuffix(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = LongSuffix;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  switch (LA(1)) {
    case 0x6c /* 'l' */: {
      match('l' /* charlit */);
      break;
    }
    case 0x4c /* 'L' */: {
      match('L' /* charlit */);
      break;
    }
    default: {
      throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
          LA(1), getFilename(), getLine(), getColumn());
    }
  }
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mUnsignedSuffix(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = UnsignedSuffix;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  switch (LA(1)) {
    case 0x75 /* 'u' */: {
      match('u' /* charlit */);
      break;
    }
    case 0x55 /* 'U' */: {
      match('U' /* charlit */);
      break;
    }
    default: {
      throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
          LA(1), getFilename(), getLine(), getColumn());
    }
  }
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mFloatSuffix(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = FloatSuffix;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  switch (LA(1)) {
    case 0x66 /* 'f' */: {
      match('f' /* charlit */);
      break;
    }
    case 0x46 /* 'F' */: {
      match('F' /* charlit */);
      break;
    }
    default: {
      throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
          LA(1), getFilename(), getLine(), getColumn());
    }
  }
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mExponent(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = Exponent;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  {
    switch (LA(1)) {
      case 0x65 /* 'e' */: {
        match('e' /* charlit */);
        break;
      }
      case 0x45 /* 'E' */: {
        match('E' /* charlit */);
        break;
      }
      default: {
        throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
            LA(1), getFilename(), getLine(), getColumn());
      }
    }
  }
  {
    switch (LA(1)) {
      case 0x2b /* '+' */: {
        match('+' /* charlit */);
        break;
      }
      case 0x2d /* '-' */: {
        match('-' /* charlit */);
        break;
      }
      case 0x30 /* '0' */:
      case 0x31 /* '1' */:
      case 0x32 /* '2' */:
      case 0x33 /* '3' */:
      case 0x34 /* '4' */:
      case 0x35 /* '5' */:
      case 0x36 /* '6' */:
      case 0x37 /* '7' */:
      case 0x38 /* '8' */:
      case 0x39 /* '9' */: {
        break;
      }
      default: {
        throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
            LA(1), getFilename(), getLine(), getColumn());
      }
    }
  }
  {  // ( ... )+
    int _cnt718 = 0;
    for (;;) {
      if (((LA(1) >= 0x30 /* '0' */ && LA(1) <= 0x39 /* '9' */))) {
        mDigit(false);
      } else {
        if (_cnt718 >= 1) {
          goto _loop718;
        } else {
          throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
              LA(1), getFilename(), getLine(), getColumn());
        }
      }

      _cnt718++;
    }
  _loop718:;
  }  // ( ... )+
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mVocabulary(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = Vocabulary;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  matchRange('\3', static_cast<unsigned char>('\377'));
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mNumber(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = Number;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  bool synPredMatched725 = false;
  if ((((LA(1) >= 0x30 /* '0' */ && LA(1) <= 0x39 /* '9' */)) &&
       (_tokenSet_8.member(LA(2))) && (true))) {
    int _m725 = mark();
    synPredMatched725 = true;
    inputState->guessing++;
    try {
      {
        {  // ( ... )+
          int _cnt723 = 0;
          for (;;) {
            if (((LA(1) >= 0x30 /* '0' */ && LA(1) <= 0x39 /* '9' */))) {
              mDigit(false);
            } else {
              if (_cnt723 >= 1) {
                goto _loop723;
              } else {
                throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
                    LA(1), getFilename(), getLine(), getColumn());
              }
            }

            _cnt723++;
          }
        _loop723:;
        }  // ( ... )+
        {
          switch (LA(1)) {
            case 0x2e /* '.' */: {
              match('.' /* charlit */);
              break;
            }
            case 0x65 /* 'e' */: {
              match('e' /* charlit */);
              break;
            }
            case 0x45 /* 'E' */: {
              match('E' /* charlit */);
              break;
            }
            default: {
              throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
                  LA(1), getFilename(), getLine(), getColumn());
            }
          }
        }
      }
    } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
      synPredMatched725 = false;
    }
    rewind(_m725);
    inputState->guessing--;
  }
  if (synPredMatched725) {
    {  // ( ... )+
      int _cnt727 = 0;
      for (;;) {
        if (((LA(1) >= 0x30 /* '0' */ && LA(1) <= 0x39 /* '9' */))) {
          mDigit(false);
        } else {
          if (_cnt727 >= 1) {
            goto _loop727;
          } else {
            throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
                LA(1), getFilename(), getLine(), getColumn());
          }
        }

        _cnt727++;
      }
    _loop727:;
    }  // ( ... )+
    {
      switch (LA(1)) {
        case 0x2e /* '.' */: {
          match('.' /* charlit */);
          {  // ( ... )*
            for (;;) {
              if (((LA(1) >= 0x30 /* '0' */ && LA(1) <= 0x39 /* '9' */))) {
                mDigit(false);
              } else {
                goto _loop730;
              }
            }
          _loop730:;
          }  // ( ... )*
          {
            if ((LA(1) == 0x45 /* 'E' */ || LA(1) == 0x65 /* 'e' */)) {
              mExponent(false);
            } else {
            }
          }
          if (inputState->guessing == 0) {
#line 2606 "CPP_parser.g"
            _ttype = FLOATONE;
#line 2158 "CPPLexer.cpp"
          }
          break;
        }
        case 0x45 /* 'E' */:
        case 0x65 /* 'e' */: {
          mExponent(false);
          if (inputState->guessing == 0) {
#line 2607 "CPP_parser.g"
            _ttype = FLOATTWO;
#line 2169 "CPPLexer.cpp"
          }
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
              LA(1), getFilename(), getLine(), getColumn());
        }
      }
    }
    {
      switch (LA(1)) {
        case 0x46 /* 'F' */:
        case 0x66 /* 'f' */: {
          mFloatSuffix(false);
          break;
        }
        case 0x4c /* 'L' */:
        case 0x6c /* 'l' */: {
          mLongSuffix(false);
          break;
        }
        default: {}
      }
    }
  } else {
    bool synPredMatched734 = false;
    if (((LA(1) == 0x2e /* '.' */) && (LA(2) == 0x2e /* '.' */))) {
      int _m734 = mark();
      synPredMatched734 = true;
      inputState->guessing++;
      try {
        { match("..."); }
      } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
        synPredMatched734 = false;
      }
      rewind(_m734);
      inputState->guessing--;
    }
    if (synPredMatched734) {
      match("...");
      if (inputState->guessing == 0) {
#line 2613 "CPP_parser.g"
        _ttype = ELLIPSIS;
#line 2221 "CPPLexer.cpp"
      }
    } else if ((LA(1) == 0x30 /* '0' */) &&
               (LA(2) == 0x58 /* 'X' */ || LA(2) == 0x78 /* 'x' */)) {
      match('0' /* charlit */);
      {
        switch (LA(1)) {
          case 0x78 /* 'x' */: {
            match('x' /* charlit */);
            break;
          }
          case 0x58 /* 'X' */: {
            match('X' /* charlit */);
            break;
          }
          default: {
            throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
                LA(1), getFilename(), getLine(), getColumn());
          }
        }
      }
      {  // ( ... )+
        int _cnt750 = 0;
        for (;;) {
          switch (LA(1)) {
            case 0x61 /* 'a' */:
            case 0x62 /* 'b' */:
            case 0x63 /* 'c' */:
            case 0x64 /* 'd' */:
            case 0x65 /* 'e' */:
            case 0x66 /* 'f' */: {
              matchRange('a', 'f');
              break;
            }
            case 0x41 /* 'A' */:
            case 0x42 /* 'B' */:
            case 0x43 /* 'C' */:
            case 0x44 /* 'D' */:
            case 0x45 /* 'E' */:
            case 0x46 /* 'F' */: {
              matchRange('A', 'F');
              break;
            }
            case 0x30 /* '0' */:
            case 0x31 /* '1' */:
            case 0x32 /* '2' */:
            case 0x33 /* '3' */:
            case 0x34 /* '4' */:
            case 0x35 /* '5' */:
            case 0x36 /* '6' */:
            case 0x37 /* '7' */:
            case 0x38 /* '8' */:
            case 0x39 /* '9' */: {
              mDigit(false);
              break;
            }
            default: {
              if (_cnt750 >= 1) {
                goto _loop750;
              } else {
                throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
                    LA(1), getFilename(), getLine(), getColumn());
              }
            }
          }
          _cnt750++;
        }
      _loop750:;
      }  // ( ... )+
      {  // ( ... )*
        for (;;) {
          switch (LA(1)) {
            case 0x4c /* 'L' */:
            case 0x6c /* 'l' */: {
              mLongSuffix(false);
              break;
            }
            case 0x55 /* 'U' */:
            case 0x75 /* 'u' */: {
              mUnsignedSuffix(false);
              break;
            }
            default: { goto _loop752; }
          }
        }
      _loop752:;
      }  // ( ... )*
      if (inputState->guessing == 0) {
#line 2637 "CPP_parser.g"
        _ttype = HEXADECIMALINT;
#line 2317 "CPPLexer.cpp"
      }
    } else if ((LA(1) == 0x2e /* '.' */) && (true)) {
      match('.' /* charlit */);
      if (inputState->guessing == 0) {
#line 2615 "CPP_parser.g"
        _ttype = DOT;
#line 2325 "CPPLexer.cpp"
      }
      {
        if (((LA(1) >= 0x30 /* '0' */ && LA(1) <= 0x39 /* '9' */))) {
          {  // ( ... )+
            int _cnt737 = 0;
            for (;;) {
              if (((LA(1) >= 0x30 /* '0' */ && LA(1) <= 0x39 /* '9' */))) {
                mDigit(false);
              } else {
                if (_cnt737 >= 1) {
                  goto _loop737;
                } else {
                  throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
                      LA(1), getFilename(), getLine(), getColumn());
                }
              }

              _cnt737++;
            }
          _loop737:;
          }  // ( ... )+
          {
            if ((LA(1) == 0x45 /* 'E' */ || LA(1) == 0x65 /* 'e' */)) {
              mExponent(false);
            } else {
            }
          }
          if (inputState->guessing == 0) {
#line 2616 "CPP_parser.g"
            _ttype = FLOATONE;
#line 2354 "CPPLexer.cpp"
          }
          {
            switch (LA(1)) {
              case 0x46 /* 'F' */:
              case 0x66 /* 'f' */: {
                mFloatSuffix(false);
                break;
              }
              case 0x4c /* 'L' */:
              case 0x6c /* 'l' */: {
                mLongSuffix(false);
                break;
              }
              default: {}
            }
          }
        } else {
        }
      }
    } else if ((LA(1) == 0x30 /* '0' */) && (true) && (true)) {
      match('0' /* charlit */);
      {  // ( ... )*
        for (;;) {
          if (((LA(1) >= 0x30 /* '0' */ && LA(1) <= 0x37 /* '7' */))) {
            matchRange('0', '7');
          } else {
            goto _loop741;
          }
        }
      _loop741:;
      }  // ( ... )*
      {  // ( ... )*
        for (;;) {
          switch (LA(1)) {
            case 0x4c /* 'L' */:
            case 0x6c /* 'l' */: {
              mLongSuffix(false);
              break;
            }
            case 0x55 /* 'U' */:
            case 0x75 /* 'u' */: {
              mUnsignedSuffix(false);
              break;
            }
            default: { goto _loop743; }
          }
        }
      _loop743:;
      }  // ( ... )*
      if (inputState->guessing == 0) {
#line 2626 "CPP_parser.g"
        _ttype = OCTALINT;
#line 2421 "CPPLexer.cpp"
      }
    } else if (((LA(1) >= 0x31 /* '1' */ && LA(1) <= 0x39 /* '9' */)) &&
               (true) && (true)) {
      matchRange('1', '9');
      {  // ( ... )*
        for (;;) {
          if (((LA(1) >= 0x30 /* '0' */ && LA(1) <= 0x39 /* '9' */))) {
            mDigit(false);
          } else {
            goto _loop745;
          }
        }
      _loop745:;
      }  // ( ... )*
      {  // ( ... )*
        for (;;) {
          switch (LA(1)) {
            case 0x4c /* 'L' */:
            case 0x6c /* 'l' */: {
              mLongSuffix(false);
              break;
            }
            case 0x55 /* 'U' */:
            case 0x75 /* 'u' */: {
              mUnsignedSuffix(false);
              break;
            }
            default: { goto _loop747; }
          }
        }
      _loop747:;
      }  // ( ... )*
      if (inputState->guessing == 0) {
#line 2631 "CPP_parser.g"
        _ttype = DECIMALINT;
#line 2464 "CPPLexer.cpp"
      }
    } else {
      throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
          LA(1), getFilename(), getLine(), getColumn());
    }
  }
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

void CPPLexer::mID(bool _createToken) {
  int _ttype;
  ANTLR_USE_NAMESPACE(antlr) RefToken _token;
  ANTLR_USE_NAMESPACE(std) string::size_type _begin = text.length();
  _ttype = ID;
  ANTLR_USE_NAMESPACE(std) string::size_type _saveIndex;

  {
    switch (LA(1)) {
      case 0x61 /* 'a' */:
      case 0x62 /* 'b' */:
      case 0x63 /* 'c' */:
      case 0x64 /* 'd' */:
      case 0x65 /* 'e' */:
      case 0x66 /* 'f' */:
      case 0x67 /* 'g' */:
      case 0x68 /* 'h' */:
      case 0x69 /* 'i' */:
      case 0x6a /* 'j' */:
      case 0x6b /* 'k' */:
      case 0x6c /* 'l' */:
      case 0x6d /* 'm' */:
      case 0x6e /* 'n' */:
      case 0x6f /* 'o' */:
      case 0x70 /* 'p' */:
      case 0x71 /* 'q' */:
      case 0x72 /* 'r' */:
      case 0x73 /* 's' */:
      case 0x74 /* 't' */:
      case 0x75 /* 'u' */:
      case 0x76 /* 'v' */:
      case 0x77 /* 'w' */:
      case 0x78 /* 'x' */:
      case 0x79 /* 'y' */:
      case 0x7a /* 'z' */: {
        matchRange('a', 'z');
        break;
      }
      case 0x41 /* 'A' */:
      case 0x42 /* 'B' */:
      case 0x43 /* 'C' */:
      case 0x44 /* 'D' */:
      case 0x45 /* 'E' */:
      case 0x46 /* 'F' */:
      case 0x47 /* 'G' */:
      case 0x48 /* 'H' */:
      case 0x49 /* 'I' */:
      case 0x4a /* 'J' */:
      case 0x4b /* 'K' */:
      case 0x4c /* 'L' */:
      case 0x4d /* 'M' */:
      case 0x4e /* 'N' */:
      case 0x4f /* 'O' */:
      case 0x50 /* 'P' */:
      case 0x51 /* 'Q' */:
      case 0x52 /* 'R' */:
      case 0x53 /* 'S' */:
      case 0x54 /* 'T' */:
      case 0x55 /* 'U' */:
      case 0x56 /* 'V' */:
      case 0x57 /* 'W' */:
      case 0x58 /* 'X' */:
      case 0x59 /* 'Y' */:
      case 0x5a /* 'Z' */: {
        matchRange('A', 'Z');
        break;
      }
      case 0x5f /* '_' */: {
        match('_' /* charlit */);
        break;
      }
      default: {
        throw ANTLR_USE_NAMESPACE(antlr) NoViableAltForCharException(
            LA(1), getFilename(), getLine(), getColumn());
      }
    }
  }
  {  // ( ... )*
    for (;;) {
      switch (LA(1)) {
        case 0x61 /* 'a' */:
        case 0x62 /* 'b' */:
        case 0x63 /* 'c' */:
        case 0x64 /* 'd' */:
        case 0x65 /* 'e' */:
        case 0x66 /* 'f' */:
        case 0x67 /* 'g' */:
        case 0x68 /* 'h' */:
        case 0x69 /* 'i' */:
        case 0x6a /* 'j' */:
        case 0x6b /* 'k' */:
        case 0x6c /* 'l' */:
        case 0x6d /* 'm' */:
        case 0x6e /* 'n' */:
        case 0x6f /* 'o' */:
        case 0x70 /* 'p' */:
        case 0x71 /* 'q' */:
        case 0x72 /* 'r' */:
        case 0x73 /* 's' */:
        case 0x74 /* 't' */:
        case 0x75 /* 'u' */:
        case 0x76 /* 'v' */:
        case 0x77 /* 'w' */:
        case 0x78 /* 'x' */:
        case 0x79 /* 'y' */:
        case 0x7a /* 'z' */: {
          matchRange('a', 'z');
          break;
        }
        case 0x41 /* 'A' */:
        case 0x42 /* 'B' */:
        case 0x43 /* 'C' */:
        case 0x44 /* 'D' */:
        case 0x45 /* 'E' */:
        case 0x46 /* 'F' */:
        case 0x47 /* 'G' */:
        case 0x48 /* 'H' */:
        case 0x49 /* 'I' */:
        case 0x4a /* 'J' */:
        case 0x4b /* 'K' */:
        case 0x4c /* 'L' */:
        case 0x4d /* 'M' */:
        case 0x4e /* 'N' */:
        case 0x4f /* 'O' */:
        case 0x50 /* 'P' */:
        case 0x51 /* 'Q' */:
        case 0x52 /* 'R' */:
        case 0x53 /* 'S' */:
        case 0x54 /* 'T' */:
        case 0x55 /* 'U' */:
        case 0x56 /* 'V' */:
        case 0x57 /* 'W' */:
        case 0x58 /* 'X' */:
        case 0x59 /* 'Y' */:
        case 0x5a /* 'Z' */: {
          matchRange('A', 'Z');
          break;
        }
        case 0x5f /* '_' */: {
          match('_' /* charlit */);
          break;
        }
        case 0x30 /* '0' */:
        case 0x31 /* '1' */:
        case 0x32 /* '2' */:
        case 0x33 /* '3' */:
        case 0x34 /* '4' */:
        case 0x35 /* '5' */:
        case 0x36 /* '6' */:
        case 0x37 /* '7' */:
        case 0x38 /* '8' */:
        case 0x39 /* '9' */: {
          matchRange('0', '9');
          break;
        }
        default: { goto _loop756; }
      }
    }
  _loop756:;
  }  // ( ... )*
  _ttype = testLiteralsTable(_ttype);
  if (_createToken && _token == ANTLR_USE_NAMESPACE(antlr) nullToken &&
      _ttype != ANTLR_USE_NAMESPACE(antlr) Token::SKIP) {
    _token = makeToken(_ttype);
    _token->setText(text.substr(_begin, text.length() - _begin));
  }
  _returnToken = _token;
  _saveIndex = 0;
}

const unsigned long CPPLexer::_tokenSet_0_data_[] = {
    0UL, 67059712UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// . 0 1 2 3 4 5 6 7 8 9
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPLexer::_tokenSet_0(_tokenSet_0_data_,
                                                              10);
const unsigned long CPPLexer::_tokenSet_1_data_[] = {
    0UL, 0UL, 2281701374UL, 134217726UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// A B C D E F G H I J K L M N O P Q R S T U V W X Y Z _ a b c d e f g
// h i j k l m n o p q r s t u v w x y z
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPLexer::_tokenSet_1(_tokenSet_1_data_,
                                                              10);
const unsigned long CPPLexer::_tokenSet_2_data_[] = {
    4294958079UL, 4294966271UL, 4294967295UL, 4294967295UL,
    4294967295UL, 4294967295UL, 4294967295UL, 4294967295UL,
    0UL,          0UL,          0UL,          0UL,
    0UL,          0UL,          0UL,          0UL};
// 0x0 0x1 0x2 0x3 0x4 0x5 0x6 0x7 0x8 0x9 0xb 0xc 0xe 0xf 0x10 0x11 0x12
// 0x13 0x14 0x15 0x16 0x17 0x18 0x19 0x1a 0x1b 0x1c 0x1d 0x1e 0x1f   !
// \" # $ % & \' ( ) + , - . / 0 1 2 3 4 5 6 7 8 9 : ; < = > ? @ A B C
// D E F G H I J K L M N O P Q R S T U V W X Y Z [ 0x5c ] ^ _ ` a b c d
// e f g h i j k l m n o p q r s t u v w x y z { | } ~ 0x7f 0x80 0x81 0x82
// 0x83 0x84 0x85 0x86 0x87 0x88 0x89 0x8a 0x8b 0x8c 0x8d 0x8e 0x8f 0x90
// 0x91 0x92 0x93 0x94 0x95 0x96 0x97 0x98 0x99 0x9a 0x9b 0x9c 0x9d 0x9e
// 0x9f 0xa0 0xa1 0xa2 0xa3 0xa4 0xa5 0xa6 0xa7 0xa8 0xa9 0xaa 0xab 0xac
// 0xad 0xae 0xaf 0xb0 0xb1 0xb2 0xb3 0xb4 0xb5 0xb6 0xb7 0xb8 0xb9 0xba
// 0xbb 0xbc 0xbd 0xbe 0xbf 0xc0
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPLexer::_tokenSet_2(_tokenSet_2_data_,
                                                              16);
const unsigned long CPPLexer::_tokenSet_3_data_[] = {
    4294958079UL, 4294967295UL, 4294967295UL, 4294967295UL,
    4294967295UL, 4294967295UL, 4294967295UL, 4294967295UL,
    0UL,          0UL,          0UL,          0UL,
    0UL,          0UL,          0UL,          0UL};
// 0x0 0x1 0x2 0x3 0x4 0x5 0x6 0x7 0x8 0x9 0xb 0xc 0xe 0xf 0x10 0x11 0x12
// 0x13 0x14 0x15 0x16 0x17 0x18 0x19 0x1a 0x1b 0x1c 0x1d 0x1e 0x1f   !
// \" # $ % & \' ( ) * + , - . / 0 1 2 3 4 5 6 7 8 9 : ; < = > ? @ A B
// C D E F G H I J K L M N O P Q R S T U V W X Y Z [ 0x5c ] ^ _ ` a b c
// d e f g h i j k l m n o p q r s t u v w x y z { | } ~ 0x7f 0x80 0x81
// 0x82 0x83 0x84 0x85 0x86 0x87 0x88 0x89 0x8a 0x8b 0x8c 0x8d 0x8e 0x8f
// 0x90 0x91 0x92 0x93 0x94 0x95 0x96 0x97 0x98 0x99 0x9a 0x9b 0x9c 0x9d
// 0x9e 0x9f 0xa0 0xa1 0xa2 0xa3 0xa4 0xa5 0xa6 0xa7 0xa8 0xa9 0xaa 0xab
// 0xac 0xad 0xae 0xaf 0xb0 0xb1 0xb2 0xb3 0xb4 0xb5 0xb6 0xb7 0xb8 0xb9
// 0xba 0xbb 0xbc 0xbd 0xbe 0xbf 0xc0
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPLexer::_tokenSet_3(_tokenSet_3_data_,
                                                              16);
const unsigned long CPPLexer::_tokenSet_4_data_[] = {
    0UL, 2164195460UL, 268435456UL, 22298694UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// \" \' 0 1 2 3 4 5 6 7 ? 0x5c a b f n r t v x
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPLexer::_tokenSet_4(_tokenSet_4_data_,
                                                              10);
const unsigned long CPPLexer::_tokenSet_5_data_[] = {
    4294958079UL, 4294967291UL, 4026531839UL, 4294967295UL,
    4294967295UL, 4294967295UL, 4294967295UL, 4294967295UL,
    0UL,          0UL,          0UL,          0UL,
    0UL,          0UL,          0UL,          0UL};
// 0x0 0x1 0x2 0x3 0x4 0x5 0x6 0x7 0x8 0x9 0xb 0xc 0xe 0xf 0x10 0x11 0x12
// 0x13 0x14 0x15 0x16 0x17 0x18 0x19 0x1a 0x1b 0x1c 0x1d 0x1e 0x1f   !
// # $ % & \' ( ) * + , - . / 0 1 2 3 4 5 6 7 8 9 : ; < = > ? @ A B C D
// E F G H I J K L M N O P Q R S T U V W X Y Z [ ] ^ _ ` a b c d e f g
// h i j k l m n o p q r s t u v w x y z { | } ~ 0x7f 0x80 0x81 0x82 0x83
// 0x84 0x85 0x86 0x87 0x88 0x89 0x8a 0x8b 0x8c 0x8d 0x8e 0x8f 0x90 0x91
// 0x92 0x93 0x94 0x95 0x96 0x97 0x98 0x99 0x9a 0x9b 0x9c 0x9d 0x9e 0x9f
// 0xa0 0xa1 0xa2 0xa3 0xa4 0xa5 0xa6 0xa7 0xa8 0xa9 0xaa 0xab 0xac 0xad
// 0xae 0xaf 0xb0 0xb1 0xb2 0xb3 0xb4 0xb5 0xb6 0xb7 0xb8 0xb9 0xba 0xbb
// 0xbc 0xbd 0xbe 0xbf 0xc0
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPLexer::_tokenSet_5(_tokenSet_5_data_,
                                                              16);
const unsigned long CPPLexer::_tokenSet_6_data_[] = {
    0UL, 67043456UL, 126UL, 126UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// \' 0 1 2 3 4 5 6 7 8 9 A B C D E F a b c d e f
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPLexer::_tokenSet_6(_tokenSet_6_data_,
                                                              10);
const unsigned long CPPLexer::_tokenSet_7_data_[] = {
    4294967295UL, 4294967167UL, 4294967295UL, 4294967295UL,
    4294967295UL, 4294967295UL, 4294967295UL, 4294967295UL,
    0UL,          0UL,          0UL,          0UL,
    0UL,          0UL,          0UL,          0UL};
// 0x0 0x1 0x2 0x3 0x4 0x5 0x6 0x7 0x8 0x9 0xa 0xb 0xc 0xd 0xe 0xf 0x10
// 0x11 0x12 0x13 0x14 0x15 0x16 0x17 0x18 0x19 0x1a 0x1b 0x1c 0x1d 0x1e
// 0x1f   ! \" # $ % & ( ) * + , - . / 0 1 2 3 4 5 6 7 8 9 : ; < = > ?
// @ A B C D E F G H I J K L M N O P Q R S T U V W X Y Z [ 0x5c ] ^ _ `
// a b c d e f g h i j k l m n o p q r s t u v w x y z { | } ~ 0x7f 0x80
// 0x81 0x82 0x83 0x84 0x85 0x86 0x87 0x88 0x89 0x8a 0x8b 0x8c 0x8d 0x8e
// 0x8f 0x90 0x91 0x92 0x93 0x94 0x95 0x96 0x97 0x98 0x99 0x9a 0x9b 0x9c
// 0x9d 0x9e 0x9f 0xa0 0xa1 0xa2 0xa3 0xa4 0xa5 0xa6 0xa7 0xa8 0xa9 0xaa
// 0xab 0xac 0xad 0xae 0xaf 0xb0 0xb1 0xb2 0xb3 0xb4 0xb5 0xb6 0xb7 0xb8
// 0xb9 0xba 0xbb 0xbc 0xbd 0xbe 0xbf 0xc0
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPLexer::_tokenSet_7(_tokenSet_7_data_,
                                                              16);
const unsigned long CPPLexer::_tokenSet_8_data_[] = {
    0UL, 67059712UL, 32UL, 32UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// . 0 1 2 3 4 5 6 7 8 9 E e
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPLexer::_tokenSet_8(_tokenSet_8_data_,
                                                              10);
