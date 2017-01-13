/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/* $ANTLR 2.7.7 (20130409): "CPP_parser.g" -> "CPPParser.cpp"$ */
#include "CPPParser.hpp"
#include <antlr/NoViableAltException.hpp>
#include <antlr/SemanticException.hpp>
#include <antlr/ASTFactory.hpp>
#line 102 "CPP_parser.g"

// File generated from CPP_parser.g
// Version 3.1 November 2005
// This file is best viewed in courier font with tabs set to 4 spaces
//
// The statements in this block appear only in CPPParser.cpp and not in
// CPPLexer.cpp

// These constants used in the previous version (3.0 July 2004) have now been
// replaced by the following,
// ID_VAR_NAME is now CPPSymbol::otVariable
// ID_FUN_NAME is now CPPSymbol::otFunction
// ID_INPUT_PARAMETER is now not used
// ID_CLASS_DEF is now CPPSymbol::otClass
// ID_SYSTEM_FUNCTION is now not used
// ID_CONST_DECL is now not used
// ID_TYPEDEF_VAR is now CPPSymbol::otTypedef

int statementTrace =
    2;  // Used to control selected (level) tracing (see support.cpp)
// 1 Shows which external and member statements selected
// 2 Shows above plus all declarations/definitions
// 3 reserved for future use
// 4 and above available for user

void CPPParser::init() {
  antlrTrace(false);  // This is a dynamic trace facility for use with
                      // -traceParser etc.
  // It requires modification in LLkParser.cpp and LLkParser.hpp
  // otherwise it should be commented out (see MyReadMe.txt)
  // true shows antlr trace (or can be set and reset during parsing)
  // false stops showing antlr trace
  // Provided the parser is always generated with -traceParser this
  // facility allows trace output to be turned on or off by changing
  // the setting here from false to true or vice versa and then
  // recompiling and linking CPPParser only thus avoiding the need
  // to use antlr.Tool to re-generate the lexer and parser again
  // with (or without) -traceParser.
  // Antlr trace can also be turned on and off dynamically using
  // antlrTrace_on or antlrTrace_off statements inserted into the
  // source code being parsed (See below).

  // Creates a dictionary to hold symbols with 4001 buckets, 200 scopes and
  // 800,000 characters
  // These can be changed to suit the size of program(s) being parsed
  symbols = new CPPDictionary(4001, 200, 800000);

  // Set template parameter scope - Not used at present
  templateParameterScope =
      symbols->getCurrentScopeIndex();  // Set template parameter scope to 0

  symbols->saveScope();  // Advance currentScope from 0 to 1
  // Set "external" scope for all types
  externalScope =
      symbols->getCurrentScopeIndex();  // Set "external" scope to 1 for types

  // Declare predefined scope "std" in external scope
  CPPSymbol* a = new CPPSymbol("std", CPPSymbol::otTypedef);
  symbols->define("std", a);

  symbols->saveScope();  // Advance currentScope from 1 to 2 (and higher) for
                         // all other symbols
  // treated as locals

  // Global flags to allow for nested declarations
  _td = false;      // For typedef
  _fd = false;      // For friend
  _sc = scInvalid;  // For StorageClass
  _tq = tqInvalid;  // For TypeQualifier
  _ts = tsInvalid;  // For TypeSpecifier
  _fs = fsInvalid;  // For FunctionSpecifier

  functionDefinition = 0;
  qualifierPrefix[0] = '\0';
  enclosingClass = (char*)"";
  assign_stmt_RHS_found = 0;
  in_parameter_list = false;
  K_and_R = false;  // used to distinguish old K & R parameter definitions
  in_return = false;
  is_address = false;
  is_pointer = false;
}

#line 85 "CPPParser.cpp"
CPPParser::CPPParser(ANTLR_USE_NAMESPACE(antlr) TokenBuffer& tokenBuf, int k)
    : ANTLR_USE_NAMESPACE(antlr) LLkParser(tokenBuf, k) {}

CPPParser::CPPParser(ANTLR_USE_NAMESPACE(antlr) TokenBuffer& tokenBuf)
    : ANTLR_USE_NAMESPACE(antlr) LLkParser(tokenBuf, 2) {}

CPPParser::CPPParser(ANTLR_USE_NAMESPACE(antlr) TokenStream& lexer, int k)
    : ANTLR_USE_NAMESPACE(antlr) LLkParser(lexer, k) {}

CPPParser::CPPParser(ANTLR_USE_NAMESPACE(antlr) TokenStream& lexer)
    : ANTLR_USE_NAMESPACE(antlr) LLkParser(lexer, 2) {}

CPPParser::CPPParser(const ANTLR_USE_NAMESPACE(antlr)
                         ParserSharedInputState& state)
    : ANTLR_USE_NAMESPACE(antlr) LLkParser(state, 2) {}

void CPPParser::translation_unit() {
  try {  // for error handling
    if (inputState->guessing == 0) {
#line 376 "CPP_parser.g"
      enterExternalScope();
#line 117 "CPPParser.cpp"
    }
    {  // ( ... )+
      int _cnt3 = 0;
      for (;;) {
        if ((_tokenSet_0.member(LA(1)))) {
          external_declaration();
        } else {
          if (_cnt3 >= 1) {
            goto _loop3;
          } else {
            throw ANTLR_USE_NAMESPACE(antlr)
                NoViableAltException(LT(1), getFilename());
          }
        }

        _cnt3++;
      }
    _loop3:;
    }  // ( ... )+
    match(ANTLR_USE_NAMESPACE(antlr) Token::EOF_TYPE);
    if (inputState->guessing == 0) {
#line 378 "CPP_parser.g"
      exitExternalScope();
#line 137 "CPPParser.cpp"
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_1);
    } else {
      throw;
    }
  }
}

void CPPParser::external_declaration() {
#line 381 "CPP_parser.g"
  char* s;
  K_and_R = false;
  FunctionSpecifier fs = fsInvalid;  // inline,virtual,explicit

#line 156 "CPPParser.cpp"

  try {  // for error handling
    {
      switch (LA(1)) {
        case LITERAL_namespace: {
          if (inputState->guessing == 0) {
#line 539 "CPP_parser.g"
            if (statementTrace >= 1) {
              printf("%d external_declaration Namespace definition\n",
                     LT(1)->getLine());
            }

#line 168 "CPPParser.cpp"
          }
          match(LITERAL_namespace);
          namespace_definition();
          break;
        }
        case SEMICOLON: {
          if (inputState->guessing == 0) {
#line 551 "CPP_parser.g"
            if (statementTrace >= 1) {
              printf("%d external_declaration Semicolon\n", LT(1)->getLine());
            }

#line 181 "CPPParser.cpp"
          }
          match(SEMICOLON);
          if (inputState->guessing == 0) {
#line 554 "CPP_parser.g"
            end_of_stmt();
#line 187 "CPPParser.cpp"
          }
          break;
        }
        case LITERAL_antlrTrace_on: {
          match(LITERAL_antlrTrace_on);
          if (inputState->guessing == 0) {
#line 558 "CPP_parser.g"
            antlrTrace(true);
#line 197 "CPPParser.cpp"
          }
          break;
        }
        case LITERAL_antlrTrace_off: {
          match(LITERAL_antlrTrace_off);
          if (inputState->guessing == 0) {
#line 561 "CPP_parser.g"
            antlrTrace(false);
#line 207 "CPPParser.cpp"
          }
          break;
        }
        default:
          bool synPredMatched7 = false;
          if (((LA(1) == LITERAL_template) && (LA(2) == LESSTHAN))) {
            int _m7 = mark();
            synPredMatched7 = true;
            inputState->guessing++;
            try {
              {
                match(LITERAL_template);
                match(LESSTHAN);
                match(GREATERTHAN);
              }
            } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
              synPredMatched7 = false;
            }
            rewind(_m7);
            inputState->guessing--;
          }
          if (synPredMatched7) {
            if (inputState->guessing == 0) {
#line 390 "CPP_parser.g"
              if (statementTrace >= 1) {
                printf(
                    "%d external_declaration template "
                    "explicit-specialisation\n",
                    LT(1)->getLine());
              }

#line 236 "CPPParser.cpp"
            }
            match(LITERAL_template);
            match(LESSTHAN);
            match(GREATERTHAN);
            external_declaration();
          } else {
            bool synPredMatched9 = false;
            if (((_tokenSet_2.member(LA(1))) && (_tokenSet_3.member(LA(2))))) {
              int _m9 = mark();
              synPredMatched9 = true;
              inputState->guessing++;
              try {
                { match(LITERAL_typedef); }
              } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
                synPredMatched9 = false;
              }
              rewind(_m9);
              inputState->guessing--;
            }
            if (synPredMatched9) {
              {
                bool synPredMatched12 = false;
                if (((LA(1) == LITERAL_typedef) &&
                     ((LA(2) >= LITERAL_struct && LA(2) <= LITERAL_class)))) {
                  int _m12 = mark();
                  synPredMatched12 = true;
                  inputState->guessing++;
                  try {
                    {
                      match(LITERAL_typedef);
                      class_specifier();
                    }
                  } catch (ANTLR_USE_NAMESPACE(antlr)
                               RecognitionException& pe) {
                    synPredMatched12 = false;
                  }
                  rewind(_m12);
                  inputState->guessing--;
                }
                if (synPredMatched12) {
                  if (inputState->guessing == 0) {
#line 399 "CPP_parser.g"
                    if (statementTrace >= 1) {
                      printf("%d external_declaration Typedef class type\n",
                             LT(1)->getLine());
                    }

#line 285 "CPPParser.cpp"
                  }
                  match(LITERAL_typedef);
                  class_decl_or_def(fs);
                  if (inputState->guessing == 0) {
#line 402 "CPP_parser.g"
                    _td = true;
#line 292 "CPPParser.cpp"
                  }
                  {
                    switch (LA(1)) {
                      case ID:
                      case LITERAL__stdcall:
                      case LITERAL___stdcall:
                      case LPAREN:
                      case OPERATOR:
                      case LITERAL_this:
                      case LITERAL_true:
                      case LITERAL_false:
                      case TILDE:
                      case STAR:
                      case AMPERSAND:
                      case SCOPE:
                      case LITERAL__cdecl:
                      case LITERAL___cdecl:
                      case LITERAL__near:
                      case LITERAL___near:
                      case LITERAL__far:
                      case LITERAL___far:
                      case LITERAL___interrupt:
                      case LITERAL_pascal:
                      case LITERAL__pascal:
                      case LITERAL___pascal: {
                        init_declarator_list();
                        break;
                      }
                      case SEMICOLON: {
                        break;
                      }
                      default: {
                        throw ANTLR_USE_NAMESPACE(antlr)
                            NoViableAltException(LT(1), getFilename());
                      }
                    }
                  }
                  match(SEMICOLON);
                  if (inputState->guessing == 0) {
#line 402 "CPP_parser.g"
                    end_of_stmt();
#line 336 "CPPParser.cpp"
                  }
                } else {
                  bool synPredMatched15 = false;
                  if (((LA(1) == LITERAL_typedef) && (LA(2) == LITERAL_enum))) {
                    int _m15 = mark();
                    synPredMatched15 = true;
                    inputState->guessing++;
                    try {
                      {
                        match(LITERAL_typedef);
                        match(LITERAL_enum);
                      }
                    } catch (ANTLR_USE_NAMESPACE(antlr)
                                 RecognitionException& pe) {
                      synPredMatched15 = false;
                    }
                    rewind(_m15);
                    inputState->guessing--;
                  }
                  if (synPredMatched15) {
                    if (inputState->guessing == 0) {
#line 405 "CPP_parser.g"
                      if (statementTrace >= 1) {
                        printf("%d external_declaration Typedef enum type\n",
                               LT(1)->getLine());
                      }

#line 363 "CPPParser.cpp"
                    }
                    match(LITERAL_typedef);
                    enum_specifier();
                    if (inputState->guessing == 0) {
#line 408 "CPP_parser.g"
                      _td = true;
#line 370 "CPPParser.cpp"
                    }
                    {
                      switch (LA(1)) {
                        case ID:
                        case LITERAL__stdcall:
                        case LITERAL___stdcall:
                        case LPAREN:
                        case OPERATOR:
                        case LITERAL_this:
                        case LITERAL_true:
                        case LITERAL_false:
                        case TILDE:
                        case STAR:
                        case AMPERSAND:
                        case SCOPE:
                        case LITERAL__cdecl:
                        case LITERAL___cdecl:
                        case LITERAL__near:
                        case LITERAL___near:
                        case LITERAL__far:
                        case LITERAL___far:
                        case LITERAL___interrupt:
                        case LITERAL_pascal:
                        case LITERAL__pascal:
                        case LITERAL___pascal: {
                          init_declarator_list();
                          break;
                        }
                        case SEMICOLON: {
                          break;
                        }
                        default: {
                          throw ANTLR_USE_NAMESPACE(antlr)
                              NoViableAltException(LT(1), getFilename());
                        }
                      }
                    }
                    match(SEMICOLON);
                    if (inputState->guessing == 0) {
#line 408 "CPP_parser.g"
                      end_of_stmt();
#line 414 "CPPParser.cpp"
                    }
                  } else {
                    bool synPredMatched18 = false;
                    if (((_tokenSet_2.member(LA(1))) &&
                         (_tokenSet_3.member(LA(2))))) {
                      int _m18 = mark();
                      synPredMatched18 = true;
                      inputState->guessing++;
                      try {
                        {
                          declaration_specifiers();
                          function_declarator(0);
                          match(SEMICOLON);
                        }
                      } catch (ANTLR_USE_NAMESPACE(antlr)
                                   RecognitionException& pe) {
                        synPredMatched18 = false;
                      }
                      rewind(_m18);
                      inputState->guessing--;
                    }
                    if (synPredMatched18) {
                      if (inputState->guessing == 0) {
#line 411 "CPP_parser.g"
                        if (statementTrace >= 1) {
                          printf(
                              "%d external_declaration Typedef function type\n",
                              LT(1)->getLine());
                        }

#line 442 "CPPParser.cpp"
                      }
                      declaration();
                    } else if ((_tokenSet_2.member(LA(1))) &&
                               (_tokenSet_3.member(LA(2)))) {
                      if (inputState->guessing == 0) {
#line 416 "CPP_parser.g"
                        if (statementTrace >= 1) {
                          printf(
                              "%d external_declaration Typedef variable type\n",
                              LT(1)->getLine());
                        }

#line 452 "CPPParser.cpp"
                      }
                      declaration();
                    } else {
                      throw ANTLR_USE_NAMESPACE(antlr)
                          NoViableAltException(LT(1), getFilename());
                    }
                  }
                }
              }
            } else {
              bool synPredMatched22 = false;
              if (((LA(1) == LITERAL_template) && (LA(2) == LESSTHAN))) {
                int _m22 = mark();
                synPredMatched22 = true;
                inputState->guessing++;
                try {
                  {
                    template_head();
                    {  // ( ... )*
                      for (;;) {
                        if ((_tokenSet_4.member(LA(1)))) {
                          fs = function_specifier();
                        } else {
                          goto _loop21;
                        }
                      }
                    _loop21:;
                    }  // ( ... )*
                    class_specifier();
                  }
                } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
                  synPredMatched22 = false;
                }
                rewind(_m22);
                inputState->guessing--;
              }
              if (synPredMatched22) {
                if (inputState->guessing == 0) {
#line 424 "CPP_parser.g"
                  if (statementTrace >= 1) {
                    printf(
                        "%d external_declaration Templated class decl or def\n",
                        LT(1)->getLine());
                  }

#line 498 "CPPParser.cpp"
                }
                template_head();
                {  // ( ... )*
                  for (;;) {
                    if ((_tokenSet_4.member(LA(1)))) {
                      fs = function_specifier();
                    } else {
                      goto _loop24;
                    }
                  }
                _loop24:;
                }  // ( ... )*
                class_decl_or_def(fs);
                {
                  switch (LA(1)) {
                    case ID:
                    case LITERAL__stdcall:
                    case LITERAL___stdcall:
                    case LPAREN:
                    case OPERATOR:
                    case LITERAL_this:
                    case LITERAL_true:
                    case LITERAL_false:
                    case TILDE:
                    case STAR:
                    case AMPERSAND:
                    case SCOPE:
                    case LITERAL__cdecl:
                    case LITERAL___cdecl:
                    case LITERAL__near:
                    case LITERAL___near:
                    case LITERAL__far:
                    case LITERAL___far:
                    case LITERAL___interrupt:
                    case LITERAL_pascal:
                    case LITERAL__pascal:
                    case LITERAL___pascal: {
                      init_declarator_list();
                      break;
                    }
                    case SEMICOLON: {
                      break;
                    }
                    default: {
                      throw ANTLR_USE_NAMESPACE(antlr)
                          NoViableAltException(LT(1), getFilename());
                    }
                  }
                }
                match(SEMICOLON);
                if (inputState->guessing == 0) {
#line 427 "CPP_parser.g"
                  end_of_stmt();
#line 556 "CPPParser.cpp"
                }
              } else {
                bool synPredMatched28 = false;
                if (((LA(1) == LITERAL_enum) && (_tokenSet_5.member(LA(2))))) {
                  int _m28 = mark();
                  synPredMatched28 = true;
                  inputState->guessing++;
                  try {
                    {
                      match(LITERAL_enum);
                      {
                        switch (LA(1)) {
                          case ID: {
                            match(ID);
                            break;
                          }
                          case LCURLY: {
                            break;
                          }
                          default: {
                            throw ANTLR_USE_NAMESPACE(antlr)
                                NoViableAltException(LT(1), getFilename());
                          }
                        }
                      }
                      match(LCURLY);
                    }
                  } catch (ANTLR_USE_NAMESPACE(antlr)
                               RecognitionException& pe) {
                    synPredMatched28 = false;
                  }
                  rewind(_m28);
                  inputState->guessing--;
                }
                if (synPredMatched28) {
                  if (inputState->guessing == 0) {
#line 431 "CPP_parser.g"
                    if (statementTrace >= 1) {
                      printf("%d external_declaration Enum definition\n",
                             LT(1)->getLine());
                    }

#line 600 "CPPParser.cpp"
                  }
                  enum_specifier();
                  {
                    switch (LA(1)) {
                      case ID:
                      case LITERAL__stdcall:
                      case LITERAL___stdcall:
                      case LPAREN:
                      case OPERATOR:
                      case LITERAL_this:
                      case LITERAL_true:
                      case LITERAL_false:
                      case TILDE:
                      case STAR:
                      case AMPERSAND:
                      case SCOPE:
                      case LITERAL__cdecl:
                      case LITERAL___cdecl:
                      case LITERAL__near:
                      case LITERAL___near:
                      case LITERAL__far:
                      case LITERAL___far:
                      case LITERAL___interrupt:
                      case LITERAL_pascal:
                      case LITERAL__pascal:
                      case LITERAL___pascal: {
                        init_declarator_list();
                        break;
                      }
                      case SEMICOLON: {
                        break;
                      }
                      default: {
                        throw ANTLR_USE_NAMESPACE(antlr)
                            NoViableAltException(LT(1), getFilename());
                      }
                    }
                  }
                  match(SEMICOLON);
                  if (inputState->guessing == 0) {
#line 434 "CPP_parser.g"
                    end_of_stmt();
#line 645 "CPPParser.cpp"
                  }
                } else {
                  bool synPredMatched32 = false;
                  if (((_tokenSet_6.member(LA(1))) &&
                       (_tokenSet_7.member(LA(2))))) {
                    int _m32 = mark();
                    synPredMatched32 = true;
                    inputState->guessing++;
                    try {
                      {
                        {
                          switch (LA(1)) {
                            case LITERAL_template: {
                              template_head();
                              break;
                            }
                            case ID:
                            case LITERAL_inline:
                            case LITERAL__inline:
                            case LITERAL___inline:
                            case LITERAL_virtual:
                            case TILDE:
                            case SCOPE: {
                              break;
                            }
                            default: {
                              throw ANTLR_USE_NAMESPACE(antlr)
                                  NoViableAltException(LT(1), getFilename());
                            }
                          }
                        }
                        dtor_head(1);
                        match(LCURLY);
                      }
                    } catch (ANTLR_USE_NAMESPACE(antlr)
                                 RecognitionException& pe) {
                      synPredMatched32 = false;
                    }
                    rewind(_m32);
                    inputState->guessing--;
                  }
                  if (synPredMatched32) {
                    if (inputState->guessing == 0) {
#line 438 "CPP_parser.g"
                      if (statementTrace >= 1) {
                        printf(
                            "%d external_declaration Destructor definition\n",
                            LT(1)->getLine());
                      }

#line 695 "CPPParser.cpp"
                    }
                    {
                      switch (LA(1)) {
                        case LITERAL_template: {
                          template_head();
                          break;
                        }
                        case ID:
                        case LITERAL_inline:
                        case LITERAL__inline:
                        case LITERAL___inline:
                        case LITERAL_virtual:
                        case TILDE:
                        case SCOPE: {
                          break;
                        }
                        default: {
                          throw ANTLR_USE_NAMESPACE(antlr)
                              NoViableAltException(LT(1), getFilename());
                        }
                      }
                    }
                    dtor_head(1);
                    dtor_body();
                  } else {
                    bool synPredMatched36 = false;
                    if (((_tokenSet_8.member(LA(1))) &&
                         (_tokenSet_9.member(LA(2))))) {
                      int _m36 = mark();
                      synPredMatched36 = true;
                      inputState->guessing++;
                      try {
                        {
                          {
                            if ((true) && (true)) {
                              ctor_decl_spec();
                            } else {
                            }
                          }
                          if (!(qualifiedItemIsOneOf(qiCtor))) {
                            throw ANTLR_USE_NAMESPACE(antlr) SemanticException(
                                "qualifiedItemIsOneOf(qiCtor)");
                          }
                        }
                      } catch (ANTLR_USE_NAMESPACE(antlr)
                                   RecognitionException& pe) {
                        synPredMatched36 = false;
                      }
                      rewind(_m36);
                      inputState->guessing--;
                    }
                    if (synPredMatched36) {
                      if (inputState->guessing == 0) {
#line 451 "CPP_parser.g"
                        if (statementTrace >= 1) {
                          printf(
                              "%d external_declaration Constructor "
                              "definition\n",
                              LT(1)->getLine());
                        }

#line 755 "CPPParser.cpp"
                      }
                      ctor_definition();
                    } else {
                      bool synPredMatched39 = false;
                      if (((_tokenSet_10.member(LA(1))) &&
                           (_tokenSet_11.member(LA(2))))) {
                        int _m39 = mark();
                        synPredMatched39 = true;
                        inputState->guessing++;
                        try {
                          {
                            {
                              switch (LA(1)) {
                                case LITERAL_inline: {
                                  match(LITERAL_inline);
                                  break;
                                }
                                case ID:
                                case OPERATOR:
                                case SCOPE: {
                                  break;
                                }
                                default: {
                                  throw ANTLR_USE_NAMESPACE(antlr)
                                      NoViableAltException(LT(1),
                                                           getFilename());
                                }
                              }
                            }
                            scope_override();
                            conversion_function_decl_or_def();
                          }
                        } catch (ANTLR_USE_NAMESPACE(antlr)
                                     RecognitionException& pe) {
                          synPredMatched39 = false;
                        }
                        rewind(_m39);
                        inputState->guessing--;
                      }
                      if (synPredMatched39) {
                        if (inputState->guessing == 0) {
#line 458 "CPP_parser.g"
                          if (statementTrace >= 1) {
                            printf(
                                "%d external_declaration Operator function\n",
                                LT(1)->getLine());
                          }

#line 802 "CPPParser.cpp"
                        }
                        {
                          switch (LA(1)) {
                            case LITERAL_inline: {
                              match(LITERAL_inline);
                              break;
                            }
                            case ID:
                            case OPERATOR:
                            case SCOPE: {
                              break;
                            }
                            default: {
                              throw ANTLR_USE_NAMESPACE(antlr)
                                  NoViableAltException(LT(1), getFilename());
                            }
                          }
                        }
                        s = scope_override();
                        conversion_function_decl_or_def();
                      } else {
                        bool synPredMatched42 = false;
                        if (((_tokenSet_12.member(LA(1))) &&
                             (_tokenSet_13.member(LA(2))))) {
                          int _m42 = mark();
                          synPredMatched42 = true;
                          inputState->guessing++;
                          try {
                            {
                              declaration_specifiers();
                              function_declarator(0);
                              match(SEMICOLON);
                            }
                          } catch (ANTLR_USE_NAMESPACE(antlr)
                                       RecognitionException& pe) {
                            synPredMatched42 = false;
                          }
                          rewind(_m42);
                          inputState->guessing--;
                        }
                        if (synPredMatched42) {
                          if (inputState->guessing == 0) {
#line 465 "CPP_parser.g"
                            if (statementTrace >= 1) {
                              printf(
                                  "%d external_declaration Function "
                                  "declaration\n",
                                  LT(1)->getLine());
                            }

#line 851 "CPPParser.cpp"
                          }
                          declaration_specifiers();
                          function_declarator(0);
                          match(SEMICOLON);
                          if (inputState->guessing == 0) {
#line 468 "CPP_parser.g"
                            end_of_stmt();
#line 859 "CPPParser.cpp"
                          }
                        } else {
                          bool synPredMatched44 = false;
                          if (((_tokenSet_14.member(LA(1))) &&
                               (_tokenSet_15.member(LA(2))))) {
                            int _m44 = mark();
                            synPredMatched44 = true;
                            inputState->guessing++;
                            try {
                              {
                                declaration_specifiers();
                                function_declarator(1);
                                match(LCURLY);
                              }
                            } catch (ANTLR_USE_NAMESPACE(antlr)
                                         RecognitionException& pe) {
                              synPredMatched44 = false;
                            }
                            rewind(_m44);
                            inputState->guessing--;
                          }
                          if (synPredMatched44) {
                            if (inputState->guessing == 0) {
#line 472 "CPP_parser.g"
                              if (statementTrace >= 1) {
                                printf(
                                    "%d external_declaration Function "
                                    "definition\n",
                                    LT(1)->getLine());
                              }

#line 887 "CPPParser.cpp"
                            }
                            function_definition();
                          } else {
                            bool synPredMatched46 = false;
                            if (((_tokenSet_14.member(LA(1))) &&
                                 (_tokenSet_15.member(LA(2))))) {
                              int _m46 = mark();
                              synPredMatched46 = true;
                              inputState->guessing++;
                              try {
                                {
                                  declaration_specifiers();
                                  function_declarator(1);
                                  declaration();
                                }
                              } catch (ANTLR_USE_NAMESPACE(antlr)
                                           RecognitionException& pe) {
                                synPredMatched46 = false;
                              }
                              rewind(_m46);
                              inputState->guessing--;
                            }
                            if (synPredMatched46) {
                              if (inputState->guessing == 0) {
#line 479 "CPP_parser.g"
                                K_and_R = true;
                                if (statementTrace >= 1) {
                                  printf(
                                      "%d external_declaration K & R function "
                                      "definition\n",
                                      LT(1)->getLine());
                                }

#line 917 "CPPParser.cpp"
                              }
                              function_definition();
                            } else {
                              bool synPredMatched48 = false;
                              if (((_tokenSet_14.member(LA(1))) &&
                                   (_tokenSet_15.member(LA(2))))) {
                                int _m48 = mark();
                                synPredMatched48 = true;
                                inputState->guessing++;
                                try {
                                  {
                                    function_declarator(1);
                                    declaration();
                                  }
                                } catch (ANTLR_USE_NAMESPACE(antlr)
                                             RecognitionException& pe) {
                                  synPredMatched48 = false;
                                }
                                rewind(_m48);
                                inputState->guessing--;
                              }
                              if (synPredMatched48) {
                                if (inputState->guessing == 0) {
#line 487 "CPP_parser.g"
                                  K_and_R = true;
                                  if (statementTrace >= 1) {
                                    printf(
                                        "%d external_declaration K & R "
                                        "function definition without return "
                                        "type\n",
                                        LT(1)->getLine());
                                  }

#line 946 "CPPParser.cpp"
                                }
                                function_definition();
                              } else {
                                bool synPredMatched53 = false;
                                if (((_tokenSet_16.member(LA(1))) &&
                                     (_tokenSet_17.member(LA(2))))) {
                                  int _m53 = mark();
                                  synPredMatched53 = true;
                                  inputState->guessing++;
                                  try {
                                    {
                                      {
                                        switch (LA(1)) {
                                          case LITERAL_friend: {
                                            match(LITERAL_friend);
                                            break;
                                          }
                                          case LITERAL_inline:
                                          case LITERAL_struct:
                                          case LITERAL_union:
                                          case LITERAL_class:
                                          case LITERAL__inline:
                                          case LITERAL___inline:
                                          case LITERAL_virtual:
                                          case LITERAL_explicit: {
                                            break;
                                          }
                                          default: {
                                            throw ANTLR_USE_NAMESPACE(antlr)
                                                NoViableAltException(
                                                    LT(1), getFilename());
                                          }
                                        }
                                      }
                                      {  // ( ... )*
                                        for (;;) {
                                          if ((_tokenSet_4.member(LA(1)))) {
                                            fs = function_specifier();
                                          } else {
                                            goto _loop52;
                                          }
                                        }
                                      _loop52:;
                                      }  // ( ... )*
                                      class_specifier();
                                    }
                                  } catch (ANTLR_USE_NAMESPACE(antlr)
                                               RecognitionException& pe) {
                                    synPredMatched53 = false;
                                  }
                                  rewind(_m53);
                                  inputState->guessing--;
                                }
                                if (synPredMatched53) {
                                  if (inputState->guessing == 0) {
#line 495 "CPP_parser.g"
                                    if (statementTrace >= 1) {
                                      printf(
                                          "%d external_declaration Class decl "
                                          "or def\n",
                                          LT(1)->getLine());
                                    }

#line 1009 "CPPParser.cpp"
                                  }
                                  {
                                    switch (LA(1)) {
                                      case LITERAL_friend: {
                                        match(LITERAL_friend);
                                        break;
                                      }
                                      case LITERAL_inline:
                                      case LITERAL_struct:
                                      case LITERAL_union:
                                      case LITERAL_class:
                                      case LITERAL__inline:
                                      case LITERAL___inline:
                                      case LITERAL_virtual:
                                      case LITERAL_explicit: {
                                        break;
                                      }
                                      default: {
                                        throw ANTLR_USE_NAMESPACE(antlr)
                                            NoViableAltException(LT(1),
                                                                 getFilename());
                                      }
                                    }
                                  }
                                  {  // ( ... )*
                                    for (;;) {
                                      if ((_tokenSet_4.member(LA(1)))) {
                                        fs = function_specifier();
                                      } else {
                                        goto _loop56;
                                      }
                                    }
                                  _loop56:;
                                  }  // ( ... )*
                                  class_decl_or_def(fs);
                                  {
                                    switch (LA(1)) {
                                      case ID:
                                      case LITERAL__stdcall:
                                      case LITERAL___stdcall:
                                      case LPAREN:
                                      case OPERATOR:
                                      case LITERAL_this:
                                      case LITERAL_true:
                                      case LITERAL_false:
                                      case TILDE:
                                      case STAR:
                                      case AMPERSAND:
                                      case SCOPE:
                                      case LITERAL__cdecl:
                                      case LITERAL___cdecl:
                                      case LITERAL__near:
                                      case LITERAL___near:
                                      case LITERAL__far:
                                      case LITERAL___far:
                                      case LITERAL___interrupt:
                                      case LITERAL_pascal:
                                      case LITERAL__pascal:
                                      case LITERAL___pascal: {
                                        init_declarator_list();
                                        break;
                                      }
                                      case SEMICOLON: {
                                        break;
                                      }
                                      default: {
                                        throw ANTLR_USE_NAMESPACE(antlr)
                                            NoViableAltException(LT(1),
                                                                 getFilename());
                                      }
                                    }
                                  }
                                  match(SEMICOLON);
                                  if (inputState->guessing == 0) {
#line 498 "CPP_parser.g"
                                    end_of_stmt();
#line 1090 "CPPParser.cpp"
                                  }
                                } else if ((LA(1) == LITERAL_template) &&
                                           (LA(2) == LESSTHAN)) {
                                  if (inputState->guessing == 0) {
#line 501 "CPP_parser.g"
                                    beginTemplateDeclaration();
#line 1097 "CPPParser.cpp"
                                  }
                                  template_head();
                                  {
                                    bool synPredMatched61 = false;
                                    if (((_tokenSet_12.member(LA(1))) &&
                                         (_tokenSet_18.member(LA(2))))) {
                                      int _m61 = mark();
                                      synPredMatched61 = true;
                                      inputState->guessing++;
                                      try {
                                        {
                                          declaration_specifiers();
                                          {
                                            switch (LA(1)) {
                                              case ID:
                                              case LITERAL__stdcall:
                                              case LITERAL___stdcall:
                                              case LPAREN:
                                              case OPERATOR:
                                              case LITERAL_this:
                                              case LITERAL_true:
                                              case LITERAL_false:
                                              case TILDE:
                                              case STAR:
                                              case AMPERSAND:
                                              case SCOPE:
                                              case LITERAL__cdecl:
                                              case LITERAL___cdecl:
                                              case LITERAL__near:
                                              case LITERAL___near:
                                              case LITERAL__far:
                                              case LITERAL___far:
                                              case LITERAL___interrupt:
                                              case LITERAL_pascal:
                                              case LITERAL__pascal:
                                              case LITERAL___pascal: {
                                                init_declarator_list();
                                                break;
                                              }
                                              case SEMICOLON: {
                                                break;
                                              }
                                              default: {
                                                throw ANTLR_USE_NAMESPACE(antlr)
                                                    NoViableAltException(
                                                        LT(1), getFilename());
                                              }
                                            }
                                          }
                                          match(SEMICOLON);
                                          if (inputState->guessing == 0) {
#line 505 "CPP_parser.g"
                                            end_of_stmt();
#line 1151 "CPPParser.cpp"
                                          }
                                        }
                                      } catch (ANTLR_USE_NAMESPACE(antlr)
                                                   RecognitionException& pe) {
                                        synPredMatched61 = false;
                                      }
                                      rewind(_m61);
                                      inputState->guessing--;
                                    }
                                    if (synPredMatched61) {
                                      if (inputState->guessing == 0) {
#line 506 "CPP_parser.g"
                                        if (statementTrace >= 1) {
                                          printf(
                                              "%d external_declaration "
                                              "Templated class forward "
                                              "declaration\n",
                                              LT(1)->getLine());
                                        }

#line 1167 "CPPParser.cpp"
                                      }
                                      declaration_specifiers();
                                      {
                                        switch (LA(1)) {
                                          case ID:
                                          case LITERAL__stdcall:
                                          case LITERAL___stdcall:
                                          case LPAREN:
                                          case OPERATOR:
                                          case LITERAL_this:
                                          case LITERAL_true:
                                          case LITERAL_false:
                                          case TILDE:
                                          case STAR:
                                          case AMPERSAND:
                                          case SCOPE:
                                          case LITERAL__cdecl:
                                          case LITERAL___cdecl:
                                          case LITERAL__near:
                                          case LITERAL___near:
                                          case LITERAL__far:
                                          case LITERAL___far:
                                          case LITERAL___interrupt:
                                          case LITERAL_pascal:
                                          case LITERAL__pascal:
                                          case LITERAL___pascal: {
                                            init_declarator_list();
                                            break;
                                          }
                                          case SEMICOLON: {
                                            break;
                                          }
                                          default: {
                                            throw ANTLR_USE_NAMESPACE(antlr)
                                                NoViableAltException(
                                                    LT(1), getFilename());
                                          }
                                        }
                                      }
                                      match(SEMICOLON);
                                      if (inputState->guessing == 0) {
#line 509 "CPP_parser.g"
                                        end_of_stmt();
#line 1212 "CPPParser.cpp"
                                      }
                                    } else {
                                      bool synPredMatched64 = false;
                                      if (((_tokenSet_2.member(LA(1))) &&
                                           (_tokenSet_3.member(LA(2))))) {
                                        int _m64 = mark();
                                        synPredMatched64 = true;
                                        inputState->guessing++;
                                        try {
                                          {
                                            declaration_specifiers();
                                            function_declarator(0);
                                            match(SEMICOLON);
                                          }
                                        } catch (ANTLR_USE_NAMESPACE(antlr)
                                                     RecognitionException& pe) {
                                          synPredMatched64 = false;
                                        }
                                        rewind(_m64);
                                        inputState->guessing--;
                                      }
                                      if (synPredMatched64) {
                                        if (inputState->guessing == 0) {
#line 513 "CPP_parser.g"
                                          if (statementTrace >= 1) {
                                            printf(
                                                "%d external_declaration "
                                                "Templated function "
                                                "declaration\n",
                                                LT(1)->getLine());
                                          }

#line 1240 "CPPParser.cpp"
                                        }
                                        declaration();
                                      } else {
                                        bool synPredMatched66 = false;
                                        if (((_tokenSet_14.member(LA(1))) &&
                                             (_tokenSet_15.member(LA(2))))) {
                                          int _m66 = mark();
                                          synPredMatched66 = true;
                                          inputState->guessing++;
                                          try {
                                            {
                                              declaration_specifiers();
                                              function_declarator(1);
                                              match(LCURLY);
                                            }
                                          } catch (ANTLR_USE_NAMESPACE(
                                              antlr) RecognitionException& pe) {
                                            synPredMatched66 = false;
                                          }
                                          rewind(_m66);
                                          inputState->guessing--;
                                        }
                                        if (synPredMatched66) {
                                          if (inputState->guessing == 0) {
#line 520 "CPP_parser.g"
                                            if (statementTrace >= 1) {
                                              printf(
                                                  "%d external_declaration_10c "
                                                  "Templated function "
                                                  "definition\n",
                                                  LT(1)->getLine());
                                            }

#line 1269 "CPPParser.cpp"
                                          }
                                          function_definition();
                                        } else {
                                          bool synPredMatched68 = false;
                                          if (((_tokenSet_8.member(LA(1))) &&
                                               (_tokenSet_9.member(LA(2))))) {
                                            int _m68 = mark();
                                            synPredMatched68 = true;
                                            inputState->guessing++;
                                            try {
                                              {
                                                ctor_decl_spec();
                                                if (!(qualifiedItemIsOneOf(
                                                        qiCtor))) {
                                                  throw ANTLR_USE_NAMESPACE(
                                                      antlr)
                                                      SemanticException(
                                                          "qualifiedItemIsOneOf"
                                                          "(qiCtor)");
                                                }
                                              }
                                            } catch (
                                                ANTLR_USE_NAMESPACE(antlr)
                                                    RecognitionException& pe) {
                                              synPredMatched68 = false;
                                            }
                                            rewind(_m68);
                                            inputState->guessing--;
                                          }
                                          if (synPredMatched68) {
                                            if (inputState->guessing == 0) {
#line 531 "CPP_parser.g"
                                              if (statementTrace >= 1) {
                                                printf(
                                                    "%d external_declaration "
                                                    "Templated constructor "
                                                    "definition\n",
                                                    LT(1)->getLine());
                                              }

#line 1298 "CPPParser.cpp"
                                            }
                                            ctor_definition();
                                          } else {
                                            throw ANTLR_USE_NAMESPACE(antlr)
                                                NoViableAltException(
                                                    LT(1), getFilename());
                                          }
                                        }
                                      }
                                    }
                                  }
                                  if (inputState->guessing == 0) {
#line 536 "CPP_parser.g"
                                    endTemplateDeclaration();
#line 1310 "CPPParser.cpp"
                                  }
                                } else if ((_tokenSet_2.member(LA(1))) &&
                                           (_tokenSet_3.member(LA(2)))) {
                                  if (inputState->guessing == 0) {
#line 545 "CPP_parser.g"
                                    if (statementTrace >= 1) {
                                      printf(
                                          "%d external_declaration "
                                          "Declaration\n",
                                          LT(1)->getLine());
                                    }

#line 1319 "CPPParser.cpp"
                                  }
                                  declaration();
                                } else {
                                  throw ANTLR_USE_NAMESPACE(antlr)
                                      NoViableAltException(LT(1),
                                                           getFilename());
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_19);
    } else {
      throw;
    }
  }
}

CPPParser::TypeSpecifier CPPParser::class_specifier() {
#line 1016 "CPP_parser.g"
  CPPParser::TypeSpecifier ts = tsInvalid;
#line 1342 "CPPParser.cpp"

  try {  // for error handling
    {
      switch (LA(1)) {
        case LITERAL_class: {
          match(LITERAL_class);
          if (inputState->guessing == 0) {
#line 1018 "CPP_parser.g"
            ts = tsCLASS;
#line 1353 "CPPParser.cpp"
          }
          break;
        }
        case LITERAL_struct: {
          match(LITERAL_struct);
          if (inputState->guessing == 0) {
#line 1019 "CPP_parser.g"
            ts = tsSTRUCT;
#line 1363 "CPPParser.cpp"
          }
          break;
        }
        case LITERAL_union: {
          match(LITERAL_union);
          if (inputState->guessing == 0) {
#line 1020 "CPP_parser.g"
            ts = tsUNION;
#line 1373 "CPPParser.cpp"
          }
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_12);
    } else {
      throw;
    }
  }
  return ts;
}

void CPPParser::class_decl_or_def(FunctionSpecifier fs) {
#line 1042 "CPP_parser.g"
  char* saveClass;
  char* id;
  TypeSpecifier ts = tsInvalid;  // Available for use

#line 1403 "CPPParser.cpp"

  try {  // for error handling
    {
      switch (LA(1)) {
        case LITERAL_class: {
          match(LITERAL_class);
          if (inputState->guessing == 0) {
#line 1048 "CPP_parser.g"
            ts = tsCLASS;
#line 1414 "CPPParser.cpp"
          }
          break;
        }
        case LITERAL_struct: {
          match(LITERAL_struct);
          if (inputState->guessing == 0) {
#line 1049 "CPP_parser.g"
            ts = tsSTRUCT;
#line 1424 "CPPParser.cpp"
          }
          break;
        }
        case LITERAL_union: {
          match(LITERAL_union);
          if (inputState->guessing == 0) {
#line 1050 "CPP_parser.g"
            ts = tsUNION;
#line 1434 "CPPParser.cpp"
          }
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
    class_prefix();
    {  // ( ... )*
      for (;;) {
        if ((LA(1) == LITERAL_GFIGNORE)) {
          match(LITERAL_GFIGNORE);
          match(LPAREN);
          expression();
          match(RPAREN);
          if (inputState->guessing == 0) {
#line 1053 "CPP_parser.g"
#line 1454 "CPPParser.cpp"
          }
        } else {
          goto _loop208;
        }
      }
    _loop208:;
    }  // ( ... )*
    {
      switch (LA(1)) {
        case ID:
        case OPERATOR:
        case LITERAL_this:
        case LITERAL_true:
        case LITERAL_false:
        case SCOPE: {
          id = qualified_id();
          {
            switch (LA(1)) {
              case SEMICOLON:
              case ID:
              case LITERAL__stdcall:
              case LITERAL___stdcall:
              case LPAREN:
              case OPERATOR:
              case LITERAL_this:
              case LITERAL_true:
              case LITERAL_false:
              case TILDE:
              case STAR:
              case AMPERSAND:
              case SCOPE:
              case LITERAL__cdecl:
              case LITERAL___cdecl:
              case LITERAL__near:
              case LITERAL___near:
              case LITERAL__far:
              case LITERAL___far:
              case LITERAL___interrupt:
              case LITERAL_pascal:
              case LITERAL__pascal:
              case LITERAL___pascal: {
                if (inputState->guessing == 0) {
#line 1058 "CPP_parser.g"
                  classForwardDeclaration(ts, fs, id);
#line 1503 "CPPParser.cpp"
                }
                break;
              }
              case LCURLY:
              case COLON: {
                if (inputState->guessing == 0) {
#line 1060 "CPP_parser.g"
                  saveClass = enclosingClass;
                  enclosingClass = symbols->strdup(id);

#line 1515 "CPPParser.cpp"
                }
                {
                  switch (LA(1)) {
                    case COLON: {
                      base_clause();
                      break;
                    }
                    case LCURLY: {
                      break;
                    }
                    default: {
                      throw ANTLR_USE_NAMESPACE(antlr)
                          NoViableAltException(LT(1), getFilename());
                    }
                  }
                }
                match(LCURLY);
                if (inputState->guessing == 0) {
#line 1065 "CPP_parser.g"
                  beginClassDefinition(ts, id);
#line 1538 "CPPParser.cpp"
                }
                {  // ( ... )*
                  for (;;) {
                    if ((_tokenSet_20.member(LA(1)))) {
                      member_declaration();
                    } else {
                      goto _loop215;
                    }
                  }
                _loop215:;
                }  // ( ... )*
                if (inputState->guessing == 0) {
#line 1067 "CPP_parser.g"
                  endClassDefinition();
#line 1555 "CPPParser.cpp"
                }
                match(RCURLY);
                if (inputState->guessing == 0) {
#line 1069 "CPP_parser.g"
                  enclosingClass = saveClass;
#line 1561 "CPPParser.cpp"
                }
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          break;
        }
        case LCURLY: {
          match(LCURLY);
          if (inputState->guessing == 0) {
#line 1073 "CPP_parser.g"
            saveClass = enclosingClass;
            enclosingClass = (char*)"__anonymous";
#line 1579 "CPPParser.cpp"
          }
          if (inputState->guessing == 0) {
#line 1074 "CPP_parser.g"
            beginClassDefinition(ts, "anonymous");
#line 1584 "CPPParser.cpp"
          }
          {  // ( ... )*
            for (;;) {
              if ((_tokenSet_20.member(LA(1)))) {
                member_declaration();
              } else {
                goto _loop217;
              }
            }
          _loop217:;
          }  // ( ... )*
          if (inputState->guessing == 0) {
#line 1076 "CPP_parser.g"
            endClassDefinition();
#line 1601 "CPPParser.cpp"
          }
          match(RCURLY);
          if (inputState->guessing == 0) {
#line 1078 "CPP_parser.g"
            enclosingClass = saveClass;
#line 1607 "CPPParser.cpp"
          }
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_21);
    } else {
      throw;
    }
  }
}

void CPPParser::init_declarator_list() {
  try {  // for error handling
    init_declarator();
    {  // ( ... )*
      for (;;) {
        if ((LA(1) == COMMA)) {
          match(COMMA);
          init_declarator();
        } else {
          goto _loop242;
        }
      }
    _loop242:;
    }  // ( ... )*
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_22);
    } else {
      throw;
    }
  }
}

void CPPParser::enum_specifier() {
#line 1108 "CPP_parser.g"
  char* id;
#line 1659 "CPPParser.cpp"

  try {  // for error handling
    match(LITERAL_enum);
    {
      switch (LA(1)) {
        case LCURLY: {
          match(LCURLY);
          enumerator_list();
          match(RCURLY);
          break;
        }
        case ID:
        case OPERATOR:
        case LITERAL_this:
        case LITERAL_true:
        case LITERAL_false:
        case SCOPE: {
          id = qualified_id();
          if (inputState->guessing == 0) {
#line 1116 "CPP_parser.g"
            beginEnumDefinition(id);
#line 1683 "CPPParser.cpp"
          }
          {
            switch (LA(1)) {
              case LCURLY: {
                match(LCURLY);
                enumerator_list();
                match(RCURLY);
                break;
              }
              case SEMICOLON:
              case ID:
              case COLON:
              case LITERAL__stdcall:
              case LITERAL___stdcall:
              case LPAREN:
              case OPERATOR:
              case LITERAL_this:
              case LITERAL_true:
              case LITERAL_false:
              case TILDE:
              case STAR:
              case AMPERSAND:
              case SCOPE:
              case LITERAL__cdecl:
              case LITERAL___cdecl:
              case LITERAL__near:
              case LITERAL___near:
              case LITERAL__far:
              case LITERAL___far:
              case LITERAL___interrupt:
              case LITERAL_pascal:
              case LITERAL__pascal:
              case LITERAL___pascal: {
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          if (inputState->guessing == 0) {
#line 1118 "CPP_parser.g"
            endEnumDefinition();
#line 1730 "CPPParser.cpp"
          }
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_23);
    } else {
      throw;
    }
  }
}

void CPPParser::declaration_specifiers() {
  ANTLR_USE_NAMESPACE(antlr)
  RefToken gfArr = ANTLR_USE_NAMESPACE(antlr) nullToken;
  ANTLR_USE_NAMESPACE(antlr)
  RefToken gfArrList = ANTLR_USE_NAMESPACE(antlr) nullToken;
  ANTLR_USE_NAMESPACE(antlr)
  RefToken gfArrElem = ANTLR_USE_NAMESPACE(antlr) nullToken;
#line 885 "CPP_parser.g"
  // Locals
  bool td = false;              // For typedef
  bool fd = false;              // For friend
  StorageClass sc = scInvalid;  // auto,register,static,extern,mutable
  TypeQualifier tq =
      tqInvalid;  // const,volatile  // aka cv_qualifier See type_qualifier
  TypeSpecifier ts = tsInvalid;  // char,int,double, etc., class,struct,union
  FunctionSpecifier fs = fsInvalid;  // inline,virtual,explicit

#line 1764 "CPPParser.cpp"

  try {  // for error handling
    if (inputState->guessing == 0) {
#line 895 "CPP_parser.g"

      // Global flags to allow for nested declarations
      _td = false;      // For typedef
      _fd = false;      // For friend
      _sc = scInvalid;  // For StorageClass    //
                        // auto,register,static,extern,mutable
      _tq = tqInvalid;  // For TypeQualifier  // aka cv_qualifier See
                        // type_qualifier
      _ts = tsInvalid;  // For TypeSpecifier
      _fs = fsInvalid;  // For FunctionSpecifier  // inline,virtual,explicit

#line 1779 "CPPParser.cpp"
    }
    {
      {  // ( ... )*
        for (;;) {
          switch (LA(1)) {
            case LITERAL_typedef: {
              match(LITERAL_typedef);
              if (inputState->guessing == 0) {
#line 906 "CPP_parser.g"
                td = true;
#line 1791 "CPPParser.cpp"
              }
              break;
            }
            case LITERAL_friend: {
              match(LITERAL_friend);
              if (inputState->guessing == 0) {
#line 907 "CPP_parser.g"
                fd = true;
#line 1801 "CPPParser.cpp"
              }
              break;
            }
            case LITERAL_extern:
            case LITERAL_auto:
            case LITERAL_register:
            case LITERAL_static:
            case LITERAL_mutable: {
              sc = storage_class_specifier();
              break;
            }
            case LITERAL_const:
            case LITERAL___const:
            case LITERAL_volatile:
            case LITERAL___volatile__: {
              tq = type_qualifier();
              break;
            }
            case LITERAL_struct:
            case LITERAL_union:
            case LITERAL_class: {
              ts = class_specifier();
              break;
            }
            case LITERAL_enum: {
              match(LITERAL_enum);
              break;
            }
            case LITERAL_inline:
            case LITERAL__inline:
            case LITERAL___inline:
            case LITERAL_virtual:
            case LITERAL_explicit: {
              fs = function_specifier();
              break;
            }
            case LITERAL__stdcall:
            case LITERAL___stdcall: {
              {
                switch (LA(1)) {
                  case LITERAL__stdcall: {
                    match(LITERAL__stdcall);
                    break;
                  }
                  case LITERAL___stdcall: {
                    match(LITERAL___stdcall);
                    break;
                  }
                  default: {
                    throw ANTLR_USE_NAMESPACE(antlr)
                        NoViableAltException(LT(1), getFilename());
                  }
                }
              }
              break;
            }
            case LITERAL_GFEXCLUDE: {
              match(LITERAL_GFEXCLUDE);
              if (inputState->guessing == 0) {
#line 914 "CPP_parser.g"
                tq |= tqGFEXCLUDE;
                _tq |= tqGFEXCLUDE;
#line 1872 "CPPParser.cpp"
              }
              break;
            }
            case LITERAL_GFINCLUDE: {
              match(LITERAL_GFINCLUDE);
              if (inputState->guessing == 0) {
#line 915 "CPP_parser.g"
                tq |= tqGFINCLUDE;
                _tq |= tqGFINCLUDE;
#line 1882 "CPPParser.cpp"
              }
              break;
            }
            case LITERAL_GFID: {
              match(LITERAL_GFID);
              if (inputState->guessing == 0) {
#line 916 "CPP_parser.g"
                tq |= tqGFID;
                _tq |= tqGFID;
#line 1892 "CPPParser.cpp"
              }
              break;
            }
            case LITERAL_GFUNREAD: {
              match(LITERAL_GFUNREAD);
              if (inputState->guessing == 0) {
#line 917 "CPP_parser.g"
                tq |= tqGFUNREAD;
                _tq |= tqGFUNREAD;
#line 1902 "CPPParser.cpp"
              }
              break;
            }
            case LITERAL_GFARRAYSIZE: {
              match(LITERAL_GFARRAYSIZE);
              match(LPAREN);
              gfArr = LT(1);
              match(ID);
              match(RPAREN);
              if (inputState->guessing == 0) {
#line 918 "CPP_parser.g"
                gfArraySize(gfArr->getText().data());
#line 1916 "CPPParser.cpp"
              }
              break;
            }
            case LITERAL_GFARRAYSIZES: {
              match(LITERAL_GFARRAYSIZES);
              match(LPAREN);
              gfArrList = LT(1);
              match(StringLiteral);
              match(RPAREN);
              if (inputState->guessing == 0) {
#line 919 "CPP_parser.g"
                gfArraySize(gfArrList->getText().data());
#line 1930 "CPPParser.cpp"
              }
              break;
            }
            case LITERAL_GFARRAYELEMSIZE: {
              match(LITERAL_GFARRAYELEMSIZE);
              match(LPAREN);
              gfArrElem = LT(1);
              match(ID);
              match(RPAREN);
              if (inputState->guessing == 0) {
#line 920 "CPP_parser.g"
                gfArrayElemSize(gfArrElem->getText().data());
#line 1944 "CPPParser.cpp"
              }
              break;
            }
            default: { goto _loop174; }
          }
        }
      _loop174:;
      }  // ( ... )*
      ts = type_specifier();
    }
    if (inputState->guessing == 0) {
#line 924 "CPP_parser.g"
      declarationSpecifier(td, fd, sc, tq, ts, fs);
#line 1961 "CPPParser.cpp"
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_24);
    } else {
      throw;
    }
  }
}

void CPPParser::function_declarator(int definition) {
  try {  // for error handling
    bool synPredMatched298 = false;
    if (((_tokenSet_25.member(LA(1))) && (_tokenSet_26.member(LA(2))))) {
      int _m298 = mark();
      synPredMatched298 = true;
      inputState->guessing++;
      try {
        { ptr_operator(); }
      } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
        synPredMatched298 = false;
      }
      rewind(_m298);
      inputState->guessing--;
    }
    if (synPredMatched298) {
      ptr_operator();
      function_declarator(definition);
    } else if ((_tokenSet_27.member(LA(1))) && (_tokenSet_28.member(LA(2)))) {
      function_direct_declarator(definition);
    } else {
      throw ANTLR_USE_NAMESPACE(antlr)
          NoViableAltException(LT(1), getFilename());
    }

  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_29);
    } else {
      throw;
    }
  }
}

void CPPParser::declaration() {
  try {  // for error handling
    bool synPredMatched159 = false;
    if (((LA(1) == LITERAL_extern) && (LA(2) == StringLiteral))) {
      int _m159 = mark();
      synPredMatched159 = true;
      inputState->guessing++;
      try {
        {
          match(LITERAL_extern);
          match(StringLiteral);
        }
      } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
        synPredMatched159 = false;
      }
      rewind(_m159);
      inputState->guessing--;
    }
    if (synPredMatched159) {
      linkage_specification();
    } else if ((_tokenSet_12.member(LA(1))) && (_tokenSet_18.member(LA(2)))) {
      if (inputState->guessing == 0) {
#line 853 "CPP_parser.g"
        beginDeclaration();
#line 2044 "CPPParser.cpp"
      }
      declaration_specifiers();
      {
        switch (LA(1)) {
          case ID:
          case LITERAL__stdcall:
          case LITERAL___stdcall:
          case LPAREN:
          case OPERATOR:
          case LITERAL_this:
          case LITERAL_true:
          case LITERAL_false:
          case TILDE:
          case STAR:
          case AMPERSAND:
          case SCOPE:
          case LITERAL__cdecl:
          case LITERAL___cdecl:
          case LITERAL__near:
          case LITERAL___near:
          case LITERAL__far:
          case LITERAL___far:
          case LITERAL___interrupt:
          case LITERAL_pascal:
          case LITERAL__pascal:
          case LITERAL___pascal: {
            init_declarator_list();
            break;
          }
          case SEMICOLON: {
            break;
          }
          default: {
            throw ANTLR_USE_NAMESPACE(antlr)
                NoViableAltException(LT(1), getFilename());
          }
        }
      }
      match(SEMICOLON);
      if (inputState->guessing == 0) {
#line 854 "CPP_parser.g"
        end_of_stmt();
#line 2089 "CPPParser.cpp"
      }
      if (inputState->guessing == 0) {
#line 855 "CPP_parser.g"
        endDeclaration();
#line 2094 "CPPParser.cpp"
      }
    } else if ((LA(1) == LITERAL_using)) {
      using_statement();
    } else {
      throw ANTLR_USE_NAMESPACE(antlr)
          NoViableAltException(LT(1), getFilename());
    }

  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_30);
    } else {
      throw;
    }
  }
}

void CPPParser::template_head() {
  try {  // for error handling
    match(LITERAL_template);
    match(LESSTHAN);
    template_parameter_list();
    match(GREATERTHAN);
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_31);
    } else {
      throw;
    }
  }
}

CPPParser::FunctionSpecifier CPPParser::function_specifier() {
#line 937 "CPP_parser.g"
  CPPParser::FunctionSpecifier fs = fsInvalid;
#line 2136 "CPPParser.cpp"

  try {  // for error handling
    switch (LA(1)) {
      case LITERAL_inline:
      case LITERAL__inline:
      case LITERAL___inline: {
        {
          switch (LA(1)) {
            case LITERAL_inline: {
              match(LITERAL_inline);
              break;
            }
            case LITERAL__inline: {
              match(LITERAL__inline);
              break;
            }
            case LITERAL___inline: {
              match(LITERAL___inline);
              break;
            }
            default: {
              throw ANTLR_USE_NAMESPACE(antlr)
                  NoViableAltException(LT(1), getFilename());
            }
          }
        }
        if (inputState->guessing == 0) {
#line 938 "CPP_parser.g"
          fs = fsINLINE;
#line 2170 "CPPParser.cpp"
        }
        break;
      }
      case LITERAL_virtual: {
        match(LITERAL_virtual);
        if (inputState->guessing == 0) {
#line 939 "CPP_parser.g"
          fs = fsVIRTUAL;
#line 2180 "CPPParser.cpp"
        }
        break;
      }
      case LITERAL_explicit: {
        match(LITERAL_explicit);
        if (inputState->guessing == 0) {
#line 940 "CPP_parser.g"
          fs = fsEXPLICIT;
#line 2190 "CPPParser.cpp"
        }
        break;
      }
      default: {
        throw ANTLR_USE_NAMESPACE(antlr)
            NoViableAltException(LT(1), getFilename());
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_14);
    } else {
      throw;
    }
  }
  return fs;
}

void CPPParser::dtor_head(int definition) {
  try {  // for error handling
    dtor_decl_spec();
    dtor_declarator(definition);
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_32);
    } else {
      throw;
    }
  }
}

void CPPParser::dtor_body() {
  try {  // for error handling
    compound_statement();
    if (inputState->guessing == 0) {
#line 1476 "CPP_parser.g"
      endDestructorDefinition();
#line 2236 "CPPParser.cpp"
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_30);
    } else {
      throw;
    }
  }
}

void CPPParser::ctor_decl_spec() {
  try {  // for error handling
    {    // ( ... )*
      for (;;) {
        switch (LA(1)) {
          case LITERAL_inline:
          case LITERAL__inline:
          case LITERAL___inline: {
            {
              switch (LA(1)) {
                case LITERAL_inline: {
                  match(LITERAL_inline);
                  break;
                }
                case LITERAL__inline: {
                  match(LITERAL__inline);
                  break;
                }
                case LITERAL___inline: {
                  match(LITERAL___inline);
                  break;
                }
                default: {
                  throw ANTLR_USE_NAMESPACE(antlr)
                      NoViableAltException(LT(1), getFilename());
                }
              }
            }
            break;
          }
          case LITERAL_explicit: {
            match(LITERAL_explicit);
            break;
          }
          default: { goto _loop311; }
        }
      }
    _loop311:;
    }  // ( ... )*
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_33);
    } else {
      throw;
    }
  }
}

void CPPParser::ctor_definition() {
  try {  // for error handling
    ctor_head();
    ctor_body();
    if (inputState->guessing == 0) {
#line 1385 "CPP_parser.g"
      endConstructorDefinition();
#line 2316 "CPPParser.cpp"
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_30);
    } else {
      throw;
    }
  }
}

char* CPPParser::scope_override() {
#line 2226 "CPP_parser.g"
  char* s = NULL;
#line 2332 "CPPParser.cpp"
  ANTLR_USE_NAMESPACE(antlr) RefToken id = ANTLR_USE_NAMESPACE(antlr) nullToken;
#line 2226 "CPP_parser.g"

  static char sitem[CPPParser_MaxQualifiedItemSize + 1];
  sitem[0] = '\0';

#line 2339 "CPPParser.cpp"

  try {  // for error handling
    {
      switch (LA(1)) {
        case SCOPE: {
          match(SCOPE);
          if (inputState->guessing == 0) {
#line 2232 "CPP_parser.g"
            strcat(sitem, "::");
#line 2350 "CPPParser.cpp"
          }
          break;
        }
        case ID:
        case OPERATOR:
        case LITERAL_this:
        case LITERAL_true:
        case LITERAL_false:
        case TILDE:
        case STAR: {
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
    {  // ( ... )*
      for (;;) {
        if (((LA(1) == ID) && (LA(2) == LESSTHAN || LA(2) == SCOPE)) &&
            (scopedItem())) {
          id = LT(1);
          match(ID);
          {
            switch (LA(1)) {
              case LESSTHAN: {
                match(LESSTHAN);
                template_argument_list();
                match(GREATERTHAN);
                break;
              }
              case SCOPE: {
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          match(SCOPE);
          {
            switch (LA(1)) {
              case LITERAL_template: {
                match(LITERAL_template);
                break;
              }
              case ID:
              case OPERATOR:
              case LITERAL_this:
              case LITERAL_true:
              case LITERAL_false:
              case TILDE:
              case STAR: {
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          if (inputState->guessing == 0) {
#line 2238 "CPP_parser.g"

            CPPSymbol* cs = (CPPSymbol*)symbols->lookup((id->getText()).data());
            strcat(sitem, (id->getText()).data());
            strcat(sitem, "::");

#line 2425 "CPPParser.cpp"
          }
        } else {
          goto _loop587;
        }
      }
    _loop587:;
    }  // ( ... )*
    if (inputState->guessing == 0) {
#line 2244 "CPP_parser.g"
      s = sitem;
#line 2438 "CPPParser.cpp"
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_34);
    } else {
      throw;
    }
  }
  return s;
}

void CPPParser::conversion_function_decl_or_def() {
#line 1302 "CPP_parser.g"
  CPPParser::TypeQualifier tq;
#line 2455 "CPPParser.cpp"

  try {  // for error handling
    match(OPERATOR);
    declaration_specifiers();
    {
      switch (LA(1)) {
        case STAR: {
          match(STAR);
          break;
        }
        case AMPERSAND: {
          match(AMPERSAND);
          break;
        }
        case LESSTHAN:
        case LPAREN: {
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
    {
      switch (LA(1)) {
        case LESSTHAN: {
          match(LESSTHAN);
          template_parameter_list();
          match(GREATERTHAN);
          break;
        }
        case LPAREN: {
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
    match(LPAREN);
    {
      switch (LA(1)) {
        case LITERAL_typedef:
        case LITERAL_enum:
        case ID:
        case LITERAL_inline:
        case LITERAL_friend:
        case LITERAL_extern:
        case LITERAL_struct:
        case LITERAL_union:
        case LITERAL_class:
        case LITERAL__stdcall:
        case LITERAL___stdcall:
        case LITERAL_GFEXCLUDE:
        case LITERAL_GFINCLUDE:
        case LITERAL_GFID:
        case LITERAL_GFUNREAD:
        case LITERAL_GFARRAYSIZE:
        case LPAREN:
        case LITERAL_GFARRAYSIZES:
        case LITERAL_GFARRAYELEMSIZE:
        case LITERAL_auto:
        case LITERAL_register:
        case LITERAL_static:
        case LITERAL_mutable:
        case LITERAL__inline:
        case LITERAL___inline:
        case LITERAL_virtual:
        case LITERAL_explicit:
        case LITERAL_typename:
        case LITERAL_char:
        case LITERAL_wchar_t:
        case LITERAL_bool:
        case LITERAL_short:
        case LITERAL_int:
        case 50:
        case 51:
        case 52:
        case 53:
        case 54:
        case 55:
        case 56:
        case 57:
        case 58:
        case 59:
        case 60:
        case 61:
        case 62:
        case 63:
        case 64:
        case 65:
        case 66:
        case 67:
        case LITERAL_long:
        case LITERAL_signed:
        case LITERAL_unsigned:
        case LITERAL_float:
        case LITERAL_double:
        case LITERAL_void:
        case LITERAL__declspec:
        case LITERAL___declspec:
        case LITERAL_const:
        case LITERAL___const:
        case LITERAL_volatile:
        case LITERAL___volatile__:
        case OPERATOR:
        case LITERAL_this:
        case LITERAL_true:
        case LITERAL_false:
        case TILDE:
        case STAR:
        case AMPERSAND:
        case ELLIPSIS:
        case SCOPE:
        case LITERAL__cdecl:
        case LITERAL___cdecl:
        case LITERAL__near:
        case LITERAL___near:
        case LITERAL__far:
        case LITERAL___far:
        case LITERAL___interrupt:
        case LITERAL_pascal:
        case LITERAL__pascal:
        case LITERAL___pascal: {
          parameter_list();
          break;
        }
        case RPAREN: {
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
    match(RPAREN);
    {  // ( ... )*
      for (;;) {
        if (((LA(1) >= LITERAL_const && LA(1) <= LITERAL___volatile__))) {
          tq = type_qualifier();
        } else {
          goto _loop293;
        }
      }
    _loop293:;
    }  // ( ... )*
    {
      switch (LA(1)) {
        case LITERAL_throw: {
          exception_specification();
          break;
        }
        case SEMICOLON:
        case LCURLY: {
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
    {
      switch (LA(1)) {
        case LCURLY: {
          compound_statement();
          break;
        }
        case SEMICOLON: {
          match(SEMICOLON);
          if (inputState->guessing == 0) {
#line 1311 "CPP_parser.g"
            end_of_stmt();
#line 2645 "CPPParser.cpp"
          }
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_30);
    } else {
      throw;
    }
  }
}

void CPPParser::function_definition() {
  try {  // for error handling
    if (inputState->guessing == 0) {
#line 822 "CPP_parser.g"

      beginFunctionDefinition();

#line 2674 "CPPParser.cpp"
    }
    {
      if (((_tokenSet_12.member(LA(1))) && (_tokenSet_13.member(LA(2)))) &&
          ((!(LA(1) == SCOPE || LA(1) == ID) ||
            qualifiedItemIsOneOf(qiType | qiCtor)))) {
        declaration_specifiers();
        function_declarator(1);
        {
          if ((_tokenSet_35.member(LA(1))) && (_tokenSet_36.member(LA(2)))) {
            {  // ( ... )*
              for (;;) {
                if ((_tokenSet_2.member(LA(1)))) {
                  declaration();
                } else {
                  goto _loop153;
                }
              }
            _loop153:;
            }  // ( ... )*
            if (inputState->guessing == 0) {
#line 831 "CPP_parser.g"
              in_parameter_list = false;
#line 2697 "CPPParser.cpp"
            }
          } else if ((LA(1) == LCURLY) && (_tokenSet_37.member(LA(2)))) {
          } else {
            throw ANTLR_USE_NAMESPACE(antlr)
                NoViableAltException(LT(1), getFilename());
          }
        }
        compound_statement();
      } else if ((_tokenSet_38.member(LA(1))) && (_tokenSet_39.member(LA(2)))) {
        function_declarator(1);
        {
          if ((_tokenSet_35.member(LA(1))) && (_tokenSet_36.member(LA(2)))) {
            {  // ( ... )*
              for (;;) {
                if ((_tokenSet_2.member(LA(1)))) {
                  declaration();
                } else {
                  goto _loop156;
                }
              }
            _loop156:;
            }  // ( ... )*
            if (inputState->guessing == 0) {
#line 840 "CPP_parser.g"
              in_parameter_list = false;
#line 2728 "CPPParser.cpp"
            }
          } else if ((LA(1) == LCURLY) && (_tokenSet_37.member(LA(2)))) {
          } else {
            throw ANTLR_USE_NAMESPACE(antlr)
                NoViableAltException(LT(1), getFilename());
          }
        }
        compound_statement();
      } else {
        throw ANTLR_USE_NAMESPACE(antlr)
            NoViableAltException(LT(1), getFilename());
      }
    }
    if (inputState->guessing == 0) {
#line 844 "CPP_parser.g"
      endFunctionDefinition();
#line 2748 "CPPParser.cpp"
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_30);
    } else {
      throw;
    }
  }
}

void CPPParser::namespace_definition() {
  ANTLR_USE_NAMESPACE(antlr) RefToken ns = ANTLR_USE_NAMESPACE(antlr) nullToken;

  try {  // for error handling
    {
      switch (LA(1)) {
        case ID: {
          ns = LT(1);
          match(ID);
          if (inputState->guessing == 0) {
#line 568 "CPP_parser.g"
            declaratorID((ns->getText()).data(), qiNamespace);
#line 2774 "CPPParser.cpp"
          }
          break;
        }
        case LCURLY: {
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
    match(LCURLY);
    if (inputState->guessing == 0) {
#line 570 "CPP_parser.g"
      enterNewLocalScope();
#line 2792 "CPPParser.cpp"
    }
    {  // ( ... )*
      for (;;) {
        if ((_tokenSet_0.member(LA(1)))) {
          external_declaration();
        } else {
          goto _loop72;
        }
      }
    _loop72:;
    }  // ( ... )*
    if (inputState->guessing == 0) {
#line 572 "CPP_parser.g"
      exitNamespaceScope();
      exitLocalScope();
#line 2809 "CPPParser.cpp"
    }
    match(RCURLY);
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_19);
    } else {
      throw;
    }
  }
}

void CPPParser::namespace_alias_definition() {
  ANTLR_USE_NAMESPACE(antlr)
  RefToken ns2 = ANTLR_USE_NAMESPACE(antlr) nullToken;
#line 577 "CPP_parser.g"
  char* qid;
#line 2827 "CPPParser.cpp"

  try {  // for error handling
    match(LITERAL_namespace);
    ns2 = LT(1);
    match(ID);
    if (inputState->guessing == 0) {
#line 581 "CPP_parser.g"
      declaratorID((ns2->getText()).data(), qiNamespace);
#line 2836 "CPPParser.cpp"
    }
    match(ASSIGNEQUAL);
    qid = qualified_id();
    match(SEMICOLON);
    if (inputState->guessing == 0) {
#line 582 "CPP_parser.g"
      end_of_stmt();
#line 2844 "CPPParser.cpp"
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_40);
    } else {
      throw;
    }
  }
}

char* CPPParser::qualified_id() {
#line 1143 "CPP_parser.g"
  char* q = NULL;
#line 2860 "CPPParser.cpp"
  ANTLR_USE_NAMESPACE(antlr) RefToken id = ANTLR_USE_NAMESPACE(antlr) nullToken;
#line 1143 "CPP_parser.g"

  char* so = NULL;
  static char qitem[CPPParser_MaxQualifiedItemSize + 1];

#line 2867 "CPPParser.cpp"

  try {  // for error handling
    so = scope_override();
    if (inputState->guessing == 0) {
#line 1150 "CPP_parser.g"
      strcpy(qitem, so);
#line 2874 "CPPParser.cpp"
    }
    {
      switch (LA(1)) {
        case ID: {
          id = LT(1);
          match(ID);
          {
            switch (LA(1)) {
              case LESSTHAN: {
                match(LESSTHAN);
                template_argument_list();
                match(GREATERTHAN);
                break;
              }
              case GREATERTHAN:
              case SEMICOLON:
              case ID:
              case LCURLY:
              case ASSIGNEQUAL:
              case COLON:
              case LITERAL__stdcall:
              case LITERAL___stdcall:
              case LPAREN:
              case RPAREN:
              case COMMA:
              case OPERATOR:
              case LITERAL_this:
              case LITERAL_true:
              case LITERAL_false:
              case LSQUARE:
              case TILDE:
              case STAR:
              case AMPERSAND:
              case ELLIPSIS:
              case SCOPE:
              case LITERAL__cdecl:
              case LITERAL___cdecl:
              case LITERAL__near:
              case LITERAL___near:
              case LITERAL__far:
              case LITERAL___far:
              case LITERAL___interrupt:
              case LITERAL_pascal:
              case LITERAL__pascal:
              case LITERAL___pascal: {
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          if (inputState->guessing == 0) {
#line 1155 "CPP_parser.g"
            strcat(qitem, (id->getText()).data());
#line 2934 "CPPParser.cpp"
          }
          break;
        }
        case OPERATOR: {
          match(OPERATOR);
          optor();
          if (inputState->guessing == 0) {
#line 1158 "CPP_parser.g"
            strcat(qitem, "operator");
            strcat(qitem, "NYI");
#line 2945 "CPPParser.cpp"
          }
          break;
        }
        case LITERAL_this: {
          match(LITERAL_this);
          break;
        }
        case LITERAL_true:
        case LITERAL_false: {
          {
            switch (LA(1)) {
              case LITERAL_true: {
                match(LITERAL_true);
                break;
              }
              case LITERAL_false: {
                match(LITERAL_false);
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
    if (inputState->guessing == 0) {
#line 1164 "CPP_parser.g"
      q = qitem;
#line 2986 "CPPParser.cpp"
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_41);
    } else {
      throw;
    }
  }
  return q;
}

void CPPParser::member_declaration() {
#line 585 "CPP_parser.g"
  char* q;
  FunctionSpecifier fs = fsInvalid;  // inline,virtual,explicit

#line 3005 "CPPParser.cpp"

  try {  // for error handling
    {
      switch (LA(1)) {
        case LITERAL_public:
        case LITERAL_protected:
        case LITERAL_private: {
          if (inputState->guessing == 0) {
#line 799 "CPP_parser.g"
            if (statementTrace >= 1) {
              printf("%d member_declaration Access specifier\n",
                     LT(1)->getLine());
            }

#line 3019 "CPPParser.cpp"
          }
          access_specifier();
          match(COLON);
          break;
        }
        case SEMICOLON: {
          if (inputState->guessing == 0) {
#line 805 "CPP_parser.g"
            if (statementTrace >= 1) {
              printf("%d member_declaration Semicolon\n", LT(1)->getLine());
            }

#line 3032 "CPPParser.cpp"
          }
          match(SEMICOLON);
          if (inputState->guessing == 0) {
#line 808 "CPP_parser.g"
            end_of_stmt();
#line 3038 "CPPParser.cpp"
          }
          break;
        }
        case LITERAL_antlrTrace_on: {
          match(LITERAL_antlrTrace_on);
          if (inputState->guessing == 0) {
#line 812 "CPP_parser.g"
            antlrTrace(true);
#line 3048 "CPPParser.cpp"
          }
          break;
        }
        case LITERAL_antlrTrace_off: {
          match(LITERAL_antlrTrace_off);
          if (inputState->guessing == 0) {
#line 815 "CPP_parser.g"
            antlrTrace(false);
#line 3058 "CPPParser.cpp"
          }
          break;
        }
        default:
          bool synPredMatched77 = false;
          if (((LA(1) == LITERAL_template) && (LA(2) == LESSTHAN))) {
            int _m77 = mark();
            synPredMatched77 = true;
            inputState->guessing++;
            try {
              {
                match(LITERAL_template);
                match(LESSTHAN);
                match(GREATERTHAN);
              }
            } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
              synPredMatched77 = false;
            }
            rewind(_m77);
            inputState->guessing--;
          }
          if (synPredMatched77) {
            if (inputState->guessing == 0) {
#line 593 "CPP_parser.g"
              if (statementTrace >= 1) {
                printf(
                    "%d member_declaration Template explicit-specialisation\n",
                    LT(1)->getLine());
              }

#line 3087 "CPPParser.cpp"
            }
            match(LITERAL_template);
            match(LESSTHAN);
            match(GREATERTHAN);
            member_declaration();
          } else {
            bool synPredMatched79 = false;
            if (((_tokenSet_2.member(LA(1))) && (_tokenSet_3.member(LA(2))))) {
              int _m79 = mark();
              synPredMatched79 = true;
              inputState->guessing++;
              try {
                { match(LITERAL_typedef); }
              } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
                synPredMatched79 = false;
              }
              rewind(_m79);
              inputState->guessing--;
            }
            if (synPredMatched79) {
              {
                bool synPredMatched82 = false;
                if (((LA(1) == LITERAL_typedef) &&
                     ((LA(2) >= LITERAL_struct && LA(2) <= LITERAL_class)))) {
                  int _m82 = mark();
                  synPredMatched82 = true;
                  inputState->guessing++;
                  try {
                    {
                      match(LITERAL_typedef);
                      class_specifier();
                    }
                  } catch (ANTLR_USE_NAMESPACE(antlr)
                               RecognitionException& pe) {
                    synPredMatched82 = false;
                  }
                  rewind(_m82);
                  inputState->guessing--;
                }
                if (synPredMatched82) {
                  if (inputState->guessing == 0) {
#line 602 "CPP_parser.g"
                    if (statementTrace >= 1) {
                      printf("%d member_declaration Typedef class type\n",
                             LT(1)->getLine());
                    }

#line 3136 "CPPParser.cpp"
                  }
                  match(LITERAL_typedef);
                  class_decl_or_def(fs);
                  if (inputState->guessing == 0) {
#line 605 "CPP_parser.g"
                    _td = true;
#line 3143 "CPPParser.cpp"
                  }
                  {
                    switch (LA(1)) {
                      case ID:
                      case LITERAL__stdcall:
                      case LITERAL___stdcall:
                      case LPAREN:
                      case OPERATOR:
                      case LITERAL_this:
                      case LITERAL_true:
                      case LITERAL_false:
                      case TILDE:
                      case STAR:
                      case AMPERSAND:
                      case SCOPE:
                      case LITERAL__cdecl:
                      case LITERAL___cdecl:
                      case LITERAL__near:
                      case LITERAL___near:
                      case LITERAL__far:
                      case LITERAL___far:
                      case LITERAL___interrupt:
                      case LITERAL_pascal:
                      case LITERAL__pascal:
                      case LITERAL___pascal: {
                        init_declarator_list();
                        break;
                      }
                      case SEMICOLON: {
                        break;
                      }
                      default: {
                        throw ANTLR_USE_NAMESPACE(antlr)
                            NoViableAltException(LT(1), getFilename());
                      }
                    }
                  }
                  match(SEMICOLON);
                  if (inputState->guessing == 0) {
#line 605 "CPP_parser.g"
                    end_of_stmt();
#line 3187 "CPPParser.cpp"
                  }
                } else {
                  bool synPredMatched85 = false;
                  if (((LA(1) == LITERAL_typedef) && (LA(2) == LITERAL_enum))) {
                    int _m85 = mark();
                    synPredMatched85 = true;
                    inputState->guessing++;
                    try {
                      {
                        match(LITERAL_typedef);
                        match(LITERAL_enum);
                      }
                    } catch (ANTLR_USE_NAMESPACE(antlr)
                                 RecognitionException& pe) {
                      synPredMatched85 = false;
                    }
                    rewind(_m85);
                    inputState->guessing--;
                  }
                  if (synPredMatched85) {
                    if (inputState->guessing == 0) {
#line 608 "CPP_parser.g"
                      if (statementTrace >= 1) {
                        printf("%d member_declaration Typedef enum type\n",
                               LT(1)->getLine());
                      }

#line 3214 "CPPParser.cpp"
                    }
                    match(LITERAL_typedef);
                    enum_specifier();
                    if (inputState->guessing == 0) {
#line 611 "CPP_parser.g"
                      _td = true;
#line 3221 "CPPParser.cpp"
                    }
                    {
                      switch (LA(1)) {
                        case ID:
                        case LITERAL__stdcall:
                        case LITERAL___stdcall:
                        case LPAREN:
                        case OPERATOR:
                        case LITERAL_this:
                        case LITERAL_true:
                        case LITERAL_false:
                        case TILDE:
                        case STAR:
                        case AMPERSAND:
                        case SCOPE:
                        case LITERAL__cdecl:
                        case LITERAL___cdecl:
                        case LITERAL__near:
                        case LITERAL___near:
                        case LITERAL__far:
                        case LITERAL___far:
                        case LITERAL___interrupt:
                        case LITERAL_pascal:
                        case LITERAL__pascal:
                        case LITERAL___pascal: {
                          init_declarator_list();
                          break;
                        }
                        case SEMICOLON: {
                          break;
                        }
                        default: {
                          throw ANTLR_USE_NAMESPACE(antlr)
                              NoViableAltException(LT(1), getFilename());
                        }
                      }
                    }
                    match(SEMICOLON);
                    if (inputState->guessing == 0) {
#line 611 "CPP_parser.g"
                      end_of_stmt();
#line 3265 "CPPParser.cpp"
                    }
                  } else {
                    bool synPredMatched88 = false;
                    if (((_tokenSet_2.member(LA(1))) &&
                         (_tokenSet_3.member(LA(2))))) {
                      int _m88 = mark();
                      synPredMatched88 = true;
                      inputState->guessing++;
                      try {
                        {
                          declaration_specifiers();
                          function_declarator(0);
                          match(SEMICOLON);
                        }
                      } catch (ANTLR_USE_NAMESPACE(antlr)
                                   RecognitionException& pe) {
                        synPredMatched88 = false;
                      }
                      rewind(_m88);
                      inputState->guessing--;
                    }
                    if (synPredMatched88) {
                      if (inputState->guessing == 0) {
#line 614 "CPP_parser.g"
                        if (statementTrace >= 1) {
                          printf(
                              "%d member_declaration Typedef function type\n",
                              LT(1)->getLine());
                        }

#line 3293 "CPPParser.cpp"
                      }
                      declaration();
                    } else if ((_tokenSet_2.member(LA(1))) &&
                               (_tokenSet_3.member(LA(2)))) {
                      if (inputState->guessing == 0) {
#line 619 "CPP_parser.g"
                        if (statementTrace >= 1) {
                          printf(
                              "%d member_declaration Typedef variable type\n",
                              LT(1)->getLine());
                        }

#line 3303 "CPPParser.cpp"
                      }
                      declaration();
                    } else {
                      throw ANTLR_USE_NAMESPACE(antlr)
                          NoViableAltException(LT(1), getFilename());
                    }
                  }
                }
              }
            } else {
              bool synPredMatched92 = false;
              if (((LA(1) == LITERAL_template) && (LA(2) == LESSTHAN))) {
                int _m92 = mark();
                synPredMatched92 = true;
                inputState->guessing++;
                try {
                  {
                    template_head();
                    {  // ( ... )*
                      for (;;) {
                        if ((_tokenSet_4.member(LA(1)))) {
                          fs = function_specifier();
                        } else {
                          goto _loop91;
                        }
                      }
                    _loop91:;
                    }  // ( ... )*
                    class_specifier();
                  }
                } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
                  synPredMatched92 = false;
                }
                rewind(_m92);
                inputState->guessing--;
              }
              if (synPredMatched92) {
                if (inputState->guessing == 0) {
#line 627 "CPP_parser.g"
                  if (statementTrace >= 1) {
                    printf(
                        "%d member_declaration Templated class decl or def\n",
                        LT(1)->getLine());
                  }

#line 3349 "CPPParser.cpp"
                }
                template_head();
                {  // ( ... )*
                  for (;;) {
                    if ((_tokenSet_4.member(LA(1)))) {
                      fs = function_specifier();
                    } else {
                      goto _loop94;
                    }
                  }
                _loop94:;
                }  // ( ... )*
                class_decl_or_def(fs);
                {
                  switch (LA(1)) {
                    case ID:
                    case LITERAL__stdcall:
                    case LITERAL___stdcall:
                    case LPAREN:
                    case OPERATOR:
                    case LITERAL_this:
                    case LITERAL_true:
                    case LITERAL_false:
                    case TILDE:
                    case STAR:
                    case AMPERSAND:
                    case SCOPE:
                    case LITERAL__cdecl:
                    case LITERAL___cdecl:
                    case LITERAL__near:
                    case LITERAL___near:
                    case LITERAL__far:
                    case LITERAL___far:
                    case LITERAL___interrupt:
                    case LITERAL_pascal:
                    case LITERAL__pascal:
                    case LITERAL___pascal: {
                      init_declarator_list();
                      break;
                    }
                    case SEMICOLON: {
                      break;
                    }
                    default: {
                      throw ANTLR_USE_NAMESPACE(antlr)
                          NoViableAltException(LT(1), getFilename());
                    }
                  }
                }
                match(SEMICOLON);
                if (inputState->guessing == 0) {
#line 630 "CPP_parser.g"
                  end_of_stmt();
#line 3407 "CPPParser.cpp"
                }
              } else {
                bool synPredMatched98 = false;
                if (((LA(1) == LITERAL_enum) && (_tokenSet_5.member(LA(2))))) {
                  int _m98 = mark();
                  synPredMatched98 = true;
                  inputState->guessing++;
                  try {
                    {
                      match(LITERAL_enum);
                      {
                        switch (LA(1)) {
                          case ID: {
                            match(ID);
                            break;
                          }
                          case LCURLY: {
                            break;
                          }
                          default: {
                            throw ANTLR_USE_NAMESPACE(antlr)
                                NoViableAltException(LT(1), getFilename());
                          }
                        }
                      }
                      match(LCURLY);
                    }
                  } catch (ANTLR_USE_NAMESPACE(antlr)
                               RecognitionException& pe) {
                    synPredMatched98 = false;
                  }
                  rewind(_m98);
                  inputState->guessing--;
                }
                if (synPredMatched98) {
                  if (inputState->guessing == 0) {
#line 634 "CPP_parser.g"
                    if (statementTrace >= 1) {
                      printf("%d member_declaration Enum definition\n",
                             LT(1)->getLine());
                    }

#line 3451 "CPPParser.cpp"
                  }
                  enum_specifier();
                  {
                    switch (LA(1)) {
                      case ID:
                      case COLON:
                      case LITERAL__stdcall:
                      case LITERAL___stdcall:
                      case LPAREN:
                      case OPERATOR:
                      case LITERAL_this:
                      case LITERAL_true:
                      case LITERAL_false:
                      case TILDE:
                      case STAR:
                      case AMPERSAND:
                      case SCOPE:
                      case LITERAL__cdecl:
                      case LITERAL___cdecl:
                      case LITERAL__near:
                      case LITERAL___near:
                      case LITERAL__far:
                      case LITERAL___far:
                      case LITERAL___interrupt:
                      case LITERAL_pascal:
                      case LITERAL__pascal:
                      case LITERAL___pascal: {
                        member_declarator_list();
                        break;
                      }
                      case SEMICOLON: {
                        break;
                      }
                      default: {
                        throw ANTLR_USE_NAMESPACE(antlr)
                            NoViableAltException(LT(1), getFilename());
                      }
                    }
                  }
                  match(SEMICOLON);
                  if (inputState->guessing == 0) {
#line 637 "CPP_parser.g"
                    end_of_stmt();
#line 3497 "CPPParser.cpp"
                  }
                } else {
                  bool synPredMatched101 = false;
                  if (((_tokenSet_8.member(LA(1))) &&
                       (_tokenSet_9.member(LA(2))))) {
                    int _m101 = mark();
                    synPredMatched101 = true;
                    inputState->guessing++;
                    try {
                      {
                        ctor_decl_spec();
                        if (!(qualifiedItemIsOneOf(qiCtor))) {
                          throw ANTLR_USE_NAMESPACE(antlr)
                              SemanticException("qualifiedItemIsOneOf(qiCtor)");
                        }
                        ctor_declarator(0);
                        match(SEMICOLON);
                      }
                    } catch (ANTLR_USE_NAMESPACE(antlr)
                                 RecognitionException& pe) {
                      synPredMatched101 = false;
                    }
                    rewind(_m101);
                    inputState->guessing--;
                  }
                  if (synPredMatched101) {
                    if (inputState->guessing == 0) {
#line 644 "CPP_parser.g"
                      if (statementTrace >= 1) {
                        printf("%d member_declaration Constructor declarator\n",
                               LT(1)->getLine());
                      }

#line 3527 "CPPParser.cpp"
                    }
                    ctor_decl_spec();
                    ctor_declarator(0);
                    match(SEMICOLON);
                    if (inputState->guessing == 0) {
#line 647 "CPP_parser.g"
                      end_of_stmt();
#line 3535 "CPPParser.cpp"
                    }
                  } else {
                    bool synPredMatched104 = false;
                    if (((_tokenSet_8.member(LA(1))) &&
                         (_tokenSet_9.member(LA(2))))) {
                      int _m104 = mark();
                      synPredMatched104 = true;
                      inputState->guessing++;
                      try {
                        {
                          ctor_decl_spec();
                          if (!(qualifiedItemIsOneOf(qiCtor))) {
                            throw ANTLR_USE_NAMESPACE(antlr) SemanticException(
                                "qualifiedItemIsOneOf(qiCtor)");
                          }
                          ctor_declarator(1);
                          {
                            switch (LA(1)) {
                              case COLON: {
                                match(COLON);
                                break;
                              }
                              case LCURLY: {
                                match(LCURLY);
                                break;
                              }
                              default: {
                                throw ANTLR_USE_NAMESPACE(antlr)
                                    NoViableAltException(LT(1), getFilename());
                              }
                            }
                          }
                        }
                      } catch (ANTLR_USE_NAMESPACE(antlr)
                                   RecognitionException& pe) {
                        synPredMatched104 = false;
                      }
                      rewind(_m104);
                      inputState->guessing--;
                    }
                    if (synPredMatched104) {
                      if (inputState->guessing == 0) {
#line 661 "CPP_parser.g"
                        if (statementTrace >= 1) {
                          printf(
                              "%d member_declaration Constructor definition\n",
                              LT(1)->getLine());
                        }

#line 3582 "CPPParser.cpp"
                      }
                      ctor_definition();
                    } else {
                      bool synPredMatched106 = false;
                      if (((_tokenSet_42.member(LA(1))) &&
                           (_tokenSet_7.member(LA(2))))) {
                        int _m106 = mark();
                        synPredMatched106 = true;
                        inputState->guessing++;
                        try {
                          {
                            dtor_head(0);
                            match(SEMICOLON);
                          }
                        } catch (ANTLR_USE_NAMESPACE(antlr)
                                     RecognitionException& pe) {
                          synPredMatched106 = false;
                        }
                        rewind(_m106);
                        inputState->guessing--;
                      }
                      if (synPredMatched106) {
                        if (inputState->guessing == 0) {
#line 670 "CPP_parser.g"
                          if (statementTrace >= 1) {
                            printf(
                                "%d member_declaration Destructor "
                                "declaration\n",
                                LT(1)->getLine());
                          }

#line 3610 "CPPParser.cpp"
                        }
                        dtor_head(0);
                        match(SEMICOLON);
                        if (inputState->guessing == 0) {
#line 673 "CPP_parser.g"
                          end_of_stmt();
#line 3617 "CPPParser.cpp"
                        }
                      } else {
                        bool synPredMatched108 = false;
                        if (((_tokenSet_42.member(LA(1))) &&
                             (_tokenSet_7.member(LA(2))))) {
                          int _m108 = mark();
                          synPredMatched108 = true;
                          inputState->guessing++;
                          try {
                            {
                              dtor_head(1);
                              match(LCURLY);
                            }
                          } catch (ANTLR_USE_NAMESPACE(antlr)
                                       RecognitionException& pe) {
                            synPredMatched108 = false;
                          }
                          rewind(_m108);
                          inputState->guessing--;
                        }
                        if (synPredMatched108) {
                          if (inputState->guessing == 0) {
#line 679 "CPP_parser.g"
                            if (statementTrace >= 1) {
                              printf(
                                  "%d member_declaration Destructor "
                                  "definition\n",
                                  LT(1)->getLine());
                            }

#line 3644 "CPPParser.cpp"
                          }
                          dtor_head(1);
                          dtor_body();
                        } else {
                          bool synPredMatched110 = false;
                          if (((_tokenSet_12.member(LA(1))) &&
                               (_tokenSet_13.member(LA(2))))) {
                            int _m110 = mark();
                            synPredMatched110 = true;
                            inputState->guessing++;
                            try {
                              {
                                declaration_specifiers();
                                function_declarator(0);
                                match(SEMICOLON);
                              }
                            } catch (ANTLR_USE_NAMESPACE(antlr)
                                         RecognitionException& pe) {
                              synPredMatched110 = false;
                            }
                            rewind(_m110);
                            inputState->guessing--;
                          }
                          if (synPredMatched110) {
                            if (inputState->guessing == 0) {
#line 686 "CPP_parser.g"
                              if (statementTrace >= 1) {
                                printf(
                                    "%d member_declaration Function "
                                    "declaration\n",
                                    LT(1)->getLine());
                              }

#line 3674 "CPPParser.cpp"
                            }
                            declaration_specifiers();
                            function_declarator(0);
                            match(SEMICOLON);
                            if (inputState->guessing == 0) {
#line 689 "CPP_parser.g"
                              end_of_stmt();
#line 3682 "CPPParser.cpp"
                            }
                          } else {
                            bool synPredMatched112 = false;
                            if (((_tokenSet_14.member(LA(1))) &&
                                 (_tokenSet_15.member(LA(2))))) {
                              int _m112 = mark();
                              synPredMatched112 = true;
                              inputState->guessing++;
                              try {
                                {
                                  declaration_specifiers();
                                  function_declarator(1);
                                  match(LCURLY);
                                }
                              } catch (ANTLR_USE_NAMESPACE(antlr)
                                           RecognitionException& pe) {
                                synPredMatched112 = false;
                              }
                              rewind(_m112);
                              inputState->guessing--;
                            }
                            if (synPredMatched112) {
                              if (inputState->guessing == 0) {
#line 693 "CPP_parser.g"
                                beginFieldDeclaration();
                                if (statementTrace >= 1) {
                                  printf(
                                      "%d member_declaration Function "
                                      "definition\n",
                                      LT(1)->getLine());
                                }

#line 3711 "CPPParser.cpp"
                              }
                              function_definition();
                            } else {
                              bool synPredMatched115 = false;
                              if (((LA(1) == LITERAL_inline ||
                                    LA(1) == OPERATOR) &&
                                   (_tokenSet_43.member(LA(2))))) {
                                int _m115 = mark();
                                synPredMatched115 = true;
                                inputState->guessing++;
                                try {
                                  {
                                    {
                                      switch (LA(1)) {
                                        case LITERAL_inline: {
                                          match(LITERAL_inline);
                                          break;
                                        }
                                        case OPERATOR: {
                                          break;
                                        }
                                        default: {
                                          throw ANTLR_USE_NAMESPACE(antlr)
                                              NoViableAltException(
                                                  LT(1), getFilename());
                                        }
                                      }
                                    }
                                    conversion_function_decl_or_def();
                                  }
                                } catch (ANTLR_USE_NAMESPACE(antlr)
                                             RecognitionException& pe) {
                                  synPredMatched115 = false;
                                }
                                rewind(_m115);
                                inputState->guessing--;
                              }
                              if (synPredMatched115) {
                                if (inputState->guessing == 0) {
#line 701 "CPP_parser.g"
                                  if (statementTrace >= 1) {
                                    printf(
                                        "%d member_declaration Operator "
                                        "function\n",
                                        LT(1)->getLine());
                                  }

#line 3755 "CPPParser.cpp"
                                }
                                {
                                  switch (LA(1)) {
                                    case LITERAL_inline: {
                                      match(LITERAL_inline);
                                      break;
                                    }
                                    case OPERATOR: {
                                      break;
                                    }
                                    default: {
                                      throw ANTLR_USE_NAMESPACE(antlr)
                                          NoViableAltException(LT(1),
                                                               getFilename());
                                    }
                                  }
                                }
                                conversion_function_decl_or_def();
                              } else {
                                bool synPredMatched118 = false;
                                if (((_tokenSet_44.member(LA(1))) &&
                                     (_tokenSet_45.member(LA(2))))) {
                                  int _m118 = mark();
                                  synPredMatched118 = true;
                                  inputState->guessing++;
                                  try {
                                    {
                                      qualified_id();
                                      match(SEMICOLON);
                                    }
                                  } catch (ANTLR_USE_NAMESPACE(antlr)
                                               RecognitionException& pe) {
                                    synPredMatched118 = false;
                                  }
                                  rewind(_m118);
                                  inputState->guessing--;
                                }
                                if (synPredMatched118) {
                                  if (inputState->guessing == 0) {
#line 710 "CPP_parser.g"
                                    if (statementTrace >= 1) {
                                      printf(
                                          "%d member_declaration Qualified "
                                          "ID\n",
                                          LT(1)->getLine());
                                    }

#line 3800 "CPPParser.cpp"
                                  }
                                  q = qualified_id();
                                  match(SEMICOLON);
                                  if (inputState->guessing == 0) {
#line 713 "CPP_parser.g"
                                    end_of_stmt();
#line 3807 "CPPParser.cpp"
                                  }
                                } else {
                                  bool synPredMatched121 = false;
                                  if (((_tokenSet_2.member(LA(1))) &&
                                       (_tokenSet_3.member(LA(2))))) {
                                    int _m121 = mark();
                                    synPredMatched121 = true;
                                    inputState->guessing++;
                                    try {
                                      {
                                        declaration_specifiers();
                                        {
                                          switch (LA(1)) {
                                            case ID:
                                            case LITERAL__stdcall:
                                            case LITERAL___stdcall:
                                            case LPAREN:
                                            case OPERATOR:
                                            case LITERAL_this:
                                            case LITERAL_true:
                                            case LITERAL_false:
                                            case TILDE:
                                            case STAR:
                                            case AMPERSAND:
                                            case SCOPE:
                                            case LITERAL__cdecl:
                                            case LITERAL___cdecl:
                                            case LITERAL__near:
                                            case LITERAL___near:
                                            case LITERAL__far:
                                            case LITERAL___far:
                                            case LITERAL___interrupt:
                                            case LITERAL_pascal:
                                            case LITERAL__pascal:
                                            case LITERAL___pascal: {
                                              init_declarator_list();
                                              break;
                                            }
                                            case SEMICOLON: {
                                              break;
                                            }
                                            default: {
                                              throw ANTLR_USE_NAMESPACE(antlr)
                                                  NoViableAltException(
                                                      LT(1), getFilename());
                                            }
                                          }
                                        }
                                        match(SEMICOLON);
                                      }
                                    } catch (ANTLR_USE_NAMESPACE(antlr)
                                                 RecognitionException& pe) {
                                      synPredMatched121 = false;
                                    }
                                    rewind(_m121);
                                    inputState->guessing--;
                                  }
                                  if (synPredMatched121) {
                                    if (inputState->guessing == 0) {
#line 718 "CPP_parser.g"
                                      beginFieldDeclaration();
                                      if (statementTrace >= 1) {
                                        printf(
                                            "%d member_declaration "
                                            "Declaration\n",
                                            LT(1)->getLine());
                                      }

#line 3873 "CPPParser.cpp"
                                    }
                                    declaration();
                                  } else {
                                    bool synPredMatched126 = false;
                                    if (((_tokenSet_16.member(LA(1))) &&
                                         (_tokenSet_17.member(LA(2))))) {
                                      int _m126 = mark();
                                      synPredMatched126 = true;
                                      inputState->guessing++;
                                      try {
                                        {
                                          {
                                            switch (LA(1)) {
                                              case LITERAL_friend: {
                                                match(LITERAL_friend);
                                                break;
                                              }
                                              case LITERAL_inline:
                                              case LITERAL_struct:
                                              case LITERAL_union:
                                              case LITERAL_class:
                                              case LITERAL__inline:
                                              case LITERAL___inline:
                                              case LITERAL_virtual:
                                              case LITERAL_explicit: {
                                                break;
                                              }
                                              default: {
                                                throw ANTLR_USE_NAMESPACE(antlr)
                                                    NoViableAltException(
                                                        LT(1), getFilename());
                                              }
                                            }
                                          }
                                          {  // ( ... )*
                                            for (;;) {
                                              if ((_tokenSet_4.member(LA(1)))) {
                                                fs = function_specifier();
                                              } else {
                                                goto _loop125;
                                              }
                                            }
                                          _loop125:;
                                          }  // ( ... )*
                                          class_specifier();
                                        }
                                      } catch (ANTLR_USE_NAMESPACE(antlr)
                                                   RecognitionException& pe) {
                                        synPredMatched126 = false;
                                      }
                                      rewind(_m126);
                                      inputState->guessing--;
                                    }
                                    if (synPredMatched126) {
                                      if (inputState->guessing == 0) {
#line 726 "CPP_parser.g"
                                        if (statementTrace >= 1) {
                                          printf(
                                              "%d member_declaration Class "
                                              "decl or def\n",
                                              LT(1)->getLine());
                                        }

#line 3936 "CPPParser.cpp"
                                      }
                                      {
                                        switch (LA(1)) {
                                          case LITERAL_friend: {
                                            match(LITERAL_friend);
                                            break;
                                          }
                                          case LITERAL_inline:
                                          case LITERAL_struct:
                                          case LITERAL_union:
                                          case LITERAL_class:
                                          case LITERAL__inline:
                                          case LITERAL___inline:
                                          case LITERAL_virtual:
                                          case LITERAL_explicit: {
                                            break;
                                          }
                                          default: {
                                            throw ANTLR_USE_NAMESPACE(antlr)
                                                NoViableAltException(
                                                    LT(1), getFilename());
                                          }
                                        }
                                      }
                                      {  // ( ... )*
                                        for (;;) {
                                          if ((_tokenSet_4.member(LA(1)))) {
                                            fs = function_specifier();
                                          } else {
                                            goto _loop129;
                                          }
                                        }
                                      _loop129:;
                                      }  // ( ... )*
                                      class_decl_or_def(fs);
                                      {
                                        switch (LA(1)) {
                                          case ID:
                                          case LITERAL__stdcall:
                                          case LITERAL___stdcall:
                                          case LPAREN:
                                          case OPERATOR:
                                          case LITERAL_this:
                                          case LITERAL_true:
                                          case LITERAL_false:
                                          case TILDE:
                                          case STAR:
                                          case AMPERSAND:
                                          case SCOPE:
                                          case LITERAL__cdecl:
                                          case LITERAL___cdecl:
                                          case LITERAL__near:
                                          case LITERAL___near:
                                          case LITERAL__far:
                                          case LITERAL___far:
                                          case LITERAL___interrupt:
                                          case LITERAL_pascal:
                                          case LITERAL__pascal:
                                          case LITERAL___pascal: {
                                            init_declarator_list();
                                            break;
                                          }
                                          case SEMICOLON: {
                                            break;
                                          }
                                          default: {
                                            throw ANTLR_USE_NAMESPACE(antlr)
                                                NoViableAltException(
                                                    LT(1), getFilename());
                                          }
                                        }
                                      }
                                      match(SEMICOLON);
                                      if (inputState->guessing == 0) {
#line 729 "CPP_parser.g"
                                        end_of_stmt();
#line 4017 "CPPParser.cpp"
                                      }
                                    } else {
                                      bool synPredMatched134 = false;
                                      if (((_tokenSet_46.member(LA(1))) &&
                                           (_tokenSet_47.member(LA(2))))) {
                                        int _m134 = mark();
                                        synPredMatched134 = true;
                                        inputState->guessing++;
                                        try {
                                          {
                                            {  // ( ... )*
                                              for (;;) {
                                                if ((_tokenSet_4.member(
                                                        LA(1)))) {
                                                  fs = function_specifier();
                                                } else {
                                                  goto _loop133;
                                                }
                                              }
                                            _loop133:;
                                            }  // ( ... )*
                                            function_declarator(0);
                                            match(SEMICOLON);
                                          }
                                        } catch (ANTLR_USE_NAMESPACE(antlr)
                                                     RecognitionException& pe) {
                                          synPredMatched134 = false;
                                        }
                                        rewind(_m134);
                                        inputState->guessing--;
                                      }
                                      if (synPredMatched134) {
                                        if (inputState->guessing == 0) {
#line 733 "CPP_parser.g"
                                          beginFieldDeclaration();
                                          fprintf(stderr,
                                                  "%d warning Function "
                                                  "declaration found without "
                                                  "return type\n",
                                                  LT(1)->getLine());
                                          if (statementTrace >= 1) {
                                            printf(
                                                "%d member_declaration "
                                                "Function declaration\n",
                                                LT(1)->getLine());
                                          }

#line 4058 "CPPParser.cpp"
                                        }
                                        {  // ( ... )*
                                          for (;;) {
                                            if ((_tokenSet_4.member(LA(1)))) {
                                              fs = function_specifier();
                                            } else {
                                              goto _loop136;
                                            }
                                          }
                                        _loop136:;
                                        }  // ( ... )*
                                        function_declarator(0);
                                        match(SEMICOLON);
                                        if (inputState->guessing == 0) {
#line 738 "CPP_parser.g"
                                          end_of_stmt();
#line 4077 "CPPParser.cpp"
                                        }
                                      } else if ((_tokenSet_38.member(LA(1))) &&
                                                 (_tokenSet_39.member(LA(2)))) {
                                        if (inputState->guessing == 0) {
#line 742 "CPP_parser.g"

                                          fprintf(stderr,
                                                  "%d warning Function "
                                                  "definition found without "
                                                  "return type\n",
                                                  LT(1)->getLine());
                                          if (statementTrace >= 1) {
                                            printf(
                                                "%d member_declaration "
                                                "Function definition without "
                                                "return type\n",
                                                LT(1)->getLine());
                                          }

#line 4088 "CPPParser.cpp"
                                        }
                                        function_declarator(1);
                                        compound_statement();
                                        if (inputState->guessing == 0) {
#line 747 "CPP_parser.g"
                                          endFunctionDefinition();
#line 4095 "CPPParser.cpp"
                                        }
                                      } else if ((LA(1) == LITERAL_template) &&
                                                 (LA(2) == LESSTHAN)) {
                                        if (inputState->guessing == 0) {
#line 750 "CPP_parser.g"
                                          beginTemplateDeclaration();
#line 4102 "CPPParser.cpp"
                                        }
                                        template_head();
                                        {
                                          bool synPredMatched140 = false;
                                          if (((_tokenSet_12.member(LA(1))) &&
                                               (_tokenSet_18.member(LA(2))))) {
                                            int _m140 = mark();
                                            synPredMatched140 = true;
                                            inputState->guessing++;
                                            try {
                                              {
                                                declaration_specifiers();
                                                {
                                                  switch (LA(1)) {
                                                    case ID:
                                                    case LITERAL__stdcall:
                                                    case LITERAL___stdcall:
                                                    case LPAREN:
                                                    case OPERATOR:
                                                    case LITERAL_this:
                                                    case LITERAL_true:
                                                    case LITERAL_false:
                                                    case TILDE:
                                                    case STAR:
                                                    case AMPERSAND:
                                                    case SCOPE:
                                                    case LITERAL__cdecl:
                                                    case LITERAL___cdecl:
                                                    case LITERAL__near:
                                                    case LITERAL___near:
                                                    case LITERAL__far:
                                                    case LITERAL___far:
                                                    case LITERAL___interrupt:
                                                    case LITERAL_pascal:
                                                    case LITERAL__pascal:
                                                    case LITERAL___pascal: {
                                                      init_declarator_list();
                                                      break;
                                                    }
                                                    case SEMICOLON: {
                                                      break;
                                                    }
                                                    default: {
                                                      throw ANTLR_USE_NAMESPACE(
                                                          antlr)
                                                          NoViableAltException(
                                                              LT(1),
                                                              getFilename());
                                                    }
                                                  }
                                                }
                                                match(SEMICOLON);
                                              }
                                            } catch (
                                                ANTLR_USE_NAMESPACE(antlr)
                                                    RecognitionException& pe) {
                                              synPredMatched140 = false;
                                            }
                                            rewind(_m140);
                                            inputState->guessing--;
                                          }
                                          if (synPredMatched140) {
                                            if (inputState->guessing == 0) {
#line 755 "CPP_parser.g"
                                              if (statementTrace >= 1) {
                                                printf(
                                                    "%d member_declaration "
                                                    "Templated forward "
                                                    "declaration\n",
                                                    LT(1)->getLine());
                                              }

#line 4167 "CPPParser.cpp"
                                            }
                                            declaration_specifiers();
                                            {
                                              switch (LA(1)) {
                                                case ID:
                                                case LITERAL__stdcall:
                                                case LITERAL___stdcall:
                                                case LPAREN:
                                                case OPERATOR:
                                                case LITERAL_this:
                                                case LITERAL_true:
                                                case LITERAL_false:
                                                case TILDE:
                                                case STAR:
                                                case AMPERSAND:
                                                case SCOPE:
                                                case LITERAL__cdecl:
                                                case LITERAL___cdecl:
                                                case LITERAL__near:
                                                case LITERAL___near:
                                                case LITERAL__far:
                                                case LITERAL___far:
                                                case LITERAL___interrupt:
                                                case LITERAL_pascal:
                                                case LITERAL__pascal:
                                                case LITERAL___pascal: {
                                                  init_declarator_list();
                                                  break;
                                                }
                                                case SEMICOLON: {
                                                  break;
                                                }
                                                default: {
                                                  throw ANTLR_USE_NAMESPACE(
                                                      antlr)
                                                      NoViableAltException(
                                                          LT(1), getFilename());
                                                }
                                              }
                                            }
                                            match(SEMICOLON);
                                            if (inputState->guessing == 0) {
#line 758 "CPP_parser.g"
                                              end_of_stmt();
#line 4212 "CPPParser.cpp"
                                            }
                                          } else {
                                            bool synPredMatched143 = false;
                                            if (((_tokenSet_2.member(LA(1))) &&
                                                 (_tokenSet_3.member(LA(2))))) {
                                              int _m143 = mark();
                                              synPredMatched143 = true;
                                              inputState->guessing++;
                                              try {
                                                {
                                                  declaration_specifiers();
                                                  function_declarator(0);
                                                  match(SEMICOLON);
                                                }
                                              } catch (ANTLR_USE_NAMESPACE(
                                                  antlr) RecognitionException&
                                                           pe) {
                                                synPredMatched143 = false;
                                              }
                                              rewind(_m143);
                                              inputState->guessing--;
                                            }
                                            if (synPredMatched143) {
                                              if (inputState->guessing == 0) {
#line 762 "CPP_parser.g"
                                                if (statementTrace >= 1) {
                                                  printf(
                                                      "%d member_declaration "
                                                      "Templated function "
                                                      "declaration\n",
                                                      LT(1)->getLine());
                                                }

#line 4240 "CPPParser.cpp"
                                              }
                                              declaration();
                                            } else {
                                              bool synPredMatched145 = false;
                                              if (((_tokenSet_14.member(
                                                       LA(1))) &&
                                                   (_tokenSet_15.member(
                                                       LA(2))))) {
                                                int _m145 = mark();
                                                synPredMatched145 = true;
                                                inputState->guessing++;
                                                try {
                                                  {
                                                    declaration_specifiers();
                                                    function_declarator(1);
                                                    match(LCURLY);
                                                  }
                                                } catch (ANTLR_USE_NAMESPACE(
                                                    antlr) RecognitionException&
                                                             pe) {
                                                  synPredMatched145 = false;
                                                }
                                                rewind(_m145);
                                                inputState->guessing--;
                                              }
                                              if (synPredMatched145) {
                                                if (inputState->guessing == 0) {
#line 769 "CPP_parser.g"
                                                  if (statementTrace >= 1) {
                                                    printf(
                                                        "%d member_declaration "
                                                        "Templated function "
                                                        "definition\n",
                                                        LT(1)->getLine());
                                                  }

#line 4269 "CPPParser.cpp"
                                                }
                                                function_definition();
                                              } else {
                                                bool synPredMatched147 = false;
                                                if (((_tokenSet_8.member(
                                                         LA(1))) &&
                                                     (_tokenSet_9.member(
                                                         LA(2))))) {
                                                  int _m147 = mark();
                                                  synPredMatched147 = true;
                                                  inputState->guessing++;
                                                  try {
                                                    {
                                                      ctor_decl_spec();
                                                      if (!(qualifiedItemIsOneOf(
                                                              qiCtor))) {
                                                        throw ANTLR_USE_NAMESPACE(
                                                            antlr)
                                                            SemanticException(
                                                                "qualifiedItemI"
                                                                "sOneOf("
                                                                "qiCtor)");
                                                      }
                                                    }
                                                  } catch (
                                                      ANTLR_USE_NAMESPACE(antlr)
                                                          RecognitionException&
                                                              pe) {
                                                    synPredMatched147 = false;
                                                  }
                                                  rewind(_m147);
                                                  inputState->guessing--;
                                                }
                                                if (synPredMatched147) {
                                                  if (inputState->guessing ==
                                                      0) {
#line 779 "CPP_parser.g"
                                                    if (statementTrace >= 1) {
                                                      printf(
                                                          "%d "
                                                          "member_declaration "
                                                          "Templated "
                                                          "constructor "
                                                          "definition\n",
                                                          LT(1)->getLine());
                                                    }

#line 4298 "CPPParser.cpp"
                                                  }
                                                  ctor_definition();
                                                } else if ((LA(1) ==
                                                            OPERATOR) &&
                                                           (_tokenSet_12.member(
                                                               LA(2)))) {
                                                  if (inputState->guessing ==
                                                      0) {
#line 785 "CPP_parser.g"
                                                    if (statementTrace >= 1) {
                                                      printf(
                                                          "%d "
                                                          "member_declaration "
                                                          "Templated operator "
                                                          "function\n",
                                                          LT(1)->getLine());
                                                    }

#line 4308 "CPPParser.cpp"
                                                  }
                                                  conversion_function_decl_or_def();
                                                } else if (
                                                    ((LA(1) >= LITERAL_struct &&
                                                      LA(1) <=
                                                          LITERAL_class)) &&
                                                    (LA(2) == ID ||
                                                     LA(2) == LCURLY)) {
                                                  if (inputState->guessing ==
                                                      0) {
#line 791 "CPP_parser.g"
                                                    if (statementTrace >= 1) {
                                                      printf(
                                                          "%d "
                                                          "member_declaration "
                                                          "Templated class "
                                                          "definition\n",
                                                          LT(1)->getLine());
                                                    }

#line 4318 "CPPParser.cpp"
                                                  }
                                                  class_head();
                                                  declaration_specifiers();
                                                  {
                                                    switch (LA(1)) {
                                                      case ID:
                                                      case LITERAL__stdcall:
                                                      case LITERAL___stdcall:
                                                      case LPAREN:
                                                      case OPERATOR:
                                                      case LITERAL_this:
                                                      case LITERAL_true:
                                                      case LITERAL_false:
                                                      case TILDE:
                                                      case STAR:
                                                      case AMPERSAND:
                                                      case SCOPE:
                                                      case LITERAL__cdecl:
                                                      case LITERAL___cdecl:
                                                      case LITERAL__near:
                                                      case LITERAL___near:
                                                      case LITERAL__far:
                                                      case LITERAL___far:
                                                      case LITERAL___interrupt:
                                                      case LITERAL_pascal:
                                                      case LITERAL__pascal:
                                                      case LITERAL___pascal: {
                                                        init_declarator_list();
                                                        break;
                                                      }
                                                      case SEMICOLON: {
                                                        break;
                                                      }
                                                      default: {
                                                        throw ANTLR_USE_NAMESPACE(
                                                            antlr)
                                                            NoViableAltException(
                                                                LT(1),
                                                                getFilename());
                                                      }
                                                    }
                                                  }
                                                  match(SEMICOLON);
                                                  if (inputState->guessing ==
                                                      0) {
#line 794 "CPP_parser.g"
                                                    end_of_stmt();
#line 4364 "CPPParser.cpp"
                                                  }
                                                } else {
                                                  throw ANTLR_USE_NAMESPACE(
                                                      antlr)
                                                      NoViableAltException(
                                                          LT(1), getFilename());
                                                }
                                              }
                                            }
                                          }
                                        }
                                        if (inputState->guessing == 0) {
#line 796 "CPP_parser.g"
                                          endTemplateDeclaration();
#line 4375 "CPPParser.cpp"
                                        }
                                      } else {
                                        throw ANTLR_USE_NAMESPACE(antlr)
                                            NoViableAltException(LT(1),
                                                                 getFilename());
                                      }
                                    }
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_40);
    } else {
      throw;
    }
  }
}

void CPPParser::member_declarator_list() {
  try {  // for error handling
    member_declarator();
    {
      switch (LA(1)) {
        case ASSIGNEQUAL: {
          match(ASSIGNEQUAL);
          match(OCTALINT);
          break;
        }
        case SEMICOLON:
        case COMMA: {
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
    {  // ( ... )*
      for (;;) {
        if ((LA(1) == COMMA)) {
          match(COMMA);
          member_declarator();
          {
            switch (LA(1)) {
              case ASSIGNEQUAL: {
                match(ASSIGNEQUAL);
                match(OCTALINT);
                break;
              }
              case SEMICOLON:
              case COMMA: {
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
        } else {
          goto _loop253;
        }
      }
    _loop253:;
    }  // ( ... )*
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_22);
    } else {
      throw;
    }
  }
}

void CPPParser::ctor_declarator(int definition) {
#line 1402 "CPP_parser.g"
  char* q;
#line 4465 "CPPParser.cpp"

  try {  // for error handling
    q = qualified_ctor_id();
    if (inputState->guessing == 0) {
#line 1406 "CPP_parser.g"
      declaratorParameterList(definition);
#line 4472 "CPPParser.cpp"
    }
    match(LPAREN);
    {
      switch (LA(1)) {
        case LITERAL_typedef:
        case LITERAL_enum:
        case ID:
        case LITERAL_inline:
        case LITERAL_friend:
        case LITERAL_extern:
        case LITERAL_struct:
        case LITERAL_union:
        case LITERAL_class:
        case LITERAL__stdcall:
        case LITERAL___stdcall:
        case LITERAL_GFEXCLUDE:
        case LITERAL_GFINCLUDE:
        case LITERAL_GFID:
        case LITERAL_GFUNREAD:
        case LITERAL_GFARRAYSIZE:
        case LPAREN:
        case LITERAL_GFARRAYSIZES:
        case LITERAL_GFARRAYELEMSIZE:
        case LITERAL_auto:
        case LITERAL_register:
        case LITERAL_static:
        case LITERAL_mutable:
        case LITERAL__inline:
        case LITERAL___inline:
        case LITERAL_virtual:
        case LITERAL_explicit:
        case LITERAL_typename:
        case LITERAL_char:
        case LITERAL_wchar_t:
        case LITERAL_bool:
        case LITERAL_short:
        case LITERAL_int:
        case 50:
        case 51:
        case 52:
        case 53:
        case 54:
        case 55:
        case 56:
        case 57:
        case 58:
        case 59:
        case 60:
        case 61:
        case 62:
        case 63:
        case 64:
        case 65:
        case 66:
        case 67:
        case LITERAL_long:
        case LITERAL_signed:
        case LITERAL_unsigned:
        case LITERAL_float:
        case LITERAL_double:
        case LITERAL_void:
        case LITERAL__declspec:
        case LITERAL___declspec:
        case LITERAL_const:
        case LITERAL___const:
        case LITERAL_volatile:
        case LITERAL___volatile__:
        case OPERATOR:
        case LITERAL_this:
        case LITERAL_true:
        case LITERAL_false:
        case TILDE:
        case STAR:
        case AMPERSAND:
        case ELLIPSIS:
        case SCOPE:
        case LITERAL__cdecl:
        case LITERAL___cdecl:
        case LITERAL__near:
        case LITERAL___near:
        case LITERAL__far:
        case LITERAL___far:
        case LITERAL___interrupt:
        case LITERAL_pascal:
        case LITERAL__pascal:
        case LITERAL___pascal: {
          parameter_list();
          break;
        }
        case RPAREN: {
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
    match(RPAREN);
    if (inputState->guessing == 0) {
#line 1408 "CPP_parser.g"
      declaratorEndParameterList(definition);
#line 4577 "CPPParser.cpp"
    }
    {
      switch (LA(1)) {
        case LITERAL_throw: {
          exception_specification();
          break;
        }
        case SEMICOLON:
        case LCURLY:
        case COLON: {
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_48);
    } else {
      throw;
    }
  }
}

void CPPParser::compound_statement() {
  try {  // for error handling
    match(LCURLY);
    if (inputState->guessing == 0) {
#line 1746 "CPP_parser.g"
      end_of_stmt();
      enterNewLocalScope();

#line 4618 "CPPParser.cpp"
    }
    {
      if ((_tokenSet_49.member(LA(1)))) {
        statement_list();
      } else if ((LA(1) == RCURLY)) {
      } else {
        throw ANTLR_USE_NAMESPACE(antlr)
            NoViableAltException(LT(1), getFilename());
      }
    }
    match(RCURLY);
    if (inputState->guessing == 0) {
#line 1751 "CPP_parser.g"
      exitLocalScope();
#line 4635 "CPPParser.cpp"
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_50);
    } else {
      throw;
    }
  }
}

void CPPParser::class_head() {
  try {  // for error handling
    {
      switch (LA(1)) {
        case LITERAL_struct: {
          match(LITERAL_struct);
          break;
        }
        case LITERAL_union: {
          match(LITERAL_union);
          break;
        }
        case LITERAL_class: {
          match(LITERAL_class);
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
    {
      switch (LA(1)) {
        case ID: {
          match(ID);
          {
            switch (LA(1)) {
              case LESSTHAN: {
                match(LESSTHAN);
                template_argument_list();
                match(GREATERTHAN);
                break;
              }
              case LCURLY:
              case COLON: {
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          {
            switch (LA(1)) {
              case COLON: {
                base_clause();
                break;
              }
              case LCURLY: {
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          break;
        }
        case LCURLY: {
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
    match(LCURLY);
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_12);
    } else {
      throw;
    }
  }
}

void CPPParser::access_specifier() {
  try {  // for error handling
    {
      switch (LA(1)) {
        case LITERAL_public: {
          match(LITERAL_public);
          break;
        }
        case LITERAL_protected: {
          match(LITERAL_protected);
          break;
        }
        case LITERAL_private: {
          match(LITERAL_private);
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_51);
    } else {
      throw;
    }
  }
}

void CPPParser::linkage_specification() {
  try {  // for error handling
    match(LITERAL_extern);
    match(StringLiteral);
    {
      switch (LA(1)) {
        case LCURLY: {
          match(LCURLY);
          {  // ( ... )*
            for (;;) {
              if ((_tokenSet_0.member(LA(1)))) {
                external_declaration();
              } else {
                goto _loop164;
              }
            }
          _loop164:;
          }  // ( ... )*
          match(RCURLY);
          break;
        }
        case LITERAL_typedef:
        case LITERAL_enum:
        case ID:
        case LITERAL_inline:
        case LITERAL_friend:
        case LITERAL_extern:
        case LITERAL_struct:
        case LITERAL_union:
        case LITERAL_class:
        case LITERAL__stdcall:
        case LITERAL___stdcall:
        case LITERAL_GFEXCLUDE:
        case LITERAL_GFINCLUDE:
        case LITERAL_GFID:
        case LITERAL_GFUNREAD:
        case LITERAL_GFARRAYSIZE:
        case LITERAL_GFARRAYSIZES:
        case LITERAL_GFARRAYELEMSIZE:
        case LITERAL_auto:
        case LITERAL_register:
        case LITERAL_static:
        case LITERAL_mutable:
        case LITERAL__inline:
        case LITERAL___inline:
        case LITERAL_virtual:
        case LITERAL_explicit:
        case LITERAL_typename:
        case LITERAL_char:
        case LITERAL_wchar_t:
        case LITERAL_bool:
        case LITERAL_short:
        case LITERAL_int:
        case 50:
        case 51:
        case 52:
        case 53:
        case 54:
        case 55:
        case 56:
        case 57:
        case 58:
        case 59:
        case 60:
        case 61:
        case 62:
        case 63:
        case 64:
        case 65:
        case 66:
        case 67:
        case LITERAL_long:
        case LITERAL_signed:
        case LITERAL_unsigned:
        case LITERAL_float:
        case LITERAL_double:
        case LITERAL_void:
        case LITERAL__declspec:
        case LITERAL___declspec:
        case LITERAL_const:
        case LITERAL___const:
        case LITERAL_volatile:
        case LITERAL___volatile__:
        case LITERAL_using:
        case SCOPE: {
          declaration();
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_30);
    } else {
      throw;
    }
  }
}

void CPPParser::using_statement() {
#line 1836 "CPP_parser.g"
  char* qid;
#line 4890 "CPPParser.cpp"

  try {  // for error handling
    match(LITERAL_using);
    {
      switch (LA(1)) {
        case LITERAL_namespace: {
          match(LITERAL_namespace);
          qid = qualified_id();
          break;
        }
        case ID:
        case LITERAL_typename:
        case OPERATOR:
        case LITERAL_this:
        case LITERAL_true:
        case LITERAL_false:
        case SCOPE: {
          {
            switch (LA(1)) {
              case LITERAL_typename: {
                match(LITERAL_typename);
                break;
              }
              case ID:
              case OPERATOR:
              case LITERAL_this:
              case LITERAL_true:
              case LITERAL_false:
              case SCOPE: {
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          qid = qualified_id();
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
    match(SEMICOLON);
    if (inputState->guessing == 0) {
#line 1843 "CPP_parser.g"
      end_of_stmt();
#line 4945 "CPPParser.cpp"
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_30);
    } else {
      throw;
    }
  }
}

void CPPParser::template_argument_list() {
  try {  // for error handling
    template_argument();
    {  // ( ... )*
      for (;;) {
        if ((LA(1) == COMMA)) {
          match(COMMA);
          template_argument();
        } else {
          goto _loop385;
        }
      }
    _loop385:;
    }  // ( ... )*
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_52);
    } else {
      throw;
    }
  }
}

void CPPParser::base_clause() {
  try {  // for error handling
    match(COLON);
    base_specifier();
    {  // ( ... )*
      for (;;) {
        if ((LA(1) == COMMA)) {
          match(COMMA);
          base_specifier();
        } else {
          goto _loop220;
        }
      }
    _loop220:;
    }  // ( ... )*
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_53);
    } else {
      throw;
    }
  }
}

CPPParser::StorageClass CPPParser::storage_class_specifier() {
#line 928 "CPP_parser.g"
  CPPParser::StorageClass sc = scInvalid;
#line 5018 "CPPParser.cpp"

  try {  // for error handling
    switch (LA(1)) {
      case LITERAL_auto: {
        match(LITERAL_auto);
        if (inputState->guessing == 0) {
#line 929 "CPP_parser.g"
          sc = scAUTO;
#line 5028 "CPPParser.cpp"
        }
        break;
      }
      case LITERAL_register: {
        match(LITERAL_register);
        if (inputState->guessing == 0) {
#line 930 "CPP_parser.g"
          sc = scREGISTER;
#line 5038 "CPPParser.cpp"
        }
        break;
      }
      case LITERAL_static: {
        match(LITERAL_static);
        if (inputState->guessing == 0) {
#line 931 "CPP_parser.g"
          sc = scSTATIC;
#line 5048 "CPPParser.cpp"
        }
        break;
      }
      case LITERAL_extern: {
        match(LITERAL_extern);
        if (inputState->guessing == 0) {
#line 932 "CPP_parser.g"
          sc = scEXTERN;
#line 5058 "CPPParser.cpp"
        }
        break;
      }
      case LITERAL_mutable: {
        match(LITERAL_mutable);
        if (inputState->guessing == 0) {
#line 933 "CPP_parser.g"
          sc = scMUTABLE;
#line 5068 "CPPParser.cpp"
        }
        break;
      }
      default: {
        throw ANTLR_USE_NAMESPACE(antlr)
            NoViableAltException(LT(1), getFilename());
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_12);
    } else {
      throw;
    }
  }
  return sc;
}

CPPParser::TypeQualifier CPPParser::type_qualifier() {
#line 1025 "CPP_parser.g"
  CPPParser::TypeQualifier tq = tqInvalid;
#line 5092 "CPPParser.cpp"

  try {  // for error handling
    {
      switch (LA(1)) {
        case LITERAL_const:
        case LITERAL___const: {
          {
            switch (LA(1)) {
              case LITERAL_const: {
                match(LITERAL_const);
                break;
              }
              case LITERAL___const: {
                match(LITERAL___const);
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          if (inputState->guessing == 0) {
#line 1027 "CPP_parser.g"
            tq = tqCONST;
#line 5121 "CPPParser.cpp"
          }
          break;
        }
        case LITERAL_volatile:
        case LITERAL___volatile__: {
          {
            switch (LA(1)) {
              case LITERAL_volatile: {
                match(LITERAL_volatile);
                break;
              }
              case LITERAL___volatile__: {
                match(LITERAL___volatile__);
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          if (inputState->guessing == 0) {
#line 1028 "CPP_parser.g"
            tq = tqVOLATILE;
#line 5149 "CPPParser.cpp"
          }
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_54);
    } else {
      throw;
    }
  }
  return tq;
}

CPPParser::TypeSpecifier CPPParser::type_specifier() {
#line 944 "CPP_parser.g"
  CPPParser::TypeSpecifier ts = tsInvalid;
#line 5174 "CPPParser.cpp"
#line 944 "CPP_parser.g"

  TypeQualifier tq = tqInvalid;

#line 5179 "CPPParser.cpp"

  try {  // for error handling
    ts = simple_type_specifier();
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_24);
    } else {
      throw;
    }
  }
  return ts;
}

CPPParser::TypeSpecifier CPPParser::simple_type_specifier() {
#line 953 "CPP_parser.g"
  CPPParser::TypeSpecifier ts = tsInvalid;
#line 5198 "CPPParser.cpp"
#line 953 "CPP_parser.g"
  char* s;
  ts = tsInvalid;

#line 5203 "CPPParser.cpp"

  try {  // for error handling
    {
      switch (LA(1)) {
        case LITERAL_typename: {
          match(LITERAL_typename);
          s = qualified_type();
          if (inputState->guessing == 0) {
#line 962 "CPP_parser.g"
            declaratorID(s, qiType);
#line 5215 "CPPParser.cpp"
          }
          break;
        }
        case LITERAL_char:
        case LITERAL_wchar_t:
        case LITERAL_bool:
        case LITERAL_short:
        case LITERAL_int:
        case 50:
        case 51:
        case 52:
        case 53:
        case 54:
        case 55:
        case 56:
        case 57:
        case 58:
        case 59:
        case 60:
        case 61:
        case 62:
        case 63:
        case 64:
        case 65:
        case 66:
        case 67:
        case LITERAL_long:
        case LITERAL_signed:
        case LITERAL_unsigned:
        case LITERAL_float:
        case LITERAL_double:
        case LITERAL_void:
        case LITERAL__declspec:
        case LITERAL___declspec: {
          {  // ( ... )+
            int _cnt192 = 0;
            for (;;) {
              switch (LA(1)) {
                case LITERAL_char: {
                  match(LITERAL_char);
                  if (inputState->guessing == 0) {
#line 964 "CPP_parser.g"
                    ts |= tsCHAR;
#line 5261 "CPPParser.cpp"
                  }
                  break;
                }
                case LITERAL_wchar_t: {
                  match(LITERAL_wchar_t);
                  if (inputState->guessing == 0) {
#line 965 "CPP_parser.g"
                    ts |= tsWCHAR_T;
#line 5271 "CPPParser.cpp"
                  }
                  break;
                }
                case LITERAL_bool: {
                  match(LITERAL_bool);
                  if (inputState->guessing == 0) {
#line 966 "CPP_parser.g"
                    ts |= tsBOOL;
#line 5281 "CPPParser.cpp"
                  }
                  break;
                }
                case LITERAL_short: {
                  match(LITERAL_short);
                  if (inputState->guessing == 0) {
#line 967 "CPP_parser.g"
                    ts |= tsSHORT;
#line 5291 "CPPParser.cpp"
                  }
                  break;
                }
                case LITERAL_int: {
                  match(LITERAL_int);
                  if (inputState->guessing == 0) {
#line 968 "CPP_parser.g"
                    ts |= tsINT;
#line 5301 "CPPParser.cpp"
                  }
                  break;
                }
                case 50:
                case 51:
                case 52: {
                  {
                    switch (LA(1)) {
                      case 50: {
                        match(50);
                        break;
                      }
                      case 51: {
                        match(51);
                        break;
                      }
                      case 52: {
                        match(52);
                        break;
                      }
                      default: {
                        throw ANTLR_USE_NAMESPACE(antlr)
                            NoViableAltException(LT(1), getFilename());
                      }
                    }
                  }
                  if (inputState->guessing == 0) {
#line 969 "CPP_parser.g"
                    ts |= tsINT;
#line 5335 "CPPParser.cpp"
                  }
                  break;
                }
                case 53:
                case 54:
                case 55: {
                  {
                    switch (LA(1)) {
                      case 53: {
                        match(53);
                        break;
                      }
                      case 54: {
                        match(54);
                        break;
                      }
                      case 55: {
                        match(55);
                        break;
                      }
                      default: {
                        throw ANTLR_USE_NAMESPACE(antlr)
                            NoViableAltException(LT(1), getFilename());
                      }
                    }
                  }
                  if (inputState->guessing == 0) {
#line 970 "CPP_parser.g"
                    ts |= tsINT;
#line 5369 "CPPParser.cpp"
                  }
                  break;
                }
                case 56:
                case 57:
                case 58: {
                  {
                    switch (LA(1)) {
                      case 56: {
                        match(56);
                        break;
                      }
                      case 57: {
                        match(57);
                        break;
                      }
                      case 58: {
                        match(58);
                        break;
                      }
                      default: {
                        throw ANTLR_USE_NAMESPACE(antlr)
                            NoViableAltException(LT(1), getFilename());
                      }
                    }
                  }
                  if (inputState->guessing == 0) {
#line 971 "CPP_parser.g"
                    ts |= tsLONG;
#line 5403 "CPPParser.cpp"
                  }
                  break;
                }
                case 59:
                case 60:
                case 61: {
                  {
                    switch (LA(1)) {
                      case 59: {
                        match(59);
                        break;
                      }
                      case 60: {
                        match(60);
                        break;
                      }
                      case 61: {
                        match(61);
                        break;
                      }
                      default: {
                        throw ANTLR_USE_NAMESPACE(antlr)
                            NoViableAltException(LT(1), getFilename());
                      }
                    }
                  }
                  if (inputState->guessing == 0) {
#line 972 "CPP_parser.g"
                    ts |= tsLONG;
#line 5437 "CPPParser.cpp"
                  }
                  break;
                }
                case 62: {
                  { match(62); }
                  if (inputState->guessing == 0) {
#line 973 "CPP_parser.g"
                    ts |= (tsUNSIGNED | tsINT);
#line 5449 "CPPParser.cpp"
                  }
                  break;
                }
                case 63: {
                  { match(63); }
                  if (inputState->guessing == 0) {
#line 974 "CPP_parser.g"
                    ts |= (tsUNSIGNED | tsINT);
#line 5461 "CPPParser.cpp"
                  }
                  break;
                }
                case 64: {
                  { match(64); }
                  if (inputState->guessing == 0) {
#line 975 "CPP_parser.g"
                    ts |= (tsUNSIGNED | tsLONG);
#line 5473 "CPPParser.cpp"
                  }
                  break;
                }
                case 65: {
                  { match(65); }
                  if (inputState->guessing == 0) {
#line 976 "CPP_parser.g"
                    ts |= (tsUNSIGNED | tsLONG);
#line 5485 "CPPParser.cpp"
                  }
                  break;
                }
                case 66:
                case 67: {
                  {
                    switch (LA(1)) {
                      case 66: {
                        match(66);
                        break;
                      }
                      case 67: {
                        match(67);
                        break;
                      }
                      default: {
                        throw ANTLR_USE_NAMESPACE(antlr)
                            NoViableAltException(LT(1), getFilename());
                      }
                    }
                  }
                  if (inputState->guessing == 0) {
#line 977 "CPP_parser.g"
                    ts |= tsLONG;
#line 5513 "CPPParser.cpp"
                  }
                  break;
                }
                case LITERAL_long: {
                  match(LITERAL_long);
                  if (inputState->guessing == 0) {
#line 978 "CPP_parser.g"
                    ts |= tsLONG;
#line 5523 "CPPParser.cpp"
                  }
                  break;
                }
                case LITERAL_signed: {
                  match(LITERAL_signed);
                  if (inputState->guessing == 0) {
#line 979 "CPP_parser.g"
                    ts |= tsSIGNED;
#line 5533 "CPPParser.cpp"
                  }
                  break;
                }
                case LITERAL_unsigned: {
                  match(LITERAL_unsigned);
                  if (inputState->guessing == 0) {
#line 980 "CPP_parser.g"
                    ts |= tsUNSIGNED;
#line 5543 "CPPParser.cpp"
                  }
                  break;
                }
                case LITERAL_float: {
                  match(LITERAL_float);
                  if (inputState->guessing == 0) {
#line 981 "CPP_parser.g"
                    ts |= tsFLOAT;
#line 5553 "CPPParser.cpp"
                  }
                  break;
                }
                case LITERAL_double: {
                  match(LITERAL_double);
                  if (inputState->guessing == 0) {
#line 982 "CPP_parser.g"
                    ts |= tsDOUBLE;
#line 5563 "CPPParser.cpp"
                  }
                  break;
                }
                case LITERAL_void: {
                  match(LITERAL_void);
                  if (inputState->guessing == 0) {
#line 983 "CPP_parser.g"
                    ts |= tsVOID;
#line 5573 "CPPParser.cpp"
                  }
                  break;
                }
                case LITERAL__declspec:
                case LITERAL___declspec: {
                  {
                    switch (LA(1)) {
                      case LITERAL__declspec: {
                        match(LITERAL__declspec);
                        break;
                      }
                      case LITERAL___declspec: {
                        match(LITERAL___declspec);
                        break;
                      }
                      default: {
                        throw ANTLR_USE_NAMESPACE(antlr)
                            NoViableAltException(LT(1), getFilename());
                      }
                    }
                  }
                  match(LPAREN);
                  match(ID);
                  match(RPAREN);
                  break;
                }
                default: {
                  if (_cnt192 >= 1) {
                    goto _loop192;
                  } else {
                    throw ANTLR_USE_NAMESPACE(antlr)
                        NoViableAltException(LT(1), getFilename());
                  }
                }
              }
              _cnt192++;
            }
          _loop192:;
          }  // ( ... )+
          break;
        }
        default:
          if (((LA(1) == ID || LA(1) == SCOPE)) &&
              (qualifiedItemIsOneOf(qiType | qiCtor))) {
            s = qualified_type();
          } else {
            throw ANTLR_USE_NAMESPACE(antlr)
                NoViableAltException(LT(1), getFilename());
          }
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_24);
    } else {
      throw;
    }
  }
  return ts;
}

char* CPPParser::qualified_type() {
#line 994 "CPP_parser.g"
  char* q = NULL;
#line 5638 "CPPParser.cpp"
  ANTLR_USE_NAMESPACE(antlr) RefToken id = ANTLR_USE_NAMESPACE(antlr) nullToken;
#line 994 "CPP_parser.g"
  char* s;
  static char qitem[CPPParser_MaxQualifiedItemSize + 1];
#line 5642 "CPPParser.cpp"

  try {  // for error handling
    s = scope_override();
    id = LT(1);
    match(ID);
    {
      if ((LA(1) == LESSTHAN) && (_tokenSet_55.member(LA(2)))) {
        match(LESSTHAN);
        template_argument_list();
        match(GREATERTHAN);
      } else if ((_tokenSet_56.member(LA(1))) && (_tokenSet_57.member(LA(2)))) {
      } else {
        throw ANTLR_USE_NAMESPACE(antlr)
            NoViableAltException(LT(1), getFilename());
      }
    }
    if (inputState->guessing == 0) {
#line 1008 "CPP_parser.g"

      strcpy(qitem, s);
      strcat(qitem, (id->getText()).data());
      q = qitem;

#line 5668 "CPPParser.cpp"
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_56);
    } else {
      throw;
    }
  }
  return q;
}

void CPPParser::class_prefix() {
  try {  // for error handling
    {    // ( ... )*
      for (;;) {
        if ((LA(1) == LITERAL__declspec || LA(1) == LITERAL___declspec)) {
          {
            switch (LA(1)) {
              case LITERAL__declspec: {
                match(LITERAL__declspec);
                break;
              }
              case LITERAL___declspec: {
                match(LITERAL___declspec);
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          match(LPAREN);
          expression();
          match(RPAREN);
        } else {
          goto _loop204;
        }
      }
    _loop204:;
    }  // ( ... )*
    if (inputState->guessing == 0) {
#line 1036 "CPP_parser.g"

#line 5721 "CPPParser.cpp"
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_58);
    } else {
      throw;
    }
  }
}

void CPPParser::expression() {
  try {  // for error handling
    assignment_expression();
    {  // ( ... )*
      for (;;) {
        if ((LA(1) == COMMA)) {
          match(COMMA);
          assignment_expression();
        } else {
          goto _loop444;
        }
      }
    _loop444:;
    }  // ( ... )*
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_59);
    } else {
      throw;
    }
  }
}

void CPPParser::base_specifier() {
#line 1089 "CPP_parser.g"
  char* qt;
#line 5765 "CPPParser.cpp"

  try {  // for error handling
    {
      switch (LA(1)) {
        case LITERAL_virtual: {
          match(LITERAL_virtual);
          {
            switch (LA(1)) {
              case LITERAL_public:
              case LITERAL_protected:
              case LITERAL_private: {
                access_specifier();
                break;
              }
              case ID:
              case SCOPE: {
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          qt = qualified_type();
          break;
        }
        case LITERAL_public:
        case LITERAL_protected:
        case LITERAL_private: {
          access_specifier();
          {
            switch (LA(1)) {
              case LITERAL_virtual: {
                match(LITERAL_virtual);
                break;
              }
              case ID:
              case SCOPE: {
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          qt = qualified_type();
          break;
        }
        case ID:
        case SCOPE: {
          qt = qualified_type();
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_60);
    } else {
      throw;
    }
  }
}

void CPPParser::enumerator_list() {
  try {  // for error handling
    enumerator();
    {  // ( ... )*
      for (;;) {
        if ((LA(1) == COMMA)) {
          match(COMMA);
          enumerator();
        } else {
          goto _loop232;
        }
      }
    _loop232:;
    }  // ( ... )*
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_61);
    } else {
      throw;
    }
  }
}

void CPPParser::enumerator() {
  ANTLR_USE_NAMESPACE(antlr) RefToken id = ANTLR_USE_NAMESPACE(antlr) nullToken;

  try {  // for error handling
    id = LT(1);
    match(ID);
    {
      switch (LA(1)) {
        case ASSIGNEQUAL: {
          match(ASSIGNEQUAL);
          constant_expression();
          break;
        }
        case RCURLY:
        case COMMA: {
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
    if (inputState->guessing == 0) {
#line 1132 "CPP_parser.g"
      enumElement((id->getText()).data());
#line 5901 "CPPParser.cpp"
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_62);
    } else {
      throw;
    }
  }
}

void CPPParser::constant_expression() {
  try {  // for error handling
    conditional_expression();
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_63);
    } else {
      throw;
    }
  }
}

void CPPParser::optor() {
  try {  // for error handling
    switch (LA(1)) {
      case LITERAL_new: {
        match(LITERAL_new);
        {
          if ((LA(1) == LSQUARE) && (LA(2) == RSQUARE)) {
            match(LSQUARE);
            match(RSQUARE);
          } else if ((_tokenSet_64.member(LA(1))) &&
                     (_tokenSet_57.member(LA(2)))) {
          } else {
            throw ANTLR_USE_NAMESPACE(antlr)
                NoViableAltException(LT(1), getFilename());
          }
        }
        break;
      }
      case LITERAL_delete: {
        match(LITERAL_delete);
        {
          if ((LA(1) == LSQUARE) && (LA(2) == RSQUARE)) {
            match(LSQUARE);
            match(RSQUARE);
          } else if ((_tokenSet_64.member(LA(1))) &&
                     (_tokenSet_57.member(LA(2)))) {
          } else {
            throw ANTLR_USE_NAMESPACE(antlr)
                NoViableAltException(LT(1), getFilename());
          }
        }
        break;
      }
      case LPAREN: {
        match(LPAREN);
        match(RPAREN);
        break;
      }
      case LSQUARE: {
        match(LSQUARE);
        match(RSQUARE);
        break;
      }
      case LESSTHAN:
      case GREATERTHAN:
      case ASSIGNEQUAL:
      case COMMA:
      case TILDE:
      case STAR:
      case AMPERSAND:
      case TIMESEQUAL:
      case DIVIDEEQUAL:
      case MINUSEQUAL:
      case PLUSEQUAL:
      case MODEQUAL:
      case SHIFTLEFTEQUAL:
      case SHIFTRIGHTEQUAL:
      case BITWISEANDEQUAL:
      case BITWISEXOREQUAL:
      case BITWISEOREQUAL:
      case OR:
      case AND:
      case BITWISEOR:
      case BITWISEXOR:
      case NOTEQUAL:
      case EQUAL:
      case LESSTHANOREQUALTO:
      case GREATERTHANOREQUALTO:
      case SHIFTLEFT:
      case SHIFTRIGHT:
      case PLUS:
      case MINUS:
      case DIVIDE:
      case MOD:
      case POINTERTOMBR:
      case PLUSPLUS:
      case MINUSMINUS:
      case POINTERTO:
      case NOT: {
        optor_simple_tokclass();
        break;
      }
      default: {
        throw ANTLR_USE_NAMESPACE(antlr)
            NoViableAltException(LT(1), getFilename());
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_64);
    } else {
      throw;
    }
  }
}

void CPPParser::typeID() {
  try {  // for error handling
    if (!(isTypeName((LT(1)->getText()).data()))) {
      throw ANTLR_USE_NAMESPACE(antlr)
          SemanticException("isTypeName((LT(1)->getText()).data())");
    }
    match(ID);
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_1);
    } else {
      throw;
    }
  }
}

void CPPParser::init_declarator() {
  try {  // for error handling
    declarator();
    {
      switch (LA(1)) {
        case ASSIGNEQUAL: {
          match(ASSIGNEQUAL);
          initializer();
          break;
        }
        case LPAREN: {
          match(LPAREN);
          expression_list();
          match(RPAREN);
          break;
        }
        case SEMICOLON:
        case COMMA: {
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_65);
    } else {
      throw;
    }
  }
}

void CPPParser::declarator() {
  try {  // for error handling
    bool synPredMatched261 = false;
    if (((_tokenSet_25.member(LA(1))) && (_tokenSet_66.member(LA(2))))) {
      int _m261 = mark();
      synPredMatched261 = true;
      inputState->guessing++;
      try {
        { ptr_operator(); }
      } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
        synPredMatched261 = false;
      }
      rewind(_m261);
      inputState->guessing--;
    }
    if (synPredMatched261) {
      ptr_operator();
      declarator();
    } else if ((_tokenSet_67.member(LA(1))) && (_tokenSet_68.member(LA(2)))) {
      direct_declarator();
    } else {
      throw ANTLR_USE_NAMESPACE(antlr)
          NoViableAltException(LT(1), getFilename());
    }

  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_69);
    } else {
      throw;
    }
  }
}

void CPPParser::initializer() {
  try {  // for error handling
    switch (LA(1)) {
      case ID:
      case StringLiteral:
      case LPAREN:
      case LITERAL_typename:
      case LITERAL_char:
      case LITERAL_wchar_t:
      case LITERAL_bool:
      case LITERAL_short:
      case LITERAL_int:
      case 50:
      case 51:
      case 52:
      case 53:
      case 54:
      case 55:
      case 56:
      case 57:
      case 58:
      case 59:
      case 60:
      case 61:
      case 62:
      case 63:
      case 64:
      case 65:
      case 66:
      case 67:
      case LITERAL_long:
      case LITERAL_signed:
      case LITERAL_unsigned:
      case LITERAL_float:
      case LITERAL_double:
      case LITERAL_void:
      case LITERAL__declspec:
      case LITERAL___declspec:
      case OPERATOR:
      case LITERAL_this:
      case LITERAL_true:
      case LITERAL_false:
      case OCTALINT:
      case TILDE:
      case STAR:
      case AMPERSAND:
      case PLUS:
      case MINUS:
      case PLUSPLUS:
      case MINUSMINUS:
      case LITERAL_sizeof:
      case LITERAL___alignof__:
      case SCOPE:
      case LITERAL_dynamic_cast:
      case LITERAL_static_cast:
      case LITERAL_reinterpret_cast:
      case LITERAL_const_cast:
      case LITERAL_typeid:
      case DECIMALINT:
      case HEXADECIMALINT:
      case CharLiteral:
      case WCharLiteral:
      case WStringLiteral:
      case FLOATONE:
      case FLOATTWO:
      case NOT:
      case LITERAL_new:
      case LITERAL_delete: {
        remainder_expression();
        break;
      }
      case LCURLY: {
        match(LCURLY);
        initializer();
        {  // ( ... )*
          for (;;) {
            if ((LA(1) == COMMA)) {
              match(COMMA);
              {
                switch (LA(1)) {
                  case ID:
                  case LCURLY:
                  case StringLiteral:
                  case LPAREN:
                  case LITERAL_typename:
                  case LITERAL_char:
                  case LITERAL_wchar_t:
                  case LITERAL_bool:
                  case LITERAL_short:
                  case LITERAL_int:
                  case 50:
                  case 51:
                  case 52:
                  case 53:
                  case 54:
                  case 55:
                  case 56:
                  case 57:
                  case 58:
                  case 59:
                  case 60:
                  case 61:
                  case 62:
                  case 63:
                  case 64:
                  case 65:
                  case 66:
                  case 67:
                  case LITERAL_long:
                  case LITERAL_signed:
                  case LITERAL_unsigned:
                  case LITERAL_float:
                  case LITERAL_double:
                  case LITERAL_void:
                  case LITERAL__declspec:
                  case LITERAL___declspec:
                  case OPERATOR:
                  case LITERAL_this:
                  case LITERAL_true:
                  case LITERAL_false:
                  case OCTALINT:
                  case TILDE:
                  case STAR:
                  case AMPERSAND:
                  case PLUS:
                  case MINUS:
                  case PLUSPLUS:
                  case MINUSMINUS:
                  case LITERAL_sizeof:
                  case LITERAL___alignof__:
                  case SCOPE:
                  case LITERAL_dynamic_cast:
                  case LITERAL_static_cast:
                  case LITERAL_reinterpret_cast:
                  case LITERAL_const_cast:
                  case LITERAL_typeid:
                  case DECIMALINT:
                  case HEXADECIMALINT:
                  case CharLiteral:
                  case WCharLiteral:
                  case WStringLiteral:
                  case FLOATONE:
                  case FLOATTWO:
                  case NOT:
                  case LITERAL_new:
                  case LITERAL_delete: {
                    initializer();
                    break;
                  }
                  case RCURLY:
                  case COMMA: {
                    break;
                  }
                  default: {
                    throw ANTLR_USE_NAMESPACE(antlr)
                        NoViableAltException(LT(1), getFilename());
                  }
                }
              }
            } else {
              goto _loop248;
            }
          }
        _loop248:;
        }  // ( ... )*
        match(RCURLY);
        break;
      }
      default: {
        throw ANTLR_USE_NAMESPACE(antlr)
            NoViableAltException(LT(1), getFilename());
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_70);
    } else {
      throw;
    }
  }
}

void CPPParser::expression_list() {
  try {  // for error handling
    assignment_expression();
    {  // ( ... )*
      for (;;) {
        if ((LA(1) == COMMA)) {
          match(COMMA);
          assignment_expression();
        } else {
          goto _loop592;
        }
      }
    _loop592:;
    }  // ( ... )*
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_71);
    } else {
      throw;
    }
  }
}

void CPPParser::remainder_expression() {
  try {  // for error handling
    {
      bool synPredMatched452 = false;
      if (((_tokenSet_72.member(LA(1))) && (_tokenSet_73.member(LA(2))))) {
        int _m452 = mark();
        synPredMatched452 = true;
        inputState->guessing++;
        try {
          {
            conditional_expression();
            {
              switch (LA(1)) {
                case COMMA: {
                  match(COMMA);
                  break;
                }
                case SEMICOLON: {
                  match(SEMICOLON);
                  break;
                }
                case RPAREN: {
                  match(RPAREN);
                  break;
                }
                default: {
                  throw ANTLR_USE_NAMESPACE(antlr)
                      NoViableAltException(LT(1), getFilename());
                }
              }
            }
          }
        } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
          synPredMatched452 = false;
        }
        rewind(_m452);
        inputState->guessing--;
      }
      if (synPredMatched452) {
        if (inputState->guessing == 0) {
#line 1879 "CPP_parser.g"
          assign_stmt_RHS_found += 1;
#line 6400 "CPPParser.cpp"
        }
        assignment_expression();
        if (inputState->guessing == 0) {
#line 1881 "CPP_parser.g"

          if (assign_stmt_RHS_found > 0) {
            assign_stmt_RHS_found -= 1;
          } else {
            fprintf(stderr, "%d warning Error in assign_stmt_RHS_found = %d\n",
                    LT(1)->getLine(), assign_stmt_RHS_found);
            fprintf(stderr, "Press return to continue\n");
            getchar();
          }

#line 6416 "CPPParser.cpp"
        }
      } else if ((_tokenSet_72.member(LA(1))) && (_tokenSet_73.member(LA(2)))) {
        assignment_expression();
      } else {
        throw ANTLR_USE_NAMESPACE(antlr)
            NoViableAltException(LT(1), getFilename());
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_74);
    } else {
      throw;
    }
  }
}

void CPPParser::member_declarator() {
  try {  // for error handling
    bool synPredMatched257 = false;
    if (((LA(1) == ID || LA(1) == COLON) && (_tokenSet_75.member(LA(2))))) {
      int _m257 = mark();
      synPredMatched257 = true;
      inputState->guessing++;
      try {
        {
          {
            switch (LA(1)) {
              case ID: {
                match(ID);
                break;
              }
              case COLON: {
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          match(COLON);
          constant_expression();
        }
      } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
        synPredMatched257 = false;
      }
      rewind(_m257);
      inputState->guessing--;
    }
    if (synPredMatched257) {
      {
        switch (LA(1)) {
          case ID: {
            match(ID);
            break;
          }
          case COLON: {
            break;
          }
          default: {
            throw ANTLR_USE_NAMESPACE(antlr)
                NoViableAltException(LT(1), getFilename());
          }
        }
      }
      match(COLON);
      constant_expression();
    } else if ((_tokenSet_76.member(LA(1))) && (_tokenSet_77.member(LA(2)))) {
      declarator();
    } else {
      throw ANTLR_USE_NAMESPACE(antlr)
          NoViableAltException(LT(1), getFilename());
    }

  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_78);
    } else {
      throw;
    }
  }
}

void CPPParser::ptr_operator() {
  try {  // for error handling
    {
      switch (LA(1)) {
        case AMPERSAND: {
          match(AMPERSAND);
          if (inputState->guessing == 0) {
#line 2198 "CPP_parser.g"
            is_address = true;
#line 6525 "CPPParser.cpp"
          }
          break;
        }
        case LITERAL__cdecl:
        case LITERAL___cdecl: {
          {
            switch (LA(1)) {
              case LITERAL__cdecl: {
                match(LITERAL__cdecl);
                break;
              }
              case LITERAL___cdecl: {
                match(LITERAL___cdecl);
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          break;
        }
        case LITERAL__near:
        case LITERAL___near: {
          {
            switch (LA(1)) {
              case LITERAL__near: {
                match(LITERAL__near);
                break;
              }
              case LITERAL___near: {
                match(LITERAL___near);
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          break;
        }
        case LITERAL__far:
        case LITERAL___far: {
          {
            switch (LA(1)) {
              case LITERAL__far: {
                match(LITERAL__far);
                break;
              }
              case LITERAL___far: {
                match(LITERAL___far);
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          break;
        }
        case LITERAL___interrupt: {
          match(LITERAL___interrupt);
          break;
        }
        case LITERAL_pascal:
        case LITERAL__pascal:
        case LITERAL___pascal: {
          {
            switch (LA(1)) {
              case LITERAL_pascal: {
                match(LITERAL_pascal);
                break;
              }
              case LITERAL__pascal: {
                match(LITERAL__pascal);
                break;
              }
              case LITERAL___pascal: {
                match(LITERAL___pascal);
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          break;
        }
        case LITERAL__stdcall:
        case LITERAL___stdcall: {
          {
            switch (LA(1)) {
              case LITERAL__stdcall: {
                match(LITERAL__stdcall);
                break;
              }
              case LITERAL___stdcall: {
                match(LITERAL___stdcall);
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          break;
        }
        case ID:
        case STAR:
        case SCOPE: {
          ptr_to_member();
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_24);
    } else {
      throw;
    }
  }
}

void CPPParser::direct_declarator() {
  ANTLR_USE_NAMESPACE(antlr)
  RefToken dtor = ANTLR_USE_NAMESPACE(antlr) nullToken;
#line 1225 "CPP_parser.g"
  char* id;
  CPPParser::TypeQualifier tq;
#line 6684 "CPPParser.cpp"

  try {  // for error handling
    switch (LA(1)) {
      case TILDE: {
        match(TILDE);
        dtor = LT(1);
        match(ID);
        if (inputState->guessing == 0) {
#line 1271 "CPP_parser.g"
          declaratorID((dtor->getText()).data(), qiDtor);
#line 6696 "CPPParser.cpp"
        }
        if (inputState->guessing == 0) {
#line 1272 "CPP_parser.g"
          fprintf(
              stderr,
              "%d warning direct_declarator5 entered unexpectedly with %s\n",
              LT(1)->getLine(), (dtor->getText()).data());
#line 6702 "CPPParser.cpp"
        }
        match(LPAREN);
        if (inputState->guessing == 0) {
#line 1274 "CPP_parser.g"
          declaratorParameterList(0);
#line 6708 "CPPParser.cpp"
        }
        {
          switch (LA(1)) {
            case LITERAL_typedef:
            case LITERAL_enum:
            case ID:
            case LITERAL_inline:
            case LITERAL_friend:
            case LITERAL_extern:
            case LITERAL_struct:
            case LITERAL_union:
            case LITERAL_class:
            case LITERAL__stdcall:
            case LITERAL___stdcall:
            case LITERAL_GFEXCLUDE:
            case LITERAL_GFINCLUDE:
            case LITERAL_GFID:
            case LITERAL_GFUNREAD:
            case LITERAL_GFARRAYSIZE:
            case LPAREN:
            case LITERAL_GFARRAYSIZES:
            case LITERAL_GFARRAYELEMSIZE:
            case LITERAL_auto:
            case LITERAL_register:
            case LITERAL_static:
            case LITERAL_mutable:
            case LITERAL__inline:
            case LITERAL___inline:
            case LITERAL_virtual:
            case LITERAL_explicit:
            case LITERAL_typename:
            case LITERAL_char:
            case LITERAL_wchar_t:
            case LITERAL_bool:
            case LITERAL_short:
            case LITERAL_int:
            case 50:
            case 51:
            case 52:
            case 53:
            case 54:
            case 55:
            case 56:
            case 57:
            case 58:
            case 59:
            case 60:
            case 61:
            case 62:
            case 63:
            case 64:
            case 65:
            case 66:
            case 67:
            case LITERAL_long:
            case LITERAL_signed:
            case LITERAL_unsigned:
            case LITERAL_float:
            case LITERAL_double:
            case LITERAL_void:
            case LITERAL__declspec:
            case LITERAL___declspec:
            case LITERAL_const:
            case LITERAL___const:
            case LITERAL_volatile:
            case LITERAL___volatile__:
            case OPERATOR:
            case LITERAL_this:
            case LITERAL_true:
            case LITERAL_false:
            case TILDE:
            case STAR:
            case AMPERSAND:
            case ELLIPSIS:
            case SCOPE:
            case LITERAL__cdecl:
            case LITERAL___cdecl:
            case LITERAL__near:
            case LITERAL___near:
            case LITERAL__far:
            case LITERAL___far:
            case LITERAL___interrupt:
            case LITERAL_pascal:
            case LITERAL__pascal:
            case LITERAL___pascal: {
              parameter_list();
              break;
            }
            case RPAREN: {
              break;
            }
            default: {
              throw ANTLR_USE_NAMESPACE(antlr)
                  NoViableAltException(LT(1), getFilename());
            }
          }
        }
        match(RPAREN);
        if (inputState->guessing == 0) {
#line 1276 "CPP_parser.g"
          declaratorEndParameterList(0);
#line 6812 "CPPParser.cpp"
        }
        break;
      }
      case LPAREN: {
        match(LPAREN);
        declarator();
        match(RPAREN);
        {
          if ((LA(1) == LPAREN || LA(1) == LSQUARE) &&
              (_tokenSet_79.member(LA(2)))) {
            declarator_suffix();
          } else if ((_tokenSet_69.member(LA(1))) &&
                     (_tokenSet_80.member(LA(2)))) {
          } else {
            throw ANTLR_USE_NAMESPACE(antlr)
                NoViableAltException(LT(1), getFilename());
          }
        }
        break;
      }
      default:
        bool synPredMatched265 = false;
        if (((_tokenSet_44.member(LA(1))) && (_tokenSet_28.member(LA(2))))) {
          int _m265 = mark();
          synPredMatched265 = true;
          inputState->guessing++;
          try {
            {
              qualified_id();
              match(LPAREN);
              {
                switch (LA(1)) {
                  case RPAREN: {
                    match(RPAREN);
                    break;
                  }
                  case LITERAL_typedef:
                  case LITERAL_enum:
                  case ID:
                  case LITERAL_inline:
                  case LITERAL_friend:
                  case LITERAL_extern:
                  case LITERAL_struct:
                  case LITERAL_union:
                  case LITERAL_class:
                  case LITERAL__stdcall:
                  case LITERAL___stdcall:
                  case LITERAL_GFEXCLUDE:
                  case LITERAL_GFINCLUDE:
                  case LITERAL_GFID:
                  case LITERAL_GFUNREAD:
                  case LITERAL_GFARRAYSIZE:
                  case LITERAL_GFARRAYSIZES:
                  case LITERAL_GFARRAYELEMSIZE:
                  case LITERAL_auto:
                  case LITERAL_register:
                  case LITERAL_static:
                  case LITERAL_mutable:
                  case LITERAL__inline:
                  case LITERAL___inline:
                  case LITERAL_virtual:
                  case LITERAL_explicit:
                  case LITERAL_typename:
                  case LITERAL_char:
                  case LITERAL_wchar_t:
                  case LITERAL_bool:
                  case LITERAL_short:
                  case LITERAL_int:
                  case 50:
                  case 51:
                  case 52:
                  case 53:
                  case 54:
                  case 55:
                  case 56:
                  case 57:
                  case 58:
                  case 59:
                  case 60:
                  case 61:
                  case 62:
                  case 63:
                  case 64:
                  case 65:
                  case 66:
                  case 67:
                  case LITERAL_long:
                  case LITERAL_signed:
                  case LITERAL_unsigned:
                  case LITERAL_float:
                  case LITERAL_double:
                  case LITERAL_void:
                  case LITERAL__declspec:
                  case LITERAL___declspec:
                  case LITERAL_const:
                  case LITERAL___const:
                  case LITERAL_volatile:
                  case LITERAL___volatile__:
                  case SCOPE: {
                    declaration_specifiers();
                    break;
                  }
                  default: {
                    throw ANTLR_USE_NAMESPACE(antlr)
                        NoViableAltException(LT(1), getFilename());
                  }
                }
              }
            }
          } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
            synPredMatched265 = false;
          }
          rewind(_m265);
          inputState->guessing--;
        }
        if (synPredMatched265) {
          id = qualified_id();
          if (inputState->guessing == 0) {
#line 1231 "CPP_parser.g"
            if (_td == true) {  // This statement is a typedef
              declaratorID(id, qiType);
            } else {
              declaratorID(id, qiFun);
            }

#line 6941 "CPPParser.cpp"
          }
          match(LPAREN);
          if (inputState->guessing == 0) {
#line 1236 "CPP_parser.g"
            declaratorParameterList(0);
#line 6947 "CPPParser.cpp"
          }
          {
            switch (LA(1)) {
              case LITERAL_typedef:
              case LITERAL_enum:
              case ID:
              case LITERAL_inline:
              case LITERAL_friend:
              case LITERAL_extern:
              case LITERAL_struct:
              case LITERAL_union:
              case LITERAL_class:
              case LITERAL__stdcall:
              case LITERAL___stdcall:
              case LITERAL_GFEXCLUDE:
              case LITERAL_GFINCLUDE:
              case LITERAL_GFID:
              case LITERAL_GFUNREAD:
              case LITERAL_GFARRAYSIZE:
              case LPAREN:
              case LITERAL_GFARRAYSIZES:
              case LITERAL_GFARRAYELEMSIZE:
              case LITERAL_auto:
              case LITERAL_register:
              case LITERAL_static:
              case LITERAL_mutable:
              case LITERAL__inline:
              case LITERAL___inline:
              case LITERAL_virtual:
              case LITERAL_explicit:
              case LITERAL_typename:
              case LITERAL_char:
              case LITERAL_wchar_t:
              case LITERAL_bool:
              case LITERAL_short:
              case LITERAL_int:
              case 50:
              case 51:
              case 52:
              case 53:
              case 54:
              case 55:
              case 56:
              case 57:
              case 58:
              case 59:
              case 60:
              case 61:
              case 62:
              case 63:
              case 64:
              case 65:
              case 66:
              case 67:
              case LITERAL_long:
              case LITERAL_signed:
              case LITERAL_unsigned:
              case LITERAL_float:
              case LITERAL_double:
              case LITERAL_void:
              case LITERAL__declspec:
              case LITERAL___declspec:
              case LITERAL_const:
              case LITERAL___const:
              case LITERAL_volatile:
              case LITERAL___volatile__:
              case OPERATOR:
              case LITERAL_this:
              case LITERAL_true:
              case LITERAL_false:
              case TILDE:
              case STAR:
              case AMPERSAND:
              case ELLIPSIS:
              case SCOPE:
              case LITERAL__cdecl:
              case LITERAL___cdecl:
              case LITERAL__near:
              case LITERAL___near:
              case LITERAL__far:
              case LITERAL___far:
              case LITERAL___interrupt:
              case LITERAL_pascal:
              case LITERAL__pascal:
              case LITERAL___pascal: {
                parameter_list();
                break;
              }
              case RPAREN: {
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          match(RPAREN);
          if (inputState->guessing == 0) {
#line 1238 "CPP_parser.g"
            declaratorEndParameterList(0);
#line 7051 "CPPParser.cpp"
          }
          {  // ( ... )*
            for (;;) {
              if (((LA(1) >= LITERAL_const && LA(1) <= LITERAL___volatile__))) {
                tq = type_qualifier();
              } else {
                goto _loop268;
              }
            }
          _loop268:;
          }  // ( ... )*
          {
            switch (LA(1)) {
              case LITERAL_throw: {
                exception_specification();
                break;
              }
              case GREATERTHAN:
              case SEMICOLON:
              case ASSIGNEQUAL:
              case LPAREN:
              case RPAREN:
              case COMMA:
              case ELLIPSIS: {
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
        } else {
          bool synPredMatched271 = false;
          if (((_tokenSet_44.member(LA(1))) && (_tokenSet_28.member(LA(2))))) {
            int _m271 = mark();
            synPredMatched271 = true;
            inputState->guessing++;
            try {
              {
                qualified_id();
                match(LPAREN);
                qualified_id();
              }
            } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
              synPredMatched271 = false;
            }
            rewind(_m271);
            inputState->guessing--;
          }
          if (synPredMatched271) {
            id = qualified_id();
            if (inputState->guessing == 0) {
#line 1244 "CPP_parser.g"
              declaratorID(id, qiVar);
#line 7113 "CPPParser.cpp"
            }
            match(LPAREN);
            expression_list();
            match(RPAREN);
          } else {
            bool synPredMatched273 = false;
            if (((_tokenSet_44.member(LA(1))) &&
                 (_tokenSet_28.member(LA(2))))) {
              int _m273 = mark();
              synPredMatched273 = true;
              inputState->guessing++;
              try {
                {
                  qualified_id();
                  match(LSQUARE);
                }
              } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
                synPredMatched273 = false;
              }
              rewind(_m273);
              inputState->guessing--;
            }
            if (synPredMatched273) {
              id = qualified_id();
              if (inputState->guessing == 0) {
#line 1251 "CPP_parser.g"
                if (_td == true) {           // This statement is a typedef
                  declaratorID(id, qiType);  // This statement is a typedef
                } else {
                  declaratorID(id, qiVar);
                }
                is_address = false;
                is_pointer = false;

#line 7147 "CPPParser.cpp"
              }
              {  // ( ... )+
                int _cnt276 = 0;
                for (;;) {
                  if ((LA(1) == LSQUARE)) {
                    match(LSQUARE);
                    {
                      switch (LA(1)) {
                        case ID:
                        case StringLiteral:
                        case LPAREN:
                        case LITERAL_typename:
                        case LITERAL_char:
                        case LITERAL_wchar_t:
                        case LITERAL_bool:
                        case LITERAL_short:
                        case LITERAL_int:
                        case 50:
                        case 51:
                        case 52:
                        case 53:
                        case 54:
                        case 55:
                        case 56:
                        case 57:
                        case 58:
                        case 59:
                        case 60:
                        case 61:
                        case 62:
                        case 63:
                        case 64:
                        case 65:
                        case 66:
                        case 67:
                        case LITERAL_long:
                        case LITERAL_signed:
                        case LITERAL_unsigned:
                        case LITERAL_float:
                        case LITERAL_double:
                        case LITERAL_void:
                        case LITERAL__declspec:
                        case LITERAL___declspec:
                        case OPERATOR:
                        case LITERAL_this:
                        case LITERAL_true:
                        case LITERAL_false:
                        case OCTALINT:
                        case TILDE:
                        case STAR:
                        case AMPERSAND:
                        case PLUS:
                        case MINUS:
                        case PLUSPLUS:
                        case MINUSMINUS:
                        case LITERAL_sizeof:
                        case LITERAL___alignof__:
                        case SCOPE:
                        case LITERAL_dynamic_cast:
                        case LITERAL_static_cast:
                        case LITERAL_reinterpret_cast:
                        case LITERAL_const_cast:
                        case LITERAL_typeid:
                        case DECIMALINT:
                        case HEXADECIMALINT:
                        case CharLiteral:
                        case WCharLiteral:
                        case WStringLiteral:
                        case FLOATONE:
                        case FLOATTWO:
                        case NOT:
                        case LITERAL_new:
                        case LITERAL_delete: {
                          constant_expression();
                          break;
                        }
                        case RSQUARE: {
                          break;
                        }
                        default: {
                          throw ANTLR_USE_NAMESPACE(antlr)
                              NoViableAltException(LT(1), getFilename());
                        }
                      }
                    }
                    match(RSQUARE);
                  } else {
                    if (_cnt276 >= 1) {
                      goto _loop276;
                    } else {
                      throw ANTLR_USE_NAMESPACE(antlr)
                          NoViableAltException(LT(1), getFilename());
                    }
                  }

                  _cnt276++;
                }
              _loop276:;
              }  // ( ... )+
              if (inputState->guessing == 0) {
#line 1259 "CPP_parser.g"
                declaratorArray();
#line 7248 "CPPParser.cpp"
              }
            } else if ((_tokenSet_44.member(LA(1))) &&
                       (_tokenSet_81.member(LA(2)))) {
              id = qualified_id();
              if (inputState->guessing == 0) {
#line 1262 "CPP_parser.g"
                if (_td == true) {
                  declaratorID(id, qiType);  // This statement is a typedef
                } else {
                  declaratorID(id, qiVar);
                }
                is_address = false;
                is_pointer = false;

#line 7261 "CPPParser.cpp"
              }
            } else {
              throw ANTLR_USE_NAMESPACE(antlr)
                  NoViableAltException(LT(1), getFilename());
            }
          }
        }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_69);
    } else {
      throw;
    }
  }
}

void CPPParser::parameter_list() {
  try {  // for error handling
    parameter_declaration_list();
    {
      switch (LA(1)) {
        case ELLIPSIS: {
          match(ELLIPSIS);
          break;
        }
        case RPAREN: {
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_71);
    } else {
      throw;
    }
  }
}

void CPPParser::exception_specification() {
#line 1560 "CPP_parser.g"
  char* so;
#line 7314 "CPPParser.cpp"

  try {  // for error handling
    match(LITERAL_throw);
    match(LPAREN);
    {
      switch (LA(1)) {
        case ID:
        case RPAREN:
        case SCOPE: {
          {
            switch (LA(1)) {
              case ID:
              case SCOPE: {
                so = scope_override();
                match(ID);
                {  // ( ... )*
                  for (;;) {
                    if ((LA(1) == COMMA)) {
                      match(COMMA);
                      so = scope_override();
                      match(ID);
                    } else {
                      goto _loop366;
                    }
                  }
                _loop366:;
                }  // ( ... )*
                break;
              }
              case RPAREN: {
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          break;
        }
        case ELLIPSIS: {
          match(ELLIPSIS);
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
    match(RPAREN);
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_82);
    } else {
      throw;
    }
  }
}

void CPPParser::declarator_suffix() {
#line 1284 "CPP_parser.g"
  CPPParser::TypeQualifier tq;
#line 7386 "CPPParser.cpp"

  try {  // for error handling
    {
      if ((LA(1) == LSQUARE)) {
        {  // ( ... )+
          int _cnt283 = 0;
          for (;;) {
            if ((LA(1) == LSQUARE)) {
              match(LSQUARE);
              {
                switch (LA(1)) {
                  case ID:
                  case StringLiteral:
                  case LPAREN:
                  case LITERAL_typename:
                  case LITERAL_char:
                  case LITERAL_wchar_t:
                  case LITERAL_bool:
                  case LITERAL_short:
                  case LITERAL_int:
                  case 50:
                  case 51:
                  case 52:
                  case 53:
                  case 54:
                  case 55:
                  case 56:
                  case 57:
                  case 58:
                  case 59:
                  case 60:
                  case 61:
                  case 62:
                  case 63:
                  case 64:
                  case 65:
                  case 66:
                  case 67:
                  case LITERAL_long:
                  case LITERAL_signed:
                  case LITERAL_unsigned:
                  case LITERAL_float:
                  case LITERAL_double:
                  case LITERAL_void:
                  case LITERAL__declspec:
                  case LITERAL___declspec:
                  case OPERATOR:
                  case LITERAL_this:
                  case LITERAL_true:
                  case LITERAL_false:
                  case OCTALINT:
                  case TILDE:
                  case STAR:
                  case AMPERSAND:
                  case PLUS:
                  case MINUS:
                  case PLUSPLUS:
                  case MINUSMINUS:
                  case LITERAL_sizeof:
                  case LITERAL___alignof__:
                  case SCOPE:
                  case LITERAL_dynamic_cast:
                  case LITERAL_static_cast:
                  case LITERAL_reinterpret_cast:
                  case LITERAL_const_cast:
                  case LITERAL_typeid:
                  case DECIMALINT:
                  case HEXADECIMALINT:
                  case CharLiteral:
                  case WCharLiteral:
                  case WStringLiteral:
                  case FLOATONE:
                  case FLOATTWO:
                  case NOT:
                  case LITERAL_new:
                  case LITERAL_delete: {
                    constant_expression();
                    break;
                  }
                  case RSQUARE: {
                    break;
                  }
                  default: {
                    throw ANTLR_USE_NAMESPACE(antlr)
                        NoViableAltException(LT(1), getFilename());
                  }
                }
              }
              match(RSQUARE);
            } else {
              if (_cnt283 >= 1) {
                goto _loop283;
              } else {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }

            _cnt283++;
          }
        _loop283:;
        }  // ( ... )+
        if (inputState->guessing == 0) {
#line 1290 "CPP_parser.g"
          declaratorArray();
#line 7490 "CPPParser.cpp"
        }
      } else if (((LA(1) == LPAREN)) &&
                 ((!((LA(1) == LPAREN) && (LA(2) == ID)) ||
                   (qualifiedItemIsOneOf(qiType | qiCtor, 1))))) {
        match(LPAREN);
        if (inputState->guessing == 0) {
#line 1293 "CPP_parser.g"
          declaratorParameterList(0);
#line 7498 "CPPParser.cpp"
        }
        {
          switch (LA(1)) {
            case LITERAL_typedef:
            case LITERAL_enum:
            case ID:
            case LITERAL_inline:
            case LITERAL_friend:
            case LITERAL_extern:
            case LITERAL_struct:
            case LITERAL_union:
            case LITERAL_class:
            case LITERAL__stdcall:
            case LITERAL___stdcall:
            case LITERAL_GFEXCLUDE:
            case LITERAL_GFINCLUDE:
            case LITERAL_GFID:
            case LITERAL_GFUNREAD:
            case LITERAL_GFARRAYSIZE:
            case LPAREN:
            case LITERAL_GFARRAYSIZES:
            case LITERAL_GFARRAYELEMSIZE:
            case LITERAL_auto:
            case LITERAL_register:
            case LITERAL_static:
            case LITERAL_mutable:
            case LITERAL__inline:
            case LITERAL___inline:
            case LITERAL_virtual:
            case LITERAL_explicit:
            case LITERAL_typename:
            case LITERAL_char:
            case LITERAL_wchar_t:
            case LITERAL_bool:
            case LITERAL_short:
            case LITERAL_int:
            case 50:
            case 51:
            case 52:
            case 53:
            case 54:
            case 55:
            case 56:
            case 57:
            case 58:
            case 59:
            case 60:
            case 61:
            case 62:
            case 63:
            case 64:
            case 65:
            case 66:
            case 67:
            case LITERAL_long:
            case LITERAL_signed:
            case LITERAL_unsigned:
            case LITERAL_float:
            case LITERAL_double:
            case LITERAL_void:
            case LITERAL__declspec:
            case LITERAL___declspec:
            case LITERAL_const:
            case LITERAL___const:
            case LITERAL_volatile:
            case LITERAL___volatile__:
            case OPERATOR:
            case LITERAL_this:
            case LITERAL_true:
            case LITERAL_false:
            case TILDE:
            case STAR:
            case AMPERSAND:
            case ELLIPSIS:
            case SCOPE:
            case LITERAL__cdecl:
            case LITERAL___cdecl:
            case LITERAL__near:
            case LITERAL___near:
            case LITERAL__far:
            case LITERAL___far:
            case LITERAL___interrupt:
            case LITERAL_pascal:
            case LITERAL__pascal:
            case LITERAL___pascal: {
              parameter_list();
              break;
            }
            case RPAREN: {
              break;
            }
            default: {
              throw ANTLR_USE_NAMESPACE(antlr)
                  NoViableAltException(LT(1), getFilename());
            }
          }
        }
        match(RPAREN);
        if (inputState->guessing == 0) {
#line 1295 "CPP_parser.g"
          declaratorEndParameterList(0);
#line 7602 "CPPParser.cpp"
        }
        {  // ( ... )*
          for (;;) {
            if (((LA(1) >= LITERAL_const && LA(1) <= LITERAL___volatile__))) {
              tq = type_qualifier();
            } else {
              goto _loop286;
            }
          }
        _loop286:;
        }  // ( ... )*
        {
          switch (LA(1)) {
            case LITERAL_throw: {
              exception_specification();
              break;
            }
            case GREATERTHAN:
            case SEMICOLON:
            case ASSIGNEQUAL:
            case LPAREN:
            case RPAREN:
            case COMMA:
            case ELLIPSIS: {
              break;
            }
            default: {
              throw ANTLR_USE_NAMESPACE(antlr)
                  NoViableAltException(LT(1), getFilename());
            }
          }
        }
      } else {
        throw ANTLR_USE_NAMESPACE(antlr)
            NoViableAltException(LT(1), getFilename());
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_69);
    } else {
      throw;
    }
  }
}

void CPPParser::template_parameter_list() {
  try {  // for error handling
    if (inputState->guessing == 0) {
#line 1581 "CPP_parser.g"
      beginTemplateParameterList();
#line 7662 "CPPParser.cpp"
    }
    template_parameter();
    {  // ( ... )*
      for (;;) {
        if ((LA(1) == COMMA)) {
          match(COMMA);
          template_parameter();
        } else {
          goto _loop370;
        }
      }
    _loop370:;
    }  // ( ... )*
    if (inputState->guessing == 0) {
#line 1583 "CPP_parser.g"
      endTemplateParameterList();
#line 7681 "CPPParser.cpp"
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_52);
    } else {
      throw;
    }
  }
}

void CPPParser::function_direct_declarator(int definition) {
#line 1324 "CPP_parser.g"
  char* q;
  CPPParser::TypeQualifier tq;
#line 7700 "CPPParser.cpp"

  try {  // for error handling
    {
      switch (LA(1)) {
        case LPAREN: {
          match(LPAREN);
          q = qualified_id();
          if (inputState->guessing == 0) {
#line 1340 "CPP_parser.g"

            declaratorID(q, qiFun);

#line 7714 "CPPParser.cpp"
          }
          match(RPAREN);
          break;
        }
        case ID:
        case OPERATOR:
        case LITERAL_this:
        case LITERAL_true:
        case LITERAL_false:
        case SCOPE: {
          q = qualified_id();
          if (inputState->guessing == 0) {
#line 1346 "CPP_parser.g"

            declaratorID(q, qiFun);

#line 7732 "CPPParser.cpp"
          }
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
    if (inputState->guessing == 0) {
#line 1351 "CPP_parser.g"

#ifdef MYCODE
      if (definition) myCode_function_direct_declarator(q);
#endif  // MYCODE

#line 7750 "CPPParser.cpp"
    }
    match(LPAREN);
    if (inputState->guessing == 0) {
#line 1359 "CPP_parser.g"

      functionParameterList();
      if (K_and_R == false) in_parameter_list = true;

#line 7760 "CPPParser.cpp"
    }
    {
      switch (LA(1)) {
        case LITERAL_typedef:
        case LITERAL_enum:
        case ID:
        case LITERAL_inline:
        case LITERAL_friend:
        case LITERAL_extern:
        case LITERAL_struct:
        case LITERAL_union:
        case LITERAL_class:
        case LITERAL__stdcall:
        case LITERAL___stdcall:
        case LITERAL_GFEXCLUDE:
        case LITERAL_GFINCLUDE:
        case LITERAL_GFID:
        case LITERAL_GFUNREAD:
        case LITERAL_GFARRAYSIZE:
        case LPAREN:
        case LITERAL_GFARRAYSIZES:
        case LITERAL_GFARRAYELEMSIZE:
        case LITERAL_auto:
        case LITERAL_register:
        case LITERAL_static:
        case LITERAL_mutable:
        case LITERAL__inline:
        case LITERAL___inline:
        case LITERAL_virtual:
        case LITERAL_explicit:
        case LITERAL_typename:
        case LITERAL_char:
        case LITERAL_wchar_t:
        case LITERAL_bool:
        case LITERAL_short:
        case LITERAL_int:
        case 50:
        case 51:
        case 52:
        case 53:
        case 54:
        case 55:
        case 56:
        case 57:
        case 58:
        case 59:
        case 60:
        case 61:
        case 62:
        case 63:
        case 64:
        case 65:
        case 66:
        case 67:
        case LITERAL_long:
        case LITERAL_signed:
        case LITERAL_unsigned:
        case LITERAL_float:
        case LITERAL_double:
        case LITERAL_void:
        case LITERAL__declspec:
        case LITERAL___declspec:
        case LITERAL_const:
        case LITERAL___const:
        case LITERAL_volatile:
        case LITERAL___volatile__:
        case OPERATOR:
        case LITERAL_this:
        case LITERAL_true:
        case LITERAL_false:
        case TILDE:
        case STAR:
        case AMPERSAND:
        case ELLIPSIS:
        case SCOPE:
        case LITERAL__cdecl:
        case LITERAL___cdecl:
        case LITERAL__near:
        case LITERAL___near:
        case LITERAL__far:
        case LITERAL___far:
        case LITERAL___interrupt:
        case LITERAL_pascal:
        case LITERAL__pascal:
        case LITERAL___pascal: {
          parameter_list();
          break;
        }
        case RPAREN: {
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
    if (inputState->guessing == 0) {
#line 1365 "CPP_parser.g"

      if (K_and_R == false) {
        in_parameter_list = false;
      } else {
        in_parameter_list = true;
      }

#line 7868 "CPPParser.cpp"
    }
    match(RPAREN);
    {  // ( ... )*
      for (;;) {
        if (((LA(1) >= LITERAL_const && LA(1) <= LITERAL___volatile__)) &&
            (_tokenSet_83.member(LA(2)))) {
          tq = type_qualifier();
        } else {
          goto _loop303;
        }
      }
    _loop303:;
    }  // ( ... )*
    {
      switch (LA(1)) {
        case ASSIGNEQUAL: {
          match(ASSIGNEQUAL);
          match(OCTALINT);
          break;
        }
        case LITERAL_typedef:
        case SEMICOLON:
        case LITERAL_enum:
        case ID:
        case LCURLY:
        case LITERAL_inline:
        case LITERAL_friend:
        case LITERAL_extern:
        case LITERAL_struct:
        case LITERAL_union:
        case LITERAL_class:
        case LITERAL__stdcall:
        case LITERAL___stdcall:
        case LITERAL_GFEXCLUDE:
        case LITERAL_GFINCLUDE:
        case LITERAL_GFID:
        case LITERAL_GFUNREAD:
        case LITERAL_GFARRAYSIZE:
        case LITERAL_GFARRAYSIZES:
        case LITERAL_GFARRAYELEMSIZE:
        case LITERAL_auto:
        case LITERAL_register:
        case LITERAL_static:
        case LITERAL_mutable:
        case LITERAL__inline:
        case LITERAL___inline:
        case LITERAL_virtual:
        case LITERAL_explicit:
        case LITERAL_typename:
        case LITERAL_char:
        case LITERAL_wchar_t:
        case LITERAL_bool:
        case LITERAL_short:
        case LITERAL_int:
        case 50:
        case 51:
        case 52:
        case 53:
        case 54:
        case 55:
        case 56:
        case 57:
        case 58:
        case 59:
        case 60:
        case 61:
        case 62:
        case 63:
        case 64:
        case 65:
        case 66:
        case 67:
        case LITERAL_long:
        case LITERAL_signed:
        case LITERAL_unsigned:
        case LITERAL_float:
        case LITERAL_double:
        case LITERAL_void:
        case LITERAL__declspec:
        case LITERAL___declspec:
        case LITERAL_const:
        case LITERAL___const:
        case LITERAL_volatile:
        case LITERAL___volatile__:
        case LITERAL_throw:
        case LITERAL_using:
        case SCOPE: {
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
    if (inputState->guessing == 0) {
#line 1375 "CPP_parser.g"
      functionEndParameterList(definition);
#line 7970 "CPPParser.cpp"
    }
    {
      switch (LA(1)) {
        case LITERAL_throw: {
          exception_specification();
          break;
        }
        case LITERAL_typedef:
        case SEMICOLON:
        case LITERAL_enum:
        case ID:
        case LCURLY:
        case LITERAL_inline:
        case LITERAL_friend:
        case LITERAL_extern:
        case LITERAL_struct:
        case LITERAL_union:
        case LITERAL_class:
        case LITERAL__stdcall:
        case LITERAL___stdcall:
        case LITERAL_GFEXCLUDE:
        case LITERAL_GFINCLUDE:
        case LITERAL_GFID:
        case LITERAL_GFUNREAD:
        case LITERAL_GFARRAYSIZE:
        case LITERAL_GFARRAYSIZES:
        case LITERAL_GFARRAYELEMSIZE:
        case LITERAL_auto:
        case LITERAL_register:
        case LITERAL_static:
        case LITERAL_mutable:
        case LITERAL__inline:
        case LITERAL___inline:
        case LITERAL_virtual:
        case LITERAL_explicit:
        case LITERAL_typename:
        case LITERAL_char:
        case LITERAL_wchar_t:
        case LITERAL_bool:
        case LITERAL_short:
        case LITERAL_int:
        case 50:
        case 51:
        case 52:
        case 53:
        case 54:
        case 55:
        case 56:
        case 57:
        case 58:
        case 59:
        case 60:
        case 61:
        case 62:
        case 63:
        case 64:
        case 65:
        case 66:
        case 67:
        case LITERAL_long:
        case LITERAL_signed:
        case LITERAL_unsigned:
        case LITERAL_float:
        case LITERAL_double:
        case LITERAL_void:
        case LITERAL__declspec:
        case LITERAL___declspec:
        case LITERAL_const:
        case LITERAL___const:
        case LITERAL_volatile:
        case LITERAL___volatile__:
        case LITERAL_using:
        case SCOPE: {
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_29);
    } else {
      throw;
    }
  }
}

void CPPParser::ctor_head() {
  try {  // for error handling
    ctor_decl_spec();
    ctor_declarator(1);
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_84);
    } else {
      throw;
    }
  }
}

void CPPParser::ctor_body() {
  try {  // for error handling
    {
      switch (LA(1)) {
        case COLON: {
          ctor_initializer();
          break;
        }
        case LCURLY: {
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
    compound_statement();
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_30);
    } else {
      throw;
    }
  }
}

char* CPPParser::qualified_ctor_id() {
#line 1414 "CPP_parser.g"
  char* q = NULL;
#line 8116 "CPPParser.cpp"
  ANTLR_USE_NAMESPACE(antlr) RefToken id = ANTLR_USE_NAMESPACE(antlr) nullToken;
#line 1414 "CPP_parser.g"

  char* so;
  static char qitem[CPPParser_MaxQualifiedItemSize + 1];

#line 8123 "CPPParser.cpp"

  try {  // for error handling
    so = scope_override();
    if (inputState->guessing == 0) {
#line 1421 "CPP_parser.g"
      strcpy(qitem, so);
#line 8130 "CPPParser.cpp"
    }
    id = LT(1);
    match(ID);
    if (inputState->guessing == 0) {
#line 1423 "CPP_parser.g"
      strcat(qitem, (id->getText()).data());
      q = qitem;
#line 8138 "CPPParser.cpp"
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_85);
    } else {
      throw;
    }
  }
  return q;
}

void CPPParser::ctor_initializer() {
  try {  // for error handling
    match(COLON);
    superclass_init();
    {  // ( ... )*
      for (;;) {
        if ((LA(1) == COMMA)) {
          match(COMMA);
          superclass_init();
        } else {
          goto _loop320;
        }
      }
    _loop320:;
    }  // ( ... )*
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_53);
    } else {
      throw;
    }
  }
}

void CPPParser::superclass_init() {
#line 1441 "CPP_parser.g"
  char* q;
#line 8184 "CPPParser.cpp"

  try {  // for error handling
    q = qualified_id();
    match(LPAREN);
    {
      switch (LA(1)) {
        case ID:
        case StringLiteral:
        case LPAREN:
        case LITERAL_typename:
        case LITERAL_char:
        case LITERAL_wchar_t:
        case LITERAL_bool:
        case LITERAL_short:
        case LITERAL_int:
        case 50:
        case 51:
        case 52:
        case 53:
        case 54:
        case 55:
        case 56:
        case 57:
        case 58:
        case 59:
        case 60:
        case 61:
        case 62:
        case 63:
        case 64:
        case 65:
        case 66:
        case 67:
        case LITERAL_long:
        case LITERAL_signed:
        case LITERAL_unsigned:
        case LITERAL_float:
        case LITERAL_double:
        case LITERAL_void:
        case LITERAL__declspec:
        case LITERAL___declspec:
        case OPERATOR:
        case LITERAL_this:
        case LITERAL_true:
        case LITERAL_false:
        case OCTALINT:
        case TILDE:
        case STAR:
        case AMPERSAND:
        case PLUS:
        case MINUS:
        case PLUSPLUS:
        case MINUSMINUS:
        case LITERAL_sizeof:
        case LITERAL___alignof__:
        case SCOPE:
        case LITERAL_dynamic_cast:
        case LITERAL_static_cast:
        case LITERAL_reinterpret_cast:
        case LITERAL_const_cast:
        case LITERAL_typeid:
        case DECIMALINT:
        case HEXADECIMALINT:
        case CharLiteral:
        case WCharLiteral:
        case WStringLiteral:
        case FLOATONE:
        case FLOATTWO:
        case NOT:
        case LITERAL_new:
        case LITERAL_delete: {
          expression_list();
          break;
        }
        case RPAREN: {
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
    match(RPAREN);
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_60);
    } else {
      throw;
    }
  }
}

void CPPParser::dtor_decl_spec() {
  try {  // for error handling
    {    // ( ... )*
      for (;;) {
        switch (LA(1)) {
          case LITERAL_inline:
          case LITERAL__inline:
          case LITERAL___inline: {
            {
              switch (LA(1)) {
                case LITERAL_inline: {
                  match(LITERAL_inline);
                  break;
                }
                case LITERAL__inline: {
                  match(LITERAL__inline);
                  break;
                }
                case LITERAL___inline: {
                  match(LITERAL___inline);
                  break;
                }
                default: {
                  throw ANTLR_USE_NAMESPACE(antlr)
                      NoViableAltException(LT(1), getFilename());
                }
              }
            }
            break;
          }
          case LITERAL_virtual: {
            match(LITERAL_virtual);
            break;
          }
          default: { goto _loop327; }
        }
      }
    _loop327:;
    }  // ( ... )*
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_86);
    } else {
      throw;
    }
  }
}

void CPPParser::dtor_declarator(int definition) {
#line 1461 "CPP_parser.g"
  char* s;
#line 8346 "CPPParser.cpp"

  try {  // for error handling
    s = scope_override();
    match(TILDE);
    match(ID);
    if (inputState->guessing == 0) {
#line 1466 "CPP_parser.g"
      declaratorParameterList(definition);
#line 8355 "CPPParser.cpp"
    }
    match(LPAREN);
    {
      switch (LA(1)) {
        case LITERAL_void: {
          match(LITERAL_void);
          break;
        }
        case RPAREN: {
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
    match(RPAREN);
    if (inputState->guessing == 0) {
#line 1468 "CPP_parser.g"
      declaratorEndParameterList(definition);
#line 8379 "CPPParser.cpp"
    }
    {
      switch (LA(1)) {
        case LITERAL_throw: {
          exception_specification();
          break;
        }
        case SEMICOLON:
        case LCURLY: {
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_32);
    } else {
      throw;
    }
  }
}

void CPPParser::parameter_declaration_list() {
  try {  // for error handling
    {
      parameter_declaration();
      {  // ( ... )*
        for (;;) {
          if ((LA(1) == COMMA)) {
            match(COMMA);
            parameter_declaration();
          } else {
            goto _loop337;
          }
        }
      _loop337:;
      }  // ( ... )*
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_87);
    } else {
      throw;
    }
  }
}

void CPPParser::parameter_declaration() {
  try {  // for error handling
    if (inputState->guessing == 0) {
#line 1494 "CPP_parser.g"
      beginParameterDeclaration();
#line 8446 "CPPParser.cpp"
    }
    {
      if (((_tokenSet_12.member(LA(1))) && (_tokenSet_88.member(LA(2)))) &&
          (!((LA(1) == SCOPE) && (LA(2) == STAR || LA(2) == OPERATOR)) &&
           (!(LA(1) == SCOPE || LA(1) == ID) ||
            qualifiedItemIsOneOf(qiType | qiCtor)))) {
        declaration_specifiers();
        {
          bool synPredMatched342 = false;
          if (((_tokenSet_76.member(LA(1))) && (_tokenSet_89.member(LA(2))))) {
            int _m342 = mark();
            synPredMatched342 = true;
            inputState->guessing++;
            try {
              { declarator(); }
            } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
              synPredMatched342 = false;
            }
            rewind(_m342);
            inputState->guessing--;
          }
          if (synPredMatched342) {
            declarator();
          } else if ((_tokenSet_90.member(LA(1))) &&
                     (_tokenSet_91.member(LA(2)))) {
            abstract_declarator();
          } else {
            throw ANTLR_USE_NAMESPACE(antlr)
                NoViableAltException(LT(1), getFilename());
          }
        }
      } else {
        bool synPredMatched344 = false;
        if (((_tokenSet_76.member(LA(1))) && (_tokenSet_89.member(LA(2))))) {
          int _m344 = mark();
          synPredMatched344 = true;
          inputState->guessing++;
          try {
            { declarator(); }
          } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
            synPredMatched344 = false;
          }
          rewind(_m344);
          inputState->guessing--;
        }
        if (synPredMatched344) {
          declarator();
        } else if ((LA(1) == ELLIPSIS)) {
          match(ELLIPSIS);
        } else {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
    {
      switch (LA(1)) {
        case ASSIGNEQUAL: {
          match(ASSIGNEQUAL);
          remainder_expression();
          break;
        }
        case GREATERTHAN:
        case RPAREN:
        case COMMA:
        case ELLIPSIS: {
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_92);
    } else {
      throw;
    }
  }
}

void CPPParser::abstract_declarator() {
  try {  // for error handling
    if ((_tokenSet_25.member(LA(1)))) {
      ptr_operator();
      abstract_declarator();
    } else {
      bool synPredMatched351 = false;
      if (((LA(1) == LPAREN) && (_tokenSet_25.member(LA(2))))) {
        int _m351 = mark();
        synPredMatched351 = true;
        inputState->guessing++;
        try {
          {
            match(LPAREN);
            {  // ( ... )+
              int _cnt350 = 0;
              for (;;) {
                if ((_tokenSet_25.member(LA(1)))) {
                  ptr_operator();
                } else {
                  if (_cnt350 >= 1) {
                    goto _loop350;
                  } else {
                    throw ANTLR_USE_NAMESPACE(antlr)
                        NoViableAltException(LT(1), getFilename());
                  }
                }

                _cnt350++;
              }
            _loop350:;
            }  // ( ... )+
            match(RPAREN);
          }
        } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
          synPredMatched351 = false;
        }
        rewind(_m351);
        inputState->guessing--;
      }
      if (synPredMatched351) {
        match(LPAREN);
        {  // ( ... )+
          int _cnt353 = 0;
          for (;;) {
            if ((_tokenSet_25.member(LA(1)))) {
              ptr_operator();
            } else {
              if (_cnt353 >= 1) {
                goto _loop353;
              } else {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }

            _cnt353++;
          }
        _loop353:;
        }  // ( ... )+
        match(RPAREN);
        {
          switch (LA(1)) {
            case LPAREN:
            case LSQUARE: {
              abstract_declarator_suffix();
              break;
            }
            case GREATERTHAN:
            case ASSIGNEQUAL:
            case RPAREN:
            case COMMA:
            case ELLIPSIS: {
              break;
            }
            default: {
              throw ANTLR_USE_NAMESPACE(antlr)
                  NoViableAltException(LT(1), getFilename());
            }
          }
        }
      } else if ((_tokenSet_93.member(LA(1))) && (_tokenSet_94.member(LA(2)))) {
        {
          switch (LA(1)) {
            case LPAREN:
            case LSQUARE: {
              abstract_declarator_suffix();
              break;
            }
            case GREATERTHAN:
            case ASSIGNEQUAL:
            case RPAREN:
            case COMMA:
            case ELLIPSIS: {
              break;
            }
            default: {
              throw ANTLR_USE_NAMESPACE(antlr)
                  NoViableAltException(LT(1), getFilename());
            }
          }
        }
      } else {
        throw ANTLR_USE_NAMESPACE(antlr)
            NoViableAltException(LT(1), getFilename());
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_95);
    } else {
      throw;
    }
  }
}

void CPPParser::type_id() {
  try {  // for error handling
    declaration_specifiers();
    abstract_declarator();
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_96);
    } else {
      throw;
    }
  }
}

void CPPParser::abstract_declarator_suffix() {
  try {  // for error handling
    switch (LA(1)) {
      case LSQUARE: {
        {  // ( ... )+
          int _cnt359 = 0;
          for (;;) {
            if ((LA(1) == LSQUARE)) {
              match(LSQUARE);
              {
                switch (LA(1)) {
                  case ID:
                  case StringLiteral:
                  case LPAREN:
                  case LITERAL_typename:
                  case LITERAL_char:
                  case LITERAL_wchar_t:
                  case LITERAL_bool:
                  case LITERAL_short:
                  case LITERAL_int:
                  case 50:
                  case 51:
                  case 52:
                  case 53:
                  case 54:
                  case 55:
                  case 56:
                  case 57:
                  case 58:
                  case 59:
                  case 60:
                  case 61:
                  case 62:
                  case 63:
                  case 64:
                  case 65:
                  case 66:
                  case 67:
                  case LITERAL_long:
                  case LITERAL_signed:
                  case LITERAL_unsigned:
                  case LITERAL_float:
                  case LITERAL_double:
                  case LITERAL_void:
                  case LITERAL__declspec:
                  case LITERAL___declspec:
                  case OPERATOR:
                  case LITERAL_this:
                  case LITERAL_true:
                  case LITERAL_false:
                  case OCTALINT:
                  case TILDE:
                  case STAR:
                  case AMPERSAND:
                  case PLUS:
                  case MINUS:
                  case PLUSPLUS:
                  case MINUSMINUS:
                  case LITERAL_sizeof:
                  case LITERAL___alignof__:
                  case SCOPE:
                  case LITERAL_dynamic_cast:
                  case LITERAL_static_cast:
                  case LITERAL_reinterpret_cast:
                  case LITERAL_const_cast:
                  case LITERAL_typeid:
                  case DECIMALINT:
                  case HEXADECIMALINT:
                  case CharLiteral:
                  case WCharLiteral:
                  case WStringLiteral:
                  case FLOATONE:
                  case FLOATTWO:
                  case NOT:
                  case LITERAL_new:
                  case LITERAL_delete: {
                    constant_expression();
                    break;
                  }
                  case RSQUARE: {
                    break;
                  }
                  default: {
                    throw ANTLR_USE_NAMESPACE(antlr)
                        NoViableAltException(LT(1), getFilename());
                  }
                }
              }
              match(RSQUARE);
            } else {
              if (_cnt359 >= 1) {
                goto _loop359;
              } else {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }

            _cnt359++;
          }
        _loop359:;
        }  // ( ... )+
        if (inputState->guessing == 0) {
#line 1548 "CPP_parser.g"
          declaratorArray();
#line 8780 "CPPParser.cpp"
        }
        break;
      }
      case LPAREN: {
        match(LPAREN);
        if (inputState->guessing == 0) {
#line 1551 "CPP_parser.g"
          declaratorParameterList(0);
#line 8790 "CPPParser.cpp"
        }
        {
          switch (LA(1)) {
            case LITERAL_typedef:
            case LITERAL_enum:
            case ID:
            case LITERAL_inline:
            case LITERAL_friend:
            case LITERAL_extern:
            case LITERAL_struct:
            case LITERAL_union:
            case LITERAL_class:
            case LITERAL__stdcall:
            case LITERAL___stdcall:
            case LITERAL_GFEXCLUDE:
            case LITERAL_GFINCLUDE:
            case LITERAL_GFID:
            case LITERAL_GFUNREAD:
            case LITERAL_GFARRAYSIZE:
            case LPAREN:
            case LITERAL_GFARRAYSIZES:
            case LITERAL_GFARRAYELEMSIZE:
            case LITERAL_auto:
            case LITERAL_register:
            case LITERAL_static:
            case LITERAL_mutable:
            case LITERAL__inline:
            case LITERAL___inline:
            case LITERAL_virtual:
            case LITERAL_explicit:
            case LITERAL_typename:
            case LITERAL_char:
            case LITERAL_wchar_t:
            case LITERAL_bool:
            case LITERAL_short:
            case LITERAL_int:
            case 50:
            case 51:
            case 52:
            case 53:
            case 54:
            case 55:
            case 56:
            case 57:
            case 58:
            case 59:
            case 60:
            case 61:
            case 62:
            case 63:
            case 64:
            case 65:
            case 66:
            case 67:
            case LITERAL_long:
            case LITERAL_signed:
            case LITERAL_unsigned:
            case LITERAL_float:
            case LITERAL_double:
            case LITERAL_void:
            case LITERAL__declspec:
            case LITERAL___declspec:
            case LITERAL_const:
            case LITERAL___const:
            case LITERAL_volatile:
            case LITERAL___volatile__:
            case OPERATOR:
            case LITERAL_this:
            case LITERAL_true:
            case LITERAL_false:
            case TILDE:
            case STAR:
            case AMPERSAND:
            case ELLIPSIS:
            case SCOPE:
            case LITERAL__cdecl:
            case LITERAL___cdecl:
            case LITERAL__near:
            case LITERAL___near:
            case LITERAL__far:
            case LITERAL___far:
            case LITERAL___interrupt:
            case LITERAL_pascal:
            case LITERAL__pascal:
            case LITERAL___pascal: {
              parameter_list();
              break;
            }
            case RPAREN: {
              break;
            }
            default: {
              throw ANTLR_USE_NAMESPACE(antlr)
                  NoViableAltException(LT(1), getFilename());
            }
          }
        }
        match(RPAREN);
        if (inputState->guessing == 0) {
#line 1554 "CPP_parser.g"
          declaratorEndParameterList(0);
#line 8894 "CPPParser.cpp"
        }
        cv_qualifier_seq();
        {
          switch (LA(1)) {
            case LITERAL_throw: {
              exception_specification();
              break;
            }
            case GREATERTHAN:
            case ASSIGNEQUAL:
            case RPAREN:
            case COMMA:
            case ELLIPSIS: {
              break;
            }
            default: {
              throw ANTLR_USE_NAMESPACE(antlr)
                  NoViableAltException(LT(1), getFilename());
            }
          }
        }
        break;
      }
      default: {
        throw ANTLR_USE_NAMESPACE(antlr)
            NoViableAltException(LT(1), getFilename());
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_95);
    } else {
      throw;
    }
  }
}

void CPPParser::cv_qualifier_seq() {
#line 2218 "CPP_parser.g"
  CPPParser::TypeQualifier tq;
#line 8939 "CPPParser.cpp"

  try {  // for error handling
    {    // ( ... )*
      for (;;) {
        if (((LA(1) >= LITERAL_const && LA(1) <= LITERAL___volatile__))) {
          tq = type_qualifier();
        } else {
          goto _loop581;
        }
      }
    _loop581:;
    }  // ( ... )*
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_97);
    } else {
      throw;
    }
  }
}

void CPPParser::template_parameter() {
  try {  // for error handling
    {
      if ((_tokenSet_98.member(LA(1))) && (_tokenSet_99.member(LA(2)))) {
        type_parameter();
      } else if ((_tokenSet_100.member(LA(1))) &&
                 (_tokenSet_15.member(LA(2)))) {
        parameter_declaration();
      } else {
        throw ANTLR_USE_NAMESPACE(antlr)
            NoViableAltException(LT(1), getFilename());
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_101);
    } else {
      throw;
    }
  }
}

void CPPParser::type_parameter() {
  ANTLR_USE_NAMESPACE(antlr) RefToken id = ANTLR_USE_NAMESPACE(antlr) nullToken;
  ANTLR_USE_NAMESPACE(antlr)
  RefToken id2 = ANTLR_USE_NAMESPACE(antlr) nullToken;

  try {  // for error handling
    {
      switch (LA(1)) {
        case LITERAL_class:
        case LITERAL_typename: {
          {
            switch (LA(1)) {
              case LITERAL_class: {
                match(LITERAL_class);
                break;
              }
              case LITERAL_typename: {
                match(LITERAL_typename);
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          {
            switch (LA(1)) {
              case ID: {
                id = LT(1);
                match(ID);
                if (inputState->guessing == 0) {
#line 1610 "CPP_parser.g"

                  templateTypeParameter((id->getText()).data());

#line 9030 "CPPParser.cpp"
                }
                {
                  switch (LA(1)) {
                    case ASSIGNEQUAL: {
                      match(ASSIGNEQUAL);
                      assigned_type_name();
                      break;
                    }
                    case GREATERTHAN:
                    case COMMA: {
                      break;
                    }
                    default: {
                      throw ANTLR_USE_NAMESPACE(antlr)
                          NoViableAltException(LT(1), getFilename());
                    }
                  }
                }
                break;
              }
              case GREATERTHAN:
              case COMMA: {
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          break;
        }
        case LITERAL_template: {
          template_head();
          match(LITERAL_class);
          {
            switch (LA(1)) {
              case ID: {
                id2 = LT(1);
                match(ID);
                if (inputState->guessing == 0) {
#line 1618 "CPP_parser.g"

                  templateTypeParameter((id2->getText()).data());

#line 9081 "CPPParser.cpp"
                }
                {
                  switch (LA(1)) {
                    case ASSIGNEQUAL: {
                      match(ASSIGNEQUAL);
                      assigned_type_name();
                      break;
                    }
                    case GREATERTHAN:
                    case COMMA: {
                      break;
                    }
                    default: {
                      throw ANTLR_USE_NAMESPACE(antlr)
                          NoViableAltException(LT(1), getFilename());
                    }
                  }
                }
                break;
              }
              case GREATERTHAN:
              case COMMA: {
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_101);
    } else {
      throw;
    }
  }
}

void CPPParser::assigned_type_name() {
#line 1631 "CPP_parser.g"
  char* qt;
  TypeSpecifier ts;
#line 9137 "CPPParser.cpp"

  try {  // for error handling
    {
      if ((LA(1) == ID || LA(1) == SCOPE) && (_tokenSet_102.member(LA(2)))) {
        qt = qualified_type();
        abstract_declarator();
      } else if ((_tokenSet_103.member(LA(1))) &&
                 (_tokenSet_104.member(LA(2)))) {
        ts = simple_type_specifier();
        abstract_declarator();
      } else {
        throw ANTLR_USE_NAMESPACE(antlr)
            NoViableAltException(LT(1), getFilename());
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_101);
    } else {
      throw;
    }
  }
}

void CPPParser::template_id() {
  try {  // for error handling
    match(ID);
    match(LESSTHAN);
    template_argument_list();
    match(GREATERTHAN);
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_1);
    } else {
      throw;
    }
  }
}

void CPPParser::template_argument() {
  try {  // for error handling
    if (((_tokenSet_12.member(LA(1))) && (_tokenSet_105.member(LA(2)))) &&
        ((!(LA(1) == SCOPE || LA(1) == ID) ||
          qualifiedItemIsOneOf(qiType | qiCtor)))) {
      type_id();
    } else if ((_tokenSet_72.member(LA(1))) && (_tokenSet_106.member(LA(2)))) {
      shift_expression();
    } else {
      throw ANTLR_USE_NAMESPACE(antlr)
          NoViableAltException(LT(1), getFilename());
    }

  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_101);
    } else {
      throw;
    }
  }
}

void CPPParser::shift_expression() {
  try {  // for error handling
    additive_expression();
    {  // ( ... )*
      for (;;) {
        if ((LA(1) == SHIFTLEFT || LA(1) == SHIFTRIGHT)) {
          {
            switch (LA(1)) {
              case SHIFTLEFT: {
                match(SHIFTLEFT);
                break;
              }
              case SHIFTRIGHT: {
                match(SHIFTRIGHT);
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          additive_expression();
        } else {
          goto _loop482;
        }
      }
    _loop482:;
    }  // ( ... )*
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_107);
    } else {
      throw;
    }
  }
}

void CPPParser::statement_list() {
  try {  // for error handling
    {    // ( ... )+
      int _cnt389 = 0;
      for (;;) {
        if ((_tokenSet_49.member(LA(1)))) {
          statement();
        } else {
          if (_cnt389 >= 1) {
            goto _loop389;
          } else {
            throw ANTLR_USE_NAMESPACE(antlr)
                NoViableAltException(LT(1), getFilename());
          }
        }

        _cnt389++;
      }
    _loop389:;
    }  // ( ... )+
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_61);
    } else {
      throw;
    }
  }
}

void CPPParser::statement() {
#line 1680 "CPP_parser.g"

  FunctionSpecifier fs = fsInvalid;  // inline,virtual,explicit

#line 9285 "CPPParser.cpp"

  try {  // for error handling
    {
      switch (LA(1)) {
        case LITERAL_case: {
          case_statement();
          break;
        }
        case LITERAL_default: {
          default_statement();
          break;
        }
        case LCURLY: {
          compound_statement();
          break;
        }
        case LITERAL_if:
        case LITERAL_switch: {
          selection_statement();
          break;
        }
        case LITERAL_while:
        case LITERAL_do:
        case LITERAL_for: {
          iteration_statement();
          break;
        }
        case LITERAL_goto:
        case LITERAL_continue:
        case LITERAL_break:
        case LITERAL_return: {
          jump_statement();
          break;
        }
        case LITERAL_try: {
          try_block();
          break;
        }
        case LITERAL_throw: {
          throw_statement();
          break;
        }
        default:
          bool synPredMatched393 = false;
          if (((_tokenSet_108.member(LA(1))) &&
               (_tokenSet_109.member(LA(2))))) {
            int _m393 = mark();
            synPredMatched393 = true;
            inputState->guessing++;
            try {
              {
                switch (LA(1)) {
                  case LITERAL_namespace: {
                    match(LITERAL_namespace);
                    break;
                  }
                  case LITERAL_using: {
                    match(LITERAL_using);
                    break;
                  }
                  default: {
                    throw ANTLR_USE_NAMESPACE(antlr)
                        NoViableAltException(LT(1), getFilename());
                  }
                }
              }
            } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
              synPredMatched393 = false;
            }
            rewind(_m393);
            inputState->guessing--;
          }
          if (synPredMatched393) {
            block_declaration();
          } else {
            bool synPredMatched397 = false;
            if (((_tokenSet_20.member(LA(1))) &&
                 (_tokenSet_110.member(LA(2))))) {
              int _m397 = mark();
              synPredMatched397 = true;
              inputState->guessing++;
              try {
                {
                  {
                    switch (LA(1)) {
                      case LITERAL_typedef: {
                        match(LITERAL_typedef);
                        break;
                      }
                      case LITERAL_struct:
                      case LITERAL_union:
                      case LITERAL_class: {
                        break;
                      }
                      default: {
                        throw ANTLR_USE_NAMESPACE(antlr)
                            NoViableAltException(LT(1), getFilename());
                      }
                    }
                  }
                  class_specifier();
                  {
                    switch (LA(1)) {
                      case ID:
                      case OPERATOR:
                      case LITERAL_this:
                      case LITERAL_true:
                      case LITERAL_false:
                      case SCOPE: {
                        qualified_id();
                        break;
                      }
                      case LCURLY: {
                        break;
                      }
                      default: {
                        throw ANTLR_USE_NAMESPACE(antlr)
                            NoViableAltException(LT(1), getFilename());
                      }
                    }
                  }
                  match(LCURLY);
                }
              } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
                synPredMatched397 = false;
              }
              rewind(_m397);
              inputState->guessing--;
            }
            if (synPredMatched397) {
              member_declaration();
            } else {
              bool synPredMatched399 = false;
              if (((_tokenSet_20.member(LA(1))) &&
                   (_tokenSet_110.member(LA(2))))) {
                int _m399 = mark();
                synPredMatched399 = true;
                inputState->guessing++;
                try {
                  {
                    declaration_specifiers();
                    match(LPAREN);
                    ptr_operator();
                    qualified_id();
                    match(RPAREN);
                  }
                } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
                  synPredMatched399 = false;
                }
                rewind(_m399);
                inputState->guessing--;
              }
              if (synPredMatched399) {
                member_declaration();
              } else {
                bool synPredMatched402 = false;
                if (((_tokenSet_20.member(LA(1))) &&
                     (_tokenSet_110.member(LA(2))))) {
                  int _m402 = mark();
                  synPredMatched402 = true;
                  inputState->guessing++;
                  try {
                    {
                      declaration_specifiers();
                      {
                        ptr_operator();
                        ptr_operator();
                      }
                      qualified_id();
                    }
                  } catch (ANTLR_USE_NAMESPACE(antlr)
                               RecognitionException& pe) {
                    synPredMatched402 = false;
                  }
                  rewind(_m402);
                  inputState->guessing--;
                }
                if (synPredMatched402) {
                  member_declaration();
                } else {
                  bool synPredMatched407 = false;
                  if (((_tokenSet_20.member(LA(1))) &&
                       (_tokenSet_110.member(LA(2))))) {
                    int _m407 = mark();
                    synPredMatched407 = true;
                    inputState->guessing++;
                    try {
                      {
                        declaration_specifiers();
                        {
                          bool synPredMatched406 = false;
                          if (((_tokenSet_25.member(LA(1))) &&
                               (_tokenSet_111.member(LA(2))))) {
                            int _m406 = mark();
                            synPredMatched406 = true;
                            inputState->guessing++;
                            try {
                              { ptr_operator(); }
                            } catch (ANTLR_USE_NAMESPACE(antlr)
                                         RecognitionException& pe) {
                              synPredMatched406 = false;
                            }
                            rewind(_m406);
                            inputState->guessing--;
                          }
                          if (synPredMatched406) {
                            ptr_operator();
                          } else if ((_tokenSet_44.member(LA(1))) && (true)) {
                          } else {
                            throw ANTLR_USE_NAMESPACE(antlr)
                                NoViableAltException(LT(1), getFilename());
                          }
                        }
                        qualified_id();
                      }
                    } catch (ANTLR_USE_NAMESPACE(antlr)
                                 RecognitionException& pe) {
                      synPredMatched407 = false;
                    }
                    rewind(_m407);
                    inputState->guessing--;
                  }
                  if (synPredMatched407) {
                    member_declaration();
                  } else if ((LA(1) == ID) && (LA(2) == COLON)) {
                    labeled_statement();
                  } else if ((_tokenSet_72.member(LA(1))) &&
                             (_tokenSet_112.member(LA(2)))) {
                    expression();
                    match(SEMICOLON);
                    if (inputState->guessing == 0) {
#line 1698 "CPP_parser.g"
                      end_of_stmt();
#line 9539 "CPPParser.cpp"
                    }
                  } else if ((LA(1) == SEMICOLON) &&
                             (_tokenSet_40.member(LA(2)))) {
                    match(SEMICOLON);
                    if (inputState->guessing == 0) {
#line 1703 "CPP_parser.g"
                      end_of_stmt();
#line 9547 "CPPParser.cpp"
                    }
                  } else if ((LA(1) == LITERAL_antlrTrace_on) &&
                             (_tokenSet_40.member(LA(2)))) {
                    match(LITERAL_antlrTrace_on);
                    if (inputState->guessing == 0) {
#line 1708 "CPP_parser.g"
                      antlrTrace(true);
#line 9555 "CPPParser.cpp"
                    }
                  } else if ((LA(1) == LITERAL_antlrTrace_off) &&
                             (_tokenSet_40.member(LA(2)))) {
                    match(LITERAL_antlrTrace_off);
                    if (inputState->guessing == 0) {
#line 1710 "CPP_parser.g"
                      antlrTrace(false);
#line 9563 "CPPParser.cpp"
                    }
                  } else {
                    throw ANTLR_USE_NAMESPACE(antlr)
                        NoViableAltException(LT(1), getFilename());
                  }
                }
              }
            }
          }
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_40);
    } else {
      throw;
    }
  }
}

void CPPParser::block_declaration() {
  try {  // for error handling
    switch (LA(1)) {
      case LITERAL_typedef:
      case LITERAL_enum:
      case ID:
      case LITERAL_inline:
      case LITERAL_friend:
      case LITERAL_extern:
      case LITERAL_struct:
      case LITERAL_union:
      case LITERAL_class:
      case LITERAL__stdcall:
      case LITERAL___stdcall:
      case LITERAL_GFEXCLUDE:
      case LITERAL_GFINCLUDE:
      case LITERAL_GFID:
      case LITERAL_GFUNREAD:
      case LITERAL_GFARRAYSIZE:
      case LITERAL_GFARRAYSIZES:
      case LITERAL_GFARRAYELEMSIZE:
      case LITERAL_auto:
      case LITERAL_register:
      case LITERAL_static:
      case LITERAL_mutable:
      case LITERAL__inline:
      case LITERAL___inline:
      case LITERAL_virtual:
      case LITERAL_explicit:
      case LITERAL_typename:
      case LITERAL_char:
      case LITERAL_wchar_t:
      case LITERAL_bool:
      case LITERAL_short:
      case LITERAL_int:
      case 50:
      case 51:
      case 52:
      case 53:
      case 54:
      case 55:
      case 56:
      case 57:
      case 58:
      case 59:
      case 60:
      case 61:
      case 62:
      case 63:
      case 64:
      case 65:
      case 66:
      case 67:
      case LITERAL_long:
      case LITERAL_signed:
      case LITERAL_unsigned:
      case LITERAL_float:
      case LITERAL_double:
      case LITERAL_void:
      case LITERAL__declspec:
      case LITERAL___declspec:
      case LITERAL_const:
      case LITERAL___const:
      case LITERAL_volatile:
      case LITERAL___volatile__:
      case SCOPE: {
        simple_declaration();
        break;
      }
      case LITERAL_asm:
      case LITERAL__asm:
      case LITERAL___asm:
      case LITERAL___asm__: {
        asm_definition();
        break;
      }
      case LITERAL_namespace: {
        namespace_alias_definition();
        break;
      }
      case LITERAL_using: {
        using_statement();
        break;
      }
      default: {
        throw ANTLR_USE_NAMESPACE(antlr)
            NoViableAltException(LT(1), getFilename());
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_40);
    } else {
      throw;
    }
  }
}

void CPPParser::labeled_statement() {
  try {  // for error handling
    match(ID);
    match(COLON);
    statement();
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_40);
    } else {
      throw;
    }
  }
}

void CPPParser::case_statement() {
  try {  // for error handling
    match(LITERAL_case);
    constant_expression();
    match(COLON);
    statement();
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_40);
    } else {
      throw;
    }
  }
}

void CPPParser::default_statement() {
  try {  // for error handling
    match(LITERAL_default);
    match(COLON);
    statement();
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_40);
    } else {
      throw;
    }
  }
}

void CPPParser::selection_statement() {
  try {  // for error handling
    switch (LA(1)) {
      case LITERAL_if: {
        match(LITERAL_if);
        match(LPAREN);
        expression();
        match(RPAREN);
        statement();
        {
          if ((LA(1) == LITERAL_else) && (_tokenSet_49.member(LA(2)))) {
            match(LITERAL_else);
            statement();
          } else if ((_tokenSet_40.member(LA(1))) &&
                     (_tokenSet_113.member(LA(2)))) {
          } else {
            throw ANTLR_USE_NAMESPACE(antlr)
                NoViableAltException(LT(1), getFilename());
          }
        }
        break;
      }
      case LITERAL_switch: {
        match(LITERAL_switch);
        match(LPAREN);
        expression();
        match(RPAREN);
        statement();
        break;
      }
      default: {
        throw ANTLR_USE_NAMESPACE(antlr)
            NoViableAltException(LT(1), getFilename());
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_40);
    } else {
      throw;
    }
  }
}

void CPPParser::iteration_statement() {
  try {  // for error handling
    switch (LA(1)) {
      case LITERAL_while: {
        match(LITERAL_while);
        match(LPAREN);
        expression();
        match(RPAREN);
        statement();
        break;
      }
      case LITERAL_do: {
        match(LITERAL_do);
        statement();
        match(LITERAL_while);
        match(LPAREN);
        expression();
        match(RPAREN);
        match(SEMICOLON);
        if (inputState->guessing == 0) {
#line 1777 "CPP_parser.g"
          end_of_stmt();
#line 9814 "CPPParser.cpp"
        }
        break;
      }
      case LITERAL_for: {
        match(LITERAL_for);
        match(LPAREN);
        {
          bool synPredMatched421 = false;
          if (((_tokenSet_2.member(LA(1))) && (_tokenSet_3.member(LA(2))))) {
            int _m421 = mark();
            synPredMatched421 = true;
            inputState->guessing++;
            try {
              { declaration(); }
            } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
              synPredMatched421 = false;
            }
            rewind(_m421);
            inputState->guessing--;
          }
          if (synPredMatched421) {
            declaration();
          } else if ((_tokenSet_72.member(LA(1))) &&
                     (_tokenSet_112.member(LA(2)))) {
            expression();
            match(SEMICOLON);
            if (inputState->guessing == 0) {
#line 1781 "CPP_parser.g"
              end_of_stmt();
#line 9848 "CPPParser.cpp"
            }
          } else if ((LA(1) == SEMICOLON)) {
            match(SEMICOLON);
            if (inputState->guessing == 0) {
#line 1782 "CPP_parser.g"
              end_of_stmt();
#line 9856 "CPPParser.cpp"
            }
          } else {
            throw ANTLR_USE_NAMESPACE(antlr)
                NoViableAltException(LT(1), getFilename());
          }
        }
        {
          switch (LA(1)) {
            case ID:
            case StringLiteral:
            case LPAREN:
            case LITERAL_typename:
            case LITERAL_char:
            case LITERAL_wchar_t:
            case LITERAL_bool:
            case LITERAL_short:
            case LITERAL_int:
            case 50:
            case 51:
            case 52:
            case 53:
            case 54:
            case 55:
            case 56:
            case 57:
            case 58:
            case 59:
            case 60:
            case 61:
            case 62:
            case 63:
            case 64:
            case 65:
            case 66:
            case 67:
            case LITERAL_long:
            case LITERAL_signed:
            case LITERAL_unsigned:
            case LITERAL_float:
            case LITERAL_double:
            case LITERAL_void:
            case LITERAL__declspec:
            case LITERAL___declspec:
            case OPERATOR:
            case LITERAL_this:
            case LITERAL_true:
            case LITERAL_false:
            case OCTALINT:
            case TILDE:
            case STAR:
            case AMPERSAND:
            case PLUS:
            case MINUS:
            case PLUSPLUS:
            case MINUSMINUS:
            case LITERAL_sizeof:
            case LITERAL___alignof__:
            case SCOPE:
            case LITERAL_dynamic_cast:
            case LITERAL_static_cast:
            case LITERAL_reinterpret_cast:
            case LITERAL_const_cast:
            case LITERAL_typeid:
            case DECIMALINT:
            case HEXADECIMALINT:
            case CharLiteral:
            case WCharLiteral:
            case WStringLiteral:
            case FLOATONE:
            case FLOATTWO:
            case NOT:
            case LITERAL_new:
            case LITERAL_delete: {
              expression();
              break;
            }
            case SEMICOLON: {
              break;
            }
            default: {
              throw ANTLR_USE_NAMESPACE(antlr)
                  NoViableAltException(LT(1), getFilename());
            }
          }
        }
        match(SEMICOLON);
        if (inputState->guessing == 0) {
#line 1784 "CPP_parser.g"
          end_of_stmt();
#line 9949 "CPPParser.cpp"
        }
        {
          switch (LA(1)) {
            case ID:
            case StringLiteral:
            case LPAREN:
            case LITERAL_typename:
            case LITERAL_char:
            case LITERAL_wchar_t:
            case LITERAL_bool:
            case LITERAL_short:
            case LITERAL_int:
            case 50:
            case 51:
            case 52:
            case 53:
            case 54:
            case 55:
            case 56:
            case 57:
            case 58:
            case 59:
            case 60:
            case 61:
            case 62:
            case 63:
            case 64:
            case 65:
            case 66:
            case 67:
            case LITERAL_long:
            case LITERAL_signed:
            case LITERAL_unsigned:
            case LITERAL_float:
            case LITERAL_double:
            case LITERAL_void:
            case LITERAL__declspec:
            case LITERAL___declspec:
            case OPERATOR:
            case LITERAL_this:
            case LITERAL_true:
            case LITERAL_false:
            case OCTALINT:
            case TILDE:
            case STAR:
            case AMPERSAND:
            case PLUS:
            case MINUS:
            case PLUSPLUS:
            case MINUSMINUS:
            case LITERAL_sizeof:
            case LITERAL___alignof__:
            case SCOPE:
            case LITERAL_dynamic_cast:
            case LITERAL_static_cast:
            case LITERAL_reinterpret_cast:
            case LITERAL_const_cast:
            case LITERAL_typeid:
            case DECIMALINT:
            case HEXADECIMALINT:
            case CharLiteral:
            case WCharLiteral:
            case WStringLiteral:
            case FLOATONE:
            case FLOATTWO:
            case NOT:
            case LITERAL_new:
            case LITERAL_delete: {
              expression();
              break;
            }
            case RPAREN: {
              break;
            }
            default: {
              throw ANTLR_USE_NAMESPACE(antlr)
                  NoViableAltException(LT(1), getFilename());
            }
          }
        }
        match(RPAREN);
        statement();
        break;
      }
      default: {
        throw ANTLR_USE_NAMESPACE(antlr)
            NoViableAltException(LT(1), getFilename());
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_40);
    } else {
      throw;
    }
  }
}

void CPPParser::jump_statement() {
  try {  // for error handling
    {
      switch (LA(1)) {
        case LITERAL_goto: {
          match(LITERAL_goto);
          match(ID);
          match(SEMICOLON);
          if (inputState->guessing == 0) {
#line 1791 "CPP_parser.g"
            end_of_stmt();
#line 10065 "CPPParser.cpp"
          }
          break;
        }
        case LITERAL_continue: {
          match(LITERAL_continue);
          match(SEMICOLON);
          if (inputState->guessing == 0) {
#line 1792 "CPP_parser.g"
            end_of_stmt();
#line 10076 "CPPParser.cpp"
          }
          break;
        }
        case LITERAL_break: {
          match(LITERAL_break);
          match(SEMICOLON);
          if (inputState->guessing == 0) {
#line 1793 "CPP_parser.g"
            end_of_stmt();
#line 10087 "CPPParser.cpp"
          }
          break;
        }
        case LITERAL_return: {
          match(LITERAL_return);
          if (inputState->guessing == 0) {
#line 1795 "CPP_parser.g"
            in_return = true;
#line 10097 "CPPParser.cpp"
          }
          {
            bool synPredMatched428 = false;
            if (((LA(1) == LPAREN) && (LA(2) == ID))) {
              int _m428 = mark();
              synPredMatched428 = true;
              inputState->guessing++;
              try {
                {
                  match(LPAREN);
                  if (!((qualifiedItemIsOneOf(qiType)))) {
                    throw ANTLR_USE_NAMESPACE(antlr)
                        SemanticException("(qualifiedItemIsOneOf(qiType) )");
                  }
                  match(ID);
                  match(RPAREN);
                }
              } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
                synPredMatched428 = false;
              }
              rewind(_m428);
              inputState->guessing--;
            }
            if (synPredMatched428) {
              match(LPAREN);
              match(ID);
              match(RPAREN);
              {
                switch (LA(1)) {
                  case ID:
                  case StringLiteral:
                  case LPAREN:
                  case LITERAL_typename:
                  case LITERAL_char:
                  case LITERAL_wchar_t:
                  case LITERAL_bool:
                  case LITERAL_short:
                  case LITERAL_int:
                  case 50:
                  case 51:
                  case 52:
                  case 53:
                  case 54:
                  case 55:
                  case 56:
                  case 57:
                  case 58:
                  case 59:
                  case 60:
                  case 61:
                  case 62:
                  case 63:
                  case 64:
                  case 65:
                  case 66:
                  case 67:
                  case LITERAL_long:
                  case LITERAL_signed:
                  case LITERAL_unsigned:
                  case LITERAL_float:
                  case LITERAL_double:
                  case LITERAL_void:
                  case LITERAL__declspec:
                  case LITERAL___declspec:
                  case OPERATOR:
                  case LITERAL_this:
                  case LITERAL_true:
                  case LITERAL_false:
                  case OCTALINT:
                  case TILDE:
                  case STAR:
                  case AMPERSAND:
                  case PLUS:
                  case MINUS:
                  case PLUSPLUS:
                  case MINUSMINUS:
                  case LITERAL_sizeof:
                  case LITERAL___alignof__:
                  case SCOPE:
                  case LITERAL_dynamic_cast:
                  case LITERAL_static_cast:
                  case LITERAL_reinterpret_cast:
                  case LITERAL_const_cast:
                  case LITERAL_typeid:
                  case DECIMALINT:
                  case HEXADECIMALINT:
                  case CharLiteral:
                  case WCharLiteral:
                  case WStringLiteral:
                  case FLOATONE:
                  case FLOATTWO:
                  case NOT:
                  case LITERAL_new:
                  case LITERAL_delete: {
                    expression();
                    break;
                  }
                  case SEMICOLON: {
                    break;
                  }
                  default: {
                    throw ANTLR_USE_NAMESPACE(antlr)
                        NoViableAltException(LT(1), getFilename());
                  }
                }
              }
            } else if ((_tokenSet_72.member(LA(1))) &&
                       (_tokenSet_112.member(LA(2)))) {
              expression();
            } else if ((LA(1) == SEMICOLON)) {
            } else {
              throw ANTLR_USE_NAMESPACE(antlr)
                  NoViableAltException(LT(1), getFilename());
            }
          }
          match(SEMICOLON);
          if (inputState->guessing == 0) {
#line 1802 "CPP_parser.g"
            in_return = false, end_of_stmt();
#line 10220 "CPPParser.cpp"
          }
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_40);
    } else {
      throw;
    }
  }
}

void CPPParser::try_block() {
  try {  // for error handling
    match(LITERAL_try);
    compound_statement();
    {  // ( ... )*
      for (;;) {
        if ((LA(1) == LITERAL_catch)) {
          handler();
        } else {
          goto _loop432;
        }
      }
    _loop432:;
    }  // ( ... )*
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_40);
    } else {
      throw;
    }
  }
}

void CPPParser::throw_statement() {
  try {  // for error handling
    match(LITERAL_throw);
    {
      switch (LA(1)) {
        case ID:
        case StringLiteral:
        case LPAREN:
        case LITERAL_typename:
        case LITERAL_char:
        case LITERAL_wchar_t:
        case LITERAL_bool:
        case LITERAL_short:
        case LITERAL_int:
        case 50:
        case 51:
        case 52:
        case 53:
        case 54:
        case 55:
        case 56:
        case 57:
        case 58:
        case 59:
        case 60:
        case 61:
        case 62:
        case 63:
        case 64:
        case 65:
        case 66:
        case 67:
        case LITERAL_long:
        case LITERAL_signed:
        case LITERAL_unsigned:
        case LITERAL_float:
        case LITERAL_double:
        case LITERAL_void:
        case LITERAL__declspec:
        case LITERAL___declspec:
        case OPERATOR:
        case LITERAL_this:
        case LITERAL_true:
        case LITERAL_false:
        case OCTALINT:
        case TILDE:
        case STAR:
        case AMPERSAND:
        case PLUS:
        case MINUS:
        case PLUSPLUS:
        case MINUSMINUS:
        case LITERAL_sizeof:
        case LITERAL___alignof__:
        case SCOPE:
        case LITERAL_dynamic_cast:
        case LITERAL_static_cast:
        case LITERAL_reinterpret_cast:
        case LITERAL_const_cast:
        case LITERAL_typeid:
        case DECIMALINT:
        case HEXADECIMALINT:
        case CharLiteral:
        case WCharLiteral:
        case WStringLiteral:
        case FLOATONE:
        case FLOATTWO:
        case NOT:
        case LITERAL_new:
        case LITERAL_delete: {
          assignment_expression();
          break;
        }
        case SEMICOLON: {
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
    match(SEMICOLON);
    if (inputState->guessing == 0) {
#line 1833 "CPP_parser.g"
      end_of_stmt();
#line 10358 "CPPParser.cpp"
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_40);
    } else {
      throw;
    }
  }
}

void CPPParser::simple_declaration() {
  try {  // for error handling
    declaration_specifiers();
    {
      switch (LA(1)) {
        case ID:
        case LITERAL__stdcall:
        case LITERAL___stdcall:
        case LPAREN:
        case OPERATOR:
        case LITERAL_this:
        case LITERAL_true:
        case LITERAL_false:
        case TILDE:
        case STAR:
        case AMPERSAND:
        case SCOPE:
        case LITERAL__cdecl:
        case LITERAL___cdecl:
        case LITERAL__near:
        case LITERAL___near:
        case LITERAL__far:
        case LITERAL___far:
        case LITERAL___interrupt:
        case LITERAL_pascal:
        case LITERAL__pascal:
        case LITERAL___pascal: {
          init_declarator_list();
          break;
        }
        case SEMICOLON: {
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
    match(SEMICOLON);
    if (inputState->guessing == 0) {
#line 1725 "CPP_parser.g"
      end_of_stmt();
#line 10417 "CPPParser.cpp"
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_40);
    } else {
      throw;
    }
  }
}

void CPPParser::asm_definition() {
  try {  // for error handling
    {
      switch (LA(1)) {
        case LITERAL_asm: {
          match(LITERAL_asm);
          break;
        }
        case LITERAL__asm: {
          match(LITERAL__asm);
          break;
        }
        case LITERAL___asm: {
          match(LITERAL___asm);
          break;
        }
        case LITERAL___asm__: {
          match(LITERAL___asm__);
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
    match(LPAREN);
    match(StringLiteral);
    match(RPAREN);
    match(SEMICOLON);
    if (inputState->guessing == 0) {
#line 1848 "CPP_parser.g"
      end_of_stmt();
#line 10468 "CPPParser.cpp"
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_40);
    } else {
      throw;
    }
  }
}

void CPPParser::handler() {
  try {  // for error handling
    match(LITERAL_catch);
    if (inputState->guessing == 0) {
#line 1814 "CPP_parser.g"
      exceptionBeginHandler();
#line 10488 "CPPParser.cpp"
    }
    if (inputState->guessing == 0) {
#line 1815 "CPP_parser.g"
      declaratorParameterList(1);
#line 10493 "CPPParser.cpp"
    }
    match(LPAREN);
    exception_declaration();
    match(RPAREN);
    if (inputState->guessing == 0) {
#line 1817 "CPP_parser.g"
      declaratorEndParameterList(1);
#line 10501 "CPPParser.cpp"
    }
    compound_statement();
    if (inputState->guessing == 0) {
#line 1819 "CPP_parser.g"
      exceptionEndHandler();
#line 10507 "CPPParser.cpp"
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_114);
    } else {
      throw;
    }
  }
}

void CPPParser::exception_declaration() {
  try {  // for error handling
    parameter_declaration_list();
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_71);
    } else {
      throw;
    }
  }
}

void CPPParser::assignment_expression() {
  try {  // for error handling
    conditional_expression();
    {
      switch (LA(1)) {
        case ASSIGNEQUAL:
        case TIMESEQUAL:
        case DIVIDEEQUAL:
        case MINUSEQUAL:
        case PLUSEQUAL:
        case MODEQUAL:
        case SHIFTLEFTEQUAL:
        case SHIFTRIGHTEQUAL:
        case BITWISEANDEQUAL:
        case BITWISEXOREQUAL:
        case BITWISEOREQUAL: {
          {
            switch (LA(1)) {
              case ASSIGNEQUAL: {
                match(ASSIGNEQUAL);
                break;
              }
              case TIMESEQUAL: {
                match(TIMESEQUAL);
                break;
              }
              case DIVIDEEQUAL: {
                match(DIVIDEEQUAL);
                break;
              }
              case MINUSEQUAL: {
                match(MINUSEQUAL);
                break;
              }
              case PLUSEQUAL: {
                match(PLUSEQUAL);
                break;
              }
              case MODEQUAL: {
                match(MODEQUAL);
                break;
              }
              case SHIFTLEFTEQUAL: {
                match(SHIFTLEFTEQUAL);
                break;
              }
              case SHIFTRIGHTEQUAL: {
                match(SHIFTRIGHTEQUAL);
                break;
              }
              case BITWISEANDEQUAL: {
                match(BITWISEANDEQUAL);
                break;
              }
              case BITWISEXOREQUAL: {
                match(BITWISEXOREQUAL);
                break;
              }
              case BITWISEOREQUAL: {
                match(BITWISEOREQUAL);
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          remainder_expression();
          break;
        }
        case GREATERTHAN:
        case SEMICOLON:
        case RCURLY:
        case COLON:
        case RPAREN:
        case COMMA:
        case RSQUARE:
        case ELLIPSIS: {
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_74);
    } else {
      throw;
    }
  }
}

void CPPParser::conditional_expression() {
  try {  // for error handling
    logical_or_expression();
    {
      switch (LA(1)) {
        case QUESTIONMARK: {
          match(QUESTIONMARK);
          expression();
          match(COLON);
          conditional_expression();
          break;
        }
        case GREATERTHAN:
        case SEMICOLON:
        case RCURLY:
        case ASSIGNEQUAL:
        case COLON:
        case RPAREN:
        case COMMA:
        case RSQUARE:
        case ELLIPSIS:
        case TIMESEQUAL:
        case DIVIDEEQUAL:
        case MINUSEQUAL:
        case PLUSEQUAL:
        case MODEQUAL:
        case SHIFTLEFTEQUAL:
        case SHIFTRIGHTEQUAL:
        case BITWISEANDEQUAL:
        case BITWISEXOREQUAL:
        case BITWISEOREQUAL: {
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_115);
    } else {
      throw;
    }
  }
}

void CPPParser::logical_or_expression() {
  try {  // for error handling
    logical_and_expression();
    {  // ( ... )*
      for (;;) {
        if ((LA(1) == OR)) {
          match(OR);
          logical_and_expression();
        } else {
          goto _loop458;
        }
      }
    _loop458:;
    }  // ( ... )*
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_116);
    } else {
      throw;
    }
  }
}

void CPPParser::logical_and_expression() {
  try {  // for error handling
    inclusive_or_expression();
    {  // ( ... )*
      for (;;) {
        if ((LA(1) == AND)) {
          match(AND);
          inclusive_or_expression();
        } else {
          goto _loop461;
        }
      }
    _loop461:;
    }  // ( ... )*
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_117);
    } else {
      throw;
    }
  }
}

void CPPParser::inclusive_or_expression() {
  try {  // for error handling
    exclusive_or_expression();
    {  // ( ... )*
      for (;;) {
        if ((LA(1) == BITWISEOR)) {
          match(BITWISEOR);
          exclusive_or_expression();
        } else {
          goto _loop464;
        }
      }
    _loop464:;
    }  // ( ... )*
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_118);
    } else {
      throw;
    }
  }
}

void CPPParser::exclusive_or_expression() {
  try {  // for error handling
    and_expression();
    {  // ( ... )*
      for (;;) {
        if ((LA(1) == BITWISEXOR)) {
          match(BITWISEXOR);
          and_expression();
        } else {
          goto _loop467;
        }
      }
    _loop467:;
    }  // ( ... )*
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_119);
    } else {
      throw;
    }
  }
}

void CPPParser::and_expression() {
  try {  // for error handling
    equality_expression();
    {  // ( ... )*
      for (;;) {
        if ((LA(1) == AMPERSAND)) {
          match(AMPERSAND);
          equality_expression();
        } else {
          goto _loop470;
        }
      }
    _loop470:;
    }  // ( ... )*
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_120);
    } else {
      throw;
    }
  }
}

void CPPParser::equality_expression() {
  try {  // for error handling
    relational_expression();
    {  // ( ... )*
      for (;;) {
        if ((LA(1) == NOTEQUAL || LA(1) == EQUAL)) {
          {
            switch (LA(1)) {
              case NOTEQUAL: {
                match(NOTEQUAL);
                break;
              }
              case EQUAL: {
                match(EQUAL);
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          relational_expression();
        } else {
          goto _loop474;
        }
      }
    _loop474:;
    }  // ( ... )*
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_121);
    } else {
      throw;
    }
  }
}

void CPPParser::relational_expression() {
  try {  // for error handling
    shift_expression();
    {  // ( ... )*
      for (;;) {
        if ((_tokenSet_122.member(LA(1))) && (_tokenSet_72.member(LA(2)))) {
          {
            switch (LA(1)) {
              case LESSTHAN: {
                match(LESSTHAN);
                break;
              }
              case GREATERTHAN: {
                match(GREATERTHAN);
                break;
              }
              case LESSTHANOREQUALTO: {
                match(LESSTHANOREQUALTO);
                break;
              }
              case GREATERTHANOREQUALTO: {
                match(GREATERTHANOREQUALTO);
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          shift_expression();
        } else {
          goto _loop478;
        }
      }
    _loop478:;
    }  // ( ... )*
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_123);
    } else {
      throw;
    }
  }
}

void CPPParser::additive_expression() {
  try {  // for error handling
    multiplicative_expression();
    {  // ( ... )*
      for (;;) {
        if ((LA(1) == PLUS || LA(1) == MINUS)) {
          {
            switch (LA(1)) {
              case PLUS: {
                match(PLUS);
                break;
              }
              case MINUS: {
                match(MINUS);
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          multiplicative_expression();
        } else {
          goto _loop486;
        }
      }
    _loop486:;
    }  // ( ... )*
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_124);
    } else {
      throw;
    }
  }
}

void CPPParser::multiplicative_expression() {
  try {  // for error handling
    pm_expression();
    {  // ( ... )*
      for (;;) {
        if ((_tokenSet_125.member(LA(1)))) {
          {
            switch (LA(1)) {
              case STAR: {
                match(STAR);
                break;
              }
              case DIVIDE: {
                match(DIVIDE);
                break;
              }
              case MOD: {
                match(MOD);
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          pm_expression();
        } else {
          goto _loop490;
        }
      }
    _loop490:;
    }  // ( ... )*
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_126);
    } else {
      throw;
    }
  }
}

void CPPParser::pm_expression() {
  try {  // for error handling
    cast_expression();
    {  // ( ... )*
      for (;;) {
        if ((LA(1) == DOTMBR || LA(1) == POINTERTOMBR)) {
          {
            switch (LA(1)) {
              case DOTMBR: {
                match(DOTMBR);
                break;
              }
              case POINTERTOMBR: {
                match(POINTERTOMBR);
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          cast_expression();
        } else {
          goto _loop494;
        }
      }
    _loop494:;
    }  // ( ... )*
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_127);
    } else {
      throw;
    }
  }
}

void CPPParser::cast_expression() {
#line 2021 "CPP_parser.g"
  TypeQualifier tq;
  TypeSpecifier ts;
#line 11084 "CPPParser.cpp"

  try {  // for error handling
    bool synPredMatched501 = false;
    if (((LA(1) == LPAREN) && (_tokenSet_128.member(LA(2))))) {
      int _m501 = mark();
      synPredMatched501 = true;
      inputState->guessing++;
      try {
        {
          match(LPAREN);
          {  // ( ... )*
            for (;;) {
              if (((LA(1) >= LITERAL_const && LA(1) <= LITERAL___volatile__))) {
                type_qualifier();
              } else {
                goto _loop498;
              }
            }
          _loop498:;
          }  // ( ... )*
          simple_type_specifier();
          {  // ( ... )*
            for (;;) {
              if ((_tokenSet_25.member(LA(1)))) {
                ptr_operator();
              } else {
                goto _loop500;
              }
            }
          _loop500:;
          }  // ( ... )*
          match(RPAREN);
          unary_expression();
        }
      } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
        synPredMatched501 = false;
      }
      rewind(_m501);
      inputState->guessing--;
    }
    if (synPredMatched501) {
      match(LPAREN);
      {  // ( ... )*
        for (;;) {
          if (((LA(1) >= LITERAL_const && LA(1) <= LITERAL___volatile__))) {
            tq = type_qualifier();
          } else {
            goto _loop503;
          }
        }
      _loop503:;
      }  // ( ... )*
      ts = simple_type_specifier();
      {  // ( ... )*
        for (;;) {
          if ((_tokenSet_25.member(LA(1)))) {
            ptr_operator();
          } else {
            goto _loop505;
          }
        }
      _loop505:;
      }  // ( ... )*
      match(RPAREN);
      unary_expression();
    } else {
      bool synPredMatched511 = false;
      if (((LA(1) == LPAREN) && (_tokenSet_128.member(LA(2))))) {
        int _m511 = mark();
        synPredMatched511 = true;
        inputState->guessing++;
        try {
          {
            match(LPAREN);
            {  // ( ... )*
              for (;;) {
                if (((LA(1) >= LITERAL_const &&
                      LA(1) <= LITERAL___volatile__))) {
                  type_qualifier();
                } else {
                  goto _loop508;
                }
              }
            _loop508:;
            }  // ( ... )*
            simple_type_specifier();
            {  // ( ... )*
              for (;;) {
                if ((_tokenSet_25.member(LA(1)))) {
                  ptr_operator();
                } else {
                  goto _loop510;
                }
              }
            _loop510:;
            }  // ( ... )*
            match(RPAREN);
          }
        } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
          synPredMatched511 = false;
        }
        rewind(_m511);
        inputState->guessing--;
      }
      if (synPredMatched511) {
        match(LPAREN);
        {  // ( ... )*
          for (;;) {
            if (((LA(1) >= LITERAL_const && LA(1) <= LITERAL___volatile__))) {
              tq = type_qualifier();
            } else {
              goto _loop513;
            }
          }
        _loop513:;
        }  // ( ... )*
        ts = simple_type_specifier();
        {  // ( ... )*
          for (;;) {
            if ((_tokenSet_25.member(LA(1)))) {
              ptr_operator();
            } else {
              goto _loop515;
            }
          }
        _loop515:;
        }  // ( ... )*
        match(RPAREN);
        cast_expression();
      } else if ((_tokenSet_72.member(LA(1))) && (_tokenSet_73.member(LA(2)))) {
        unary_expression();
      } else {
        throw ANTLR_USE_NAMESPACE(antlr)
            NoViableAltException(LT(1), getFilename());
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_129);
    } else {
      throw;
    }
  }
}

void CPPParser::unary_expression() {
  try {  // for error handling
    {
      switch (LA(1)) {
        case PLUSPLUS: {
          match(PLUSPLUS);
          unary_expression();
          break;
        }
        case MINUSMINUS: {
          match(MINUSMINUS);
          unary_expression();
          break;
        }
        case LITERAL_sizeof:
        case LITERAL___alignof__: {
          {
            switch (LA(1)) {
              case LITERAL_sizeof: {
                match(LITERAL_sizeof);
                break;
              }
              case LITERAL___alignof__: {
                match(LITERAL___alignof__);
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          {
            bool synPredMatched523 = false;
            if (((_tokenSet_72.member(LA(1))) &&
                 (_tokenSet_73.member(LA(2))))) {
              int _m523 = mark();
              synPredMatched523 = true;
              inputState->guessing++;
              try {
                { unary_expression(); }
              } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
                synPredMatched523 = false;
              }
              rewind(_m523);
              inputState->guessing--;
            }
            if (synPredMatched523) {
              unary_expression();
            } else if ((LA(1) == LPAREN) && (_tokenSet_12.member(LA(2)))) {
              match(LPAREN);
              type_id();
              match(RPAREN);
            } else {
              throw ANTLR_USE_NAMESPACE(antlr)
                  NoViableAltException(LT(1), getFilename());
            }
          }
          break;
        }
        default:
          bool synPredMatched519 = false;
          if (((_tokenSet_130.member(LA(1))) &&
               (_tokenSet_131.member(LA(2))))) {
            int _m519 = mark();
            synPredMatched519 = true;
            inputState->guessing++;
            try {
              { postfix_expression(); }
            } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
              synPredMatched519 = false;
            }
            rewind(_m519);
            inputState->guessing--;
          }
          if (synPredMatched519) {
            postfix_expression();
          } else if ((_tokenSet_132.member(LA(1))) &&
                     (_tokenSet_72.member(LA(2)))) {
            unary_operator();
            cast_expression();
          } else if ((_tokenSet_133.member(LA(1))) &&
                     (_tokenSet_134.member(LA(2)))) {
            {
              switch (LA(1)) {
                case SCOPE: {
                  match(SCOPE);
                  break;
                }
                case LITERAL_new:
                case LITERAL_delete: {
                  break;
                }
                default: {
                  throw ANTLR_USE_NAMESPACE(antlr)
                      NoViableAltException(LT(1), getFilename());
                }
              }
            }
            {
              switch (LA(1)) {
                case LITERAL_new: {
                  new_expression();
                  break;
                }
                case LITERAL_delete: {
                  delete_expression();
                  break;
                }
                default: {
                  throw ANTLR_USE_NAMESPACE(antlr)
                      NoViableAltException(LT(1), getFilename());
                }
              }
            }
          } else {
            throw ANTLR_USE_NAMESPACE(antlr)
                NoViableAltException(LT(1), getFilename());
          }
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_129);
    } else {
      throw;
    }
  }
}

void CPPParser::postfix_expression() {
#line 2063 "CPP_parser.g"

  TypeSpecifier ts;

#line 11404 "CPPParser.cpp"

  try {  // for error handling
    {
      switch (LA(1)) {
        case LITERAL_dynamic_cast:
        case LITERAL_static_cast:
        case LITERAL_reinterpret_cast:
        case LITERAL_const_cast: {
          {
            switch (LA(1)) {
              case LITERAL_dynamic_cast: {
                match(LITERAL_dynamic_cast);
                break;
              }
              case LITERAL_static_cast: {
                match(LITERAL_static_cast);
                break;
              }
              case LITERAL_reinterpret_cast: {
                match(LITERAL_reinterpret_cast);
                break;
              }
              case LITERAL_const_cast: {
                match(LITERAL_const_cast);
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          match(LESSTHAN);
          {
            switch (LA(1)) {
              case LITERAL_const: {
                match(LITERAL_const);
                break;
              }
              case LITERAL___const: {
                match(LITERAL___const);
                break;
              }
              case ID:
              case LITERAL_typename:
              case LITERAL_char:
              case LITERAL_wchar_t:
              case LITERAL_bool:
              case LITERAL_short:
              case LITERAL_int:
              case 50:
              case 51:
              case 52:
              case 53:
              case 54:
              case 55:
              case 56:
              case 57:
              case 58:
              case 59:
              case 60:
              case 61:
              case 62:
              case 63:
              case 64:
              case 65:
              case 66:
              case 67:
              case LITERAL_long:
              case LITERAL_signed:
              case LITERAL_unsigned:
              case LITERAL_float:
              case LITERAL_double:
              case LITERAL_void:
              case LITERAL__declspec:
              case LITERAL___declspec:
              case SCOPE: {
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          ts = type_specifier();
          {
            switch (LA(1)) {
              case ID:
              case LITERAL__stdcall:
              case LITERAL___stdcall:
              case STAR:
              case AMPERSAND:
              case SCOPE:
              case LITERAL__cdecl:
              case LITERAL___cdecl:
              case LITERAL__near:
              case LITERAL___near:
              case LITERAL__far:
              case LITERAL___far:
              case LITERAL___interrupt:
              case LITERAL_pascal:
              case LITERAL__pascal:
              case LITERAL___pascal: {
                ptr_operator();
                break;
              }
              case GREATERTHAN: {
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          match(GREATERTHAN);
          match(LPAREN);
          expression();
          match(RPAREN);
          break;
        }
        case LITERAL_typeid: {
          match(LITERAL_typeid);
          match(LPAREN);
          {
            bool synPredMatched544 = false;
            if (((_tokenSet_12.member(LA(1))) &&
                 (_tokenSet_135.member(LA(2))))) {
              int _m544 = mark();
              synPredMatched544 = true;
              inputState->guessing++;
              try {
                { type_id(); }
              } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
                synPredMatched544 = false;
              }
              rewind(_m544);
              inputState->guessing--;
            }
            if (synPredMatched544) {
              type_id();
            } else if ((_tokenSet_72.member(LA(1))) &&
                       (_tokenSet_136.member(LA(2)))) {
              expression();
            } else {
              throw ANTLR_USE_NAMESPACE(antlr)
                  NoViableAltException(LT(1), getFilename());
            }
          }
          match(RPAREN);
          break;
        }
        default:
          bool synPredMatched529 = false;
          if ((((_tokenSet_103.member(LA(1))) &&
                (_tokenSet_137.member(LA(2)))) &&
               (!(LA(1) == LPAREN)))) {
            int _m529 = mark();
            synPredMatched529 = true;
            inputState->guessing++;
            try {
              {
                ts = simple_type_specifier();
                match(LPAREN);
                match(RPAREN);
                match(LPAREN);
              }
            } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
              synPredMatched529 = false;
            }
            rewind(_m529);
            inputState->guessing--;
          }
          if (synPredMatched529) {
            ts = simple_type_specifier();
            match(LPAREN);
            match(RPAREN);
            match(LPAREN);
            {
              switch (LA(1)) {
                case ID:
                case StringLiteral:
                case LPAREN:
                case LITERAL_typename:
                case LITERAL_char:
                case LITERAL_wchar_t:
                case LITERAL_bool:
                case LITERAL_short:
                case LITERAL_int:
                case 50:
                case 51:
                case 52:
                case 53:
                case 54:
                case 55:
                case 56:
                case 57:
                case 58:
                case 59:
                case 60:
                case 61:
                case 62:
                case 63:
                case 64:
                case 65:
                case 66:
                case 67:
                case LITERAL_long:
                case LITERAL_signed:
                case LITERAL_unsigned:
                case LITERAL_float:
                case LITERAL_double:
                case LITERAL_void:
                case LITERAL__declspec:
                case LITERAL___declspec:
                case OPERATOR:
                case LITERAL_this:
                case LITERAL_true:
                case LITERAL_false:
                case OCTALINT:
                case TILDE:
                case STAR:
                case AMPERSAND:
                case PLUS:
                case MINUS:
                case PLUSPLUS:
                case MINUSMINUS:
                case LITERAL_sizeof:
                case LITERAL___alignof__:
                case SCOPE:
                case LITERAL_dynamic_cast:
                case LITERAL_static_cast:
                case LITERAL_reinterpret_cast:
                case LITERAL_const_cast:
                case LITERAL_typeid:
                case DECIMALINT:
                case HEXADECIMALINT:
                case CharLiteral:
                case WCharLiteral:
                case WStringLiteral:
                case FLOATONE:
                case FLOATTWO:
                case NOT:
                case LITERAL_new:
                case LITERAL_delete: {
                  expression_list();
                  break;
                }
                case RPAREN: {
                  break;
                }
                default: {
                  throw ANTLR_USE_NAMESPACE(antlr)
                      NoViableAltException(LT(1), getFilename());
                }
              }
            }
            match(RPAREN);
          } else {
            bool synPredMatched532 = false;
            if ((((_tokenSet_103.member(LA(1))) &&
                  (_tokenSet_137.member(LA(2)))) &&
                 (!(LA(1) == LPAREN)))) {
              int _m532 = mark();
              synPredMatched532 = true;
              inputState->guessing++;
              try {
                {
                  ts = simple_type_specifier();
                  match(LPAREN);
                }
              } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
                synPredMatched532 = false;
              }
              rewind(_m532);
              inputState->guessing--;
            }
            if (synPredMatched532) {
              ts = simple_type_specifier();
              match(LPAREN);
              {
                switch (LA(1)) {
                  case ID:
                  case StringLiteral:
                  case LPAREN:
                  case LITERAL_typename:
                  case LITERAL_char:
                  case LITERAL_wchar_t:
                  case LITERAL_bool:
                  case LITERAL_short:
                  case LITERAL_int:
                  case 50:
                  case 51:
                  case 52:
                  case 53:
                  case 54:
                  case 55:
                  case 56:
                  case 57:
                  case 58:
                  case 59:
                  case 60:
                  case 61:
                  case 62:
                  case 63:
                  case 64:
                  case 65:
                  case 66:
                  case 67:
                  case LITERAL_long:
                  case LITERAL_signed:
                  case LITERAL_unsigned:
                  case LITERAL_float:
                  case LITERAL_double:
                  case LITERAL_void:
                  case LITERAL__declspec:
                  case LITERAL___declspec:
                  case OPERATOR:
                  case LITERAL_this:
                  case LITERAL_true:
                  case LITERAL_false:
                  case OCTALINT:
                  case TILDE:
                  case STAR:
                  case AMPERSAND:
                  case PLUS:
                  case MINUS:
                  case PLUSPLUS:
                  case MINUSMINUS:
                  case LITERAL_sizeof:
                  case LITERAL___alignof__:
                  case SCOPE:
                  case LITERAL_dynamic_cast:
                  case LITERAL_static_cast:
                  case LITERAL_reinterpret_cast:
                  case LITERAL_const_cast:
                  case LITERAL_typeid:
                  case DECIMALINT:
                  case HEXADECIMALINT:
                  case CharLiteral:
                  case WCharLiteral:
                  case WStringLiteral:
                  case FLOATONE:
                  case FLOATTWO:
                  case NOT:
                  case LITERAL_new:
                  case LITERAL_delete: {
                    expression_list();
                    break;
                  }
                  case RPAREN: {
                    break;
                  }
                  default: {
                    throw ANTLR_USE_NAMESPACE(antlr)
                        NoViableAltException(LT(1), getFilename());
                  }
                }
              }
              match(RPAREN);
            } else if ((_tokenSet_138.member(LA(1))) &&
                       (_tokenSet_131.member(LA(2)))) {
              primary_expression();
              {  // ( ... )*
                for (;;) {
                  switch (LA(1)) {
                    case LSQUARE: {
                      match(LSQUARE);
                      expression();
                      match(RSQUARE);
                      break;
                    }
                    case LPAREN: {
                      match(LPAREN);
                      {
                        switch (LA(1)) {
                          case ID:
                          case StringLiteral:
                          case LPAREN:
                          case LITERAL_typename:
                          case LITERAL_char:
                          case LITERAL_wchar_t:
                          case LITERAL_bool:
                          case LITERAL_short:
                          case LITERAL_int:
                          case 50:
                          case 51:
                          case 52:
                          case 53:
                          case 54:
                          case 55:
                          case 56:
                          case 57:
                          case 58:
                          case 59:
                          case 60:
                          case 61:
                          case 62:
                          case 63:
                          case 64:
                          case 65:
                          case 66:
                          case 67:
                          case LITERAL_long:
                          case LITERAL_signed:
                          case LITERAL_unsigned:
                          case LITERAL_float:
                          case LITERAL_double:
                          case LITERAL_void:
                          case LITERAL__declspec:
                          case LITERAL___declspec:
                          case OPERATOR:
                          case LITERAL_this:
                          case LITERAL_true:
                          case LITERAL_false:
                          case OCTALINT:
                          case TILDE:
                          case STAR:
                          case AMPERSAND:
                          case PLUS:
                          case MINUS:
                          case PLUSPLUS:
                          case MINUSMINUS:
                          case LITERAL_sizeof:
                          case LITERAL___alignof__:
                          case SCOPE:
                          case LITERAL_dynamic_cast:
                          case LITERAL_static_cast:
                          case LITERAL_reinterpret_cast:
                          case LITERAL_const_cast:
                          case LITERAL_typeid:
                          case DECIMALINT:
                          case HEXADECIMALINT:
                          case CharLiteral:
                          case WCharLiteral:
                          case WStringLiteral:
                          case FLOATONE:
                          case FLOATTWO:
                          case NOT:
                          case LITERAL_new:
                          case LITERAL_delete: {
                            expression_list();
                            break;
                          }
                          case RPAREN: {
                            break;
                          }
                          default: {
                            throw ANTLR_USE_NAMESPACE(antlr)
                                NoViableAltException(LT(1), getFilename());
                          }
                        }
                      }
                      match(RPAREN);
                      break;
                    }
                    case DOT:
                    case POINTERTO: {
                      {
                        switch (LA(1)) {
                          case DOT: {
                            match(DOT);
                            break;
                          }
                          case POINTERTO: {
                            match(POINTERTO);
                            break;
                          }
                          default: {
                            throw ANTLR_USE_NAMESPACE(antlr)
                                NoViableAltException(LT(1), getFilename());
                          }
                        }
                      }
                      {
                        switch (LA(1)) {
                          case LITERAL_template: {
                            match(LITERAL_template);
                            break;
                          }
                          case ID:
                          case OPERATOR:
                          case TILDE:
                          case SCOPE: {
                            break;
                          }
                          default: {
                            throw ANTLR_USE_NAMESPACE(antlr)
                                NoViableAltException(LT(1), getFilename());
                          }
                        }
                      }
                      id_expression();
                      break;
                    }
                    case PLUSPLUS: {
                      match(PLUSPLUS);
                      break;
                    }
                    case MINUSMINUS: {
                      match(MINUSMINUS);
                      break;
                    }
                    default: { goto _loop538; }
                  }
                }
              _loop538:;
              }  // ( ... )*
            } else {
              throw ANTLR_USE_NAMESPACE(antlr)
                  NoViableAltException(LT(1), getFilename());
            }
          }
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_129);
    } else {
      throw;
    }
  }
}

void CPPParser::unary_operator() {
  try {  // for error handling
    switch (LA(1)) {
      case AMPERSAND: {
        match(AMPERSAND);
        break;
      }
      case STAR: {
        match(STAR);
        break;
      }
      case PLUS: {
        match(PLUS);
        break;
      }
      case MINUS: {
        match(MINUS);
        break;
      }
      case TILDE: {
        match(TILDE);
        break;
      }
      case NOT: {
        match(NOT);
        break;
      }
      default: {
        throw ANTLR_USE_NAMESPACE(antlr)
            NoViableAltException(LT(1), getFilename());
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_72);
    } else {
      throw;
    }
  }
}

void CPPParser::new_expression() {
  try {  // for error handling
    {
      match(LITERAL_new);
      {
        bool synPredMatched557 = false;
        if (((LA(1) == LPAREN) && (_tokenSet_72.member(LA(2))))) {
          int _m557 = mark();
          synPredMatched557 = true;
          inputState->guessing++;
          try {
            {
              match(LPAREN);
              expression_list();
              match(RPAREN);
            }
          } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& pe) {
            synPredMatched557 = false;
          }
          rewind(_m557);
          inputState->guessing--;
        }
        if (synPredMatched557) {
          match(LPAREN);
          expression_list();
          match(RPAREN);
        } else if ((_tokenSet_139.member(LA(1))) &&
                   (_tokenSet_140.member(LA(2)))) {
        } else {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
      {
        switch (LA(1)) {
          case LITERAL_typedef:
          case LITERAL_enum:
          case ID:
          case LITERAL_inline:
          case LITERAL_friend:
          case LITERAL_extern:
          case LITERAL_struct:
          case LITERAL_union:
          case LITERAL_class:
          case LITERAL__stdcall:
          case LITERAL___stdcall:
          case LITERAL_GFEXCLUDE:
          case LITERAL_GFINCLUDE:
          case LITERAL_GFID:
          case LITERAL_GFUNREAD:
          case LITERAL_GFARRAYSIZE:
          case LITERAL_GFARRAYSIZES:
          case LITERAL_GFARRAYELEMSIZE:
          case LITERAL_auto:
          case LITERAL_register:
          case LITERAL_static:
          case LITERAL_mutable:
          case LITERAL__inline:
          case LITERAL___inline:
          case LITERAL_virtual:
          case LITERAL_explicit:
          case LITERAL_typename:
          case LITERAL_char:
          case LITERAL_wchar_t:
          case LITERAL_bool:
          case LITERAL_short:
          case LITERAL_int:
          case 50:
          case 51:
          case 52:
          case 53:
          case 54:
          case 55:
          case 56:
          case 57:
          case 58:
          case 59:
          case 60:
          case 61:
          case 62:
          case 63:
          case 64:
          case 65:
          case 66:
          case 67:
          case LITERAL_long:
          case LITERAL_signed:
          case LITERAL_unsigned:
          case LITERAL_float:
          case LITERAL_double:
          case LITERAL_void:
          case LITERAL__declspec:
          case LITERAL___declspec:
          case LITERAL_const:
          case LITERAL___const:
          case LITERAL_volatile:
          case LITERAL___volatile__:
          case SCOPE: {
            new_type_id();
            break;
          }
          case LPAREN: {
            match(LPAREN);
            type_id();
            match(RPAREN);
            break;
          }
          default: {
            throw ANTLR_USE_NAMESPACE(antlr)
                NoViableAltException(LT(1), getFilename());
          }
        }
      }
      {
        switch (LA(1)) {
          case LPAREN: {
            new_initializer();
            break;
          }
          case LESSTHAN:
          case GREATERTHAN:
          case SEMICOLON:
          case RCURLY:
          case ASSIGNEQUAL:
          case COLON:
          case RPAREN:
          case COMMA:
          case RSQUARE:
          case STAR:
          case AMPERSAND:
          case ELLIPSIS:
          case TIMESEQUAL:
          case DIVIDEEQUAL:
          case MINUSEQUAL:
          case PLUSEQUAL:
          case MODEQUAL:
          case SHIFTLEFTEQUAL:
          case SHIFTRIGHTEQUAL:
          case BITWISEANDEQUAL:
          case BITWISEXOREQUAL:
          case BITWISEOREQUAL:
          case QUESTIONMARK:
          case OR:
          case AND:
          case BITWISEOR:
          case BITWISEXOR:
          case NOTEQUAL:
          case EQUAL:
          case LESSTHANOREQUALTO:
          case GREATERTHANOREQUALTO:
          case SHIFTLEFT:
          case SHIFTRIGHT:
          case PLUS:
          case MINUS:
          case DIVIDE:
          case MOD:
          case DOTMBR:
          case POINTERTOMBR: {
            break;
          }
          default: {
            throw ANTLR_USE_NAMESPACE(antlr)
                NoViableAltException(LT(1), getFilename());
          }
        }
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_129);
    } else {
      throw;
    }
  }
}

void CPPParser::delete_expression() {
  try {  // for error handling
    match(LITERAL_delete);
    {
      switch (LA(1)) {
        case LSQUARE: {
          match(LSQUARE);
          match(RSQUARE);
          break;
        }
        case ID:
        case StringLiteral:
        case LPAREN:
        case LITERAL_typename:
        case LITERAL_char:
        case LITERAL_wchar_t:
        case LITERAL_bool:
        case LITERAL_short:
        case LITERAL_int:
        case 50:
        case 51:
        case 52:
        case 53:
        case 54:
        case 55:
        case 56:
        case 57:
        case 58:
        case 59:
        case 60:
        case 61:
        case 62:
        case 63:
        case 64:
        case 65:
        case 66:
        case 67:
        case LITERAL_long:
        case LITERAL_signed:
        case LITERAL_unsigned:
        case LITERAL_float:
        case LITERAL_double:
        case LITERAL_void:
        case LITERAL__declspec:
        case LITERAL___declspec:
        case OPERATOR:
        case LITERAL_this:
        case LITERAL_true:
        case LITERAL_false:
        case OCTALINT:
        case TILDE:
        case STAR:
        case AMPERSAND:
        case PLUS:
        case MINUS:
        case PLUSPLUS:
        case MINUSMINUS:
        case LITERAL_sizeof:
        case LITERAL___alignof__:
        case SCOPE:
        case LITERAL_dynamic_cast:
        case LITERAL_static_cast:
        case LITERAL_reinterpret_cast:
        case LITERAL_const_cast:
        case LITERAL_typeid:
        case DECIMALINT:
        case HEXADECIMALINT:
        case CharLiteral:
        case WCharLiteral:
        case WStringLiteral:
        case FLOATONE:
        case FLOATTWO:
        case NOT:
        case LITERAL_new:
        case LITERAL_delete: {
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
    cast_expression();
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_129);
    } else {
      throw;
    }
  }
}

void CPPParser::primary_expression() {
  try {  // for error handling
    switch (LA(1)) {
      case ID:
      case OPERATOR:
      case TILDE:
      case SCOPE: {
        id_expression();
        break;
      }
      case StringLiteral:
      case LITERAL_true:
      case LITERAL_false:
      case OCTALINT:
      case DECIMALINT:
      case HEXADECIMALINT:
      case CharLiteral:
      case WCharLiteral:
      case WStringLiteral:
      case FLOATONE:
      case FLOATTWO: {
        constant();
        break;
      }
      case LITERAL_this: {
        match(LITERAL_this);
        break;
      }
      case LPAREN: {
        match(LPAREN);
        expression();
        match(RPAREN);
        break;
      }
      default: {
        throw ANTLR_USE_NAMESPACE(antlr)
            NoViableAltException(LT(1), getFilename());
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_141);
    } else {
      throw;
    }
  }
}

void CPPParser::id_expression() {
#line 2105 "CPP_parser.g"
  char* s;
#line 12353 "CPPParser.cpp"

  try {  // for error handling
    s = scope_override();
    {
      switch (LA(1)) {
        case ID: {
          match(ID);
          break;
        }
        case OPERATOR: {
          match(OPERATOR);
          optor();
          break;
        }
        case TILDE: {
          match(TILDE);
          {
            switch (LA(1)) {
              case STAR: {
                match(STAR);
                break;
              }
              case ID: {
                break;
              }
              default: {
                throw ANTLR_USE_NAMESPACE(antlr)
                    NoViableAltException(LT(1), getFilename());
              }
            }
          }
          match(ID);
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_141);
    } else {
      throw;
    }
  }
}

void CPPParser::constant() {
  try {  // for error handling
    switch (LA(1)) {
      case OCTALINT: {
        match(OCTALINT);
        break;
      }
      case DECIMALINT: {
        match(DECIMALINT);
        break;
      }
      case HEXADECIMALINT: {
        match(HEXADECIMALINT);
        break;
      }
      case CharLiteral: {
        match(CharLiteral);
        break;
      }
      case WCharLiteral: {
        match(WCharLiteral);
        break;
      }
      case StringLiteral:
      case WStringLiteral: {
        {  // ( ... )+
          int _cnt551 = 0;
          for (;;) {
            switch (LA(1)) {
              case StringLiteral: {
                match(StringLiteral);
                break;
              }
              case WStringLiteral: {
                match(WStringLiteral);
                break;
              }
              default: {
                if (_cnt551 >= 1) {
                  goto _loop551;
                } else {
                  throw ANTLR_USE_NAMESPACE(antlr)
                      NoViableAltException(LT(1), getFilename());
                }
              }
            }
            _cnt551++;
          }
        _loop551:;
        }  // ( ... )+
        break;
      }
      case FLOATONE: {
        match(FLOATONE);
        break;
      }
      case FLOATTWO: {
        match(FLOATTWO);
        break;
      }
      case LITERAL_true: {
        match(LITERAL_true);
        break;
      }
      case LITERAL_false: {
        match(LITERAL_false);
        break;
      }
      default: {
        throw ANTLR_USE_NAMESPACE(antlr)
            NoViableAltException(LT(1), getFilename());
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_141);
    } else {
      throw;
    }
  }
}

void CPPParser::new_type_id() {
  try {  // for error handling
    declaration_specifiers();
    {
      if ((_tokenSet_142.member(LA(1))) && (_tokenSet_143.member(LA(2)))) {
        new_declarator();
      } else if ((_tokenSet_144.member(LA(1))) &&
                 (_tokenSet_57.member(LA(2)))) {
      } else {
        throw ANTLR_USE_NAMESPACE(antlr)
            NoViableAltException(LT(1), getFilename());
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_144);
    } else {
      throw;
    }
  }
}

void CPPParser::new_initializer() {
  try {  // for error handling
    match(LPAREN);
    {
      switch (LA(1)) {
        case ID:
        case StringLiteral:
        case LPAREN:
        case LITERAL_typename:
        case LITERAL_char:
        case LITERAL_wchar_t:
        case LITERAL_bool:
        case LITERAL_short:
        case LITERAL_int:
        case 50:
        case 51:
        case 52:
        case 53:
        case 54:
        case 55:
        case 56:
        case 57:
        case 58:
        case 59:
        case 60:
        case 61:
        case 62:
        case 63:
        case 64:
        case 65:
        case 66:
        case 67:
        case LITERAL_long:
        case LITERAL_signed:
        case LITERAL_unsigned:
        case LITERAL_float:
        case LITERAL_double:
        case LITERAL_void:
        case LITERAL__declspec:
        case LITERAL___declspec:
        case OPERATOR:
        case LITERAL_this:
        case LITERAL_true:
        case LITERAL_false:
        case OCTALINT:
        case TILDE:
        case STAR:
        case AMPERSAND:
        case PLUS:
        case MINUS:
        case PLUSPLUS:
        case MINUSMINUS:
        case LITERAL_sizeof:
        case LITERAL___alignof__:
        case SCOPE:
        case LITERAL_dynamic_cast:
        case LITERAL_static_cast:
        case LITERAL_reinterpret_cast:
        case LITERAL_const_cast:
        case LITERAL_typeid:
        case DECIMALINT:
        case HEXADECIMALINT:
        case CharLiteral:
        case WCharLiteral:
        case WStringLiteral:
        case FLOATONE:
        case FLOATTWO:
        case NOT:
        case LITERAL_new:
        case LITERAL_delete: {
          expression_list();
          break;
        }
        case RPAREN: {
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
    match(RPAREN);
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_129);
    } else {
      throw;
    }
  }
}

void CPPParser::new_declarator() {
  try {  // for error handling
    switch (LA(1)) {
      case ID:
      case LITERAL__stdcall:
      case LITERAL___stdcall:
      case STAR:
      case AMPERSAND:
      case SCOPE:
      case LITERAL__cdecl:
      case LITERAL___cdecl:
      case LITERAL__near:
      case LITERAL___near:
      case LITERAL__far:
      case LITERAL___far:
      case LITERAL___interrupt:
      case LITERAL_pascal:
      case LITERAL__pascal:
      case LITERAL___pascal: {
        ptr_operator();
        {
          if ((_tokenSet_142.member(LA(1))) && (_tokenSet_143.member(LA(2)))) {
            new_declarator();
          } else if ((_tokenSet_144.member(LA(1))) &&
                     (_tokenSet_57.member(LA(2)))) {
          } else {
            throw ANTLR_USE_NAMESPACE(antlr)
                NoViableAltException(LT(1), getFilename());
          }
        }
        break;
      }
      case LSQUARE: {
        direct_new_declarator();
        break;
      }
      default: {
        throw ANTLR_USE_NAMESPACE(antlr)
            NoViableAltException(LT(1), getFilename());
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_144);
    } else {
      throw;
    }
  }
}

void CPPParser::direct_new_declarator() {
  try {  // for error handling
    {    // ( ... )+
      int _cnt570 = 0;
      for (;;) {
        if ((LA(1) == LSQUARE)) {
          match(LSQUARE);
          expression();
          match(RSQUARE);
        } else {
          if (_cnt570 >= 1) {
            goto _loop570;
          } else {
            throw ANTLR_USE_NAMESPACE(antlr)
                NoViableAltException(LT(1), getFilename());
          }
        }

        _cnt570++;
      }
    _loop570:;
    }  // ( ... )+
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_144);
    } else {
      throw;
    }
  }
}

void CPPParser::ptr_to_member() {
#line 2211 "CPP_parser.g"
  char* s;
#line 12715 "CPPParser.cpp"

  try {  // for error handling
    s = scope_override();
    match(STAR);
    if (inputState->guessing == 0) {
#line 2214 "CPP_parser.g"
      is_pointer = true;
#line 12723 "CPPParser.cpp"
    }
    cv_qualifier_seq();
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_24);
    } else {
      throw;
    }
  }
}

void CPPParser::optor_simple_tokclass() {
  try {  // for error handling
    {
      switch (LA(1)) {
        case PLUS: {
          match(PLUS);
          break;
        }
        case MINUS: {
          match(MINUS);
          break;
        }
        case STAR: {
          match(STAR);
          break;
        }
        case DIVIDE: {
          match(DIVIDE);
          break;
        }
        case MOD: {
          match(MOD);
          break;
        }
        case BITWISEXOR: {
          match(BITWISEXOR);
          break;
        }
        case AMPERSAND: {
          match(AMPERSAND);
          break;
        }
        case BITWISEOR: {
          match(BITWISEOR);
          break;
        }
        case TILDE: {
          match(TILDE);
          break;
        }
        case NOT: {
          match(NOT);
          break;
        }
        case SHIFTLEFT: {
          match(SHIFTLEFT);
          break;
        }
        case SHIFTRIGHT: {
          match(SHIFTRIGHT);
          break;
        }
        case ASSIGNEQUAL: {
          match(ASSIGNEQUAL);
          break;
        }
        case TIMESEQUAL: {
          match(TIMESEQUAL);
          break;
        }
        case DIVIDEEQUAL: {
          match(DIVIDEEQUAL);
          break;
        }
        case MODEQUAL: {
          match(MODEQUAL);
          break;
        }
        case PLUSEQUAL: {
          match(PLUSEQUAL);
          break;
        }
        case MINUSEQUAL: {
          match(MINUSEQUAL);
          break;
        }
        case SHIFTLEFTEQUAL: {
          match(SHIFTLEFTEQUAL);
          break;
        }
        case SHIFTRIGHTEQUAL: {
          match(SHIFTRIGHTEQUAL);
          break;
        }
        case BITWISEANDEQUAL: {
          match(BITWISEANDEQUAL);
          break;
        }
        case BITWISEXOREQUAL: {
          match(BITWISEXOREQUAL);
          break;
        }
        case BITWISEOREQUAL: {
          match(BITWISEOREQUAL);
          break;
        }
        case EQUAL: {
          match(EQUAL);
          break;
        }
        case NOTEQUAL: {
          match(NOTEQUAL);
          break;
        }
        case LESSTHAN: {
          match(LESSTHAN);
          break;
        }
        case GREATERTHAN: {
          match(GREATERTHAN);
          break;
        }
        case LESSTHANOREQUALTO: {
          match(LESSTHANOREQUALTO);
          break;
        }
        case GREATERTHANOREQUALTO: {
          match(GREATERTHANOREQUALTO);
          break;
        }
        case OR: {
          match(OR);
          break;
        }
        case AND: {
          match(AND);
          break;
        }
        case PLUSPLUS: {
          match(PLUSPLUS);
          break;
        }
        case MINUSMINUS: {
          match(MINUSMINUS);
          break;
        }
        case COMMA: {
          match(COMMA);
          break;
        }
        case POINTERTO: {
          match(POINTERTO);
          break;
        }
        case POINTERTOMBR: {
          match(POINTERTOMBR);
          break;
        }
        default: {
          throw ANTLR_USE_NAMESPACE(antlr)
              NoViableAltException(LT(1), getFilename());
        }
      }
    }
  } catch (ANTLR_USE_NAMESPACE(antlr) RecognitionException& ex) {
    if (inputState->guessing == 0) {
      reportError(ex);
      recover(ex, _tokenSet_64);
    } else {
      throw;
    }
  }
}

void CPPParser::initializeASTFactory(ANTLR_USE_NAMESPACE(antlr) ASTFactory&) {}
const char* CPPParser::tokenNames[] = {"<0>",
                                       "EOF",
                                       "<2>",
                                       "NULL_TREE_LOOKAHEAD",
                                       "\"template\"",
                                       "LESSTHAN",
                                       "GREATERTHAN",
                                       "\"typedef\"",
                                       "SEMICOLON",
                                       "\"enum\"",
                                       "ID",
                                       "LCURLY",
                                       "\"inline\"",
                                       "\"friend\"",
                                       "\"namespace\"",
                                       "\"antlrTrace_on\"",
                                       "\"antlrTrace_off\"",
                                       "RCURLY",
                                       "ASSIGNEQUAL",
                                       "COLON",
                                       "\"extern\"",
                                       "StringLiteral",
                                       "\"struct\"",
                                       "\"union\"",
                                       "\"class\"",
                                       "\"_stdcall\"",
                                       "\"__stdcall\"",
                                       "\"GFEXCLUDE\"",
                                       "\"GFINCLUDE\"",
                                       "\"GFID\"",
                                       "\"GFUNREAD\"",
                                       "\"GFARRAYSIZE\"",
                                       "LPAREN",
                                       "RPAREN",
                                       "\"GFARRAYSIZES\"",
                                       "\"GFARRAYELEMSIZE\"",
                                       "\"auto\"",
                                       "\"register\"",
                                       "\"static\"",
                                       "\"mutable\"",
                                       "\"_inline\"",
                                       "\"__inline\"",
                                       "\"virtual\"",
                                       "\"explicit\"",
                                       "\"typename\"",
                                       "\"char\"",
                                       "\"wchar_t\"",
                                       "\"bool\"",
                                       "\"short\"",
                                       "\"int\"",
                                       "\"_int8\"",
                                       "\"__int8\"",
                                       "\"int8_t\"",
                                       "\"_int16\"",
                                       "\"__int16\"",
                                       "\"int16_t\"",
                                       "\"_int32\"",
                                       "\"__int32\"",
                                       "\"int32_t\"",
                                       "\"_int64\"",
                                       "\"__int64\"",
                                       "\"int64_t\"",
                                       "\"uint8_t\"",
                                       "\"uint16_t\"",
                                       "\"uint32_t\"",
                                       "\"uint64_t\"",
                                       "\"_w64\"",
                                       "\"__w64\"",
                                       "\"long\"",
                                       "\"signed\"",
                                       "\"unsigned\"",
                                       "\"float\"",
                                       "\"double\"",
                                       "\"void\"",
                                       "\"_declspec\"",
                                       "\"__declspec\"",
                                       "\"const\"",
                                       "\"__const\"",
                                       "\"volatile\"",
                                       "\"__volatile__\"",
                                       "\"GFIGNORE\"",
                                       "COMMA",
                                       "\"public\"",
                                       "\"protected\"",
                                       "\"private\"",
                                       "\"operator\"",
                                       "\"this\"",
                                       "\"true\"",
                                       "\"false\"",
                                       "OCTALINT",
                                       "LSQUARE",
                                       "RSQUARE",
                                       "TILDE",
                                       "STAR",
                                       "AMPERSAND",
                                       "ELLIPSIS",
                                       "\"throw\"",
                                       "\"using\"",
                                       "\"case\"",
                                       "\"default\"",
                                       "\"if\"",
                                       "\"else\"",
                                       "\"switch\"",
                                       "\"while\"",
                                       "\"do\"",
                                       "\"for\"",
                                       "\"goto\"",
                                       "\"continue\"",
                                       "\"break\"",
                                       "\"return\"",
                                       "\"try\"",
                                       "\"catch\"",
                                       "\"asm\"",
                                       "\"_asm\"",
                                       "\"__asm\"",
                                       "\"__asm__\"",
                                       "TIMESEQUAL",
                                       "DIVIDEEQUAL",
                                       "MINUSEQUAL",
                                       "PLUSEQUAL",
                                       "MODEQUAL",
                                       "SHIFTLEFTEQUAL",
                                       "SHIFTRIGHTEQUAL",
                                       "BITWISEANDEQUAL",
                                       "BITWISEXOREQUAL",
                                       "BITWISEOREQUAL",
                                       "QUESTIONMARK",
                                       "OR",
                                       "AND",
                                       "BITWISEOR",
                                       "BITWISEXOR",
                                       "NOTEQUAL",
                                       "EQUAL",
                                       "LESSTHANOREQUALTO",
                                       "GREATERTHANOREQUALTO",
                                       "SHIFTLEFT",
                                       "SHIFTRIGHT",
                                       "PLUS",
                                       "MINUS",
                                       "DIVIDE",
                                       "MOD",
                                       "DOTMBR",
                                       "POINTERTOMBR",
                                       "PLUSPLUS",
                                       "MINUSMINUS",
                                       "\"sizeof\"",
                                       "\"__alignof__\"",
                                       "SCOPE",
                                       "DOT",
                                       "POINTERTO",
                                       "\"dynamic_cast\"",
                                       "\"static_cast\"",
                                       "\"reinterpret_cast\"",
                                       "\"const_cast\"",
                                       "\"typeid\"",
                                       "DECIMALINT",
                                       "HEXADECIMALINT",
                                       "CharLiteral",
                                       "WCharLiteral",
                                       "WStringLiteral",
                                       "FLOATONE",
                                       "FLOATTWO",
                                       "NOT",
                                       "\"new\"",
                                       "\"_cdecl\"",
                                       "\"__cdecl\"",
                                       "\"_near\"",
                                       "\"__near\"",
                                       "\"_far\"",
                                       "\"__far\"",
                                       "\"__interrupt\"",
                                       "\"pascal\"",
                                       "\"_pascal\"",
                                       "\"__pascal\"",
                                       "\"delete\"",
                                       "Whitespace",
                                       "Comment",
                                       "CPPComment",
                                       "a preprocessor directive",
                                       "LineDirective",
                                       "Space",
                                       "Pragma",
                                       "Error",
                                       "EndOfLine",
                                       "Escape",
                                       "Digit",
                                       "Decimal",
                                       "LongSuffix",
                                       "UnsignedSuffix",
                                       "FloatSuffix",
                                       "Exponent",
                                       "Vocabulary",
                                       "Number",
                                       0};

const unsigned long CPPParser::_tokenSet_0_data_[] = {
    4291950480UL, 4294967293UL, 1910571007UL, 2UL, 524288UL, 16368UL,
    0UL,          0UL,          0UL,          0UL, 0UL,      0UL};
// "template" "typedef" SEMICOLON "enum" ID "inline" "friend" "namespace"
// "antlrTrace_on" "antlrTrace_off" "extern" "struct" "union" "class" "_stdcall"
// "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID" "GFUNREAD" "GFARRAYSIZE"
// LPAREN "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto" "register" "static" "mutable"
// "_inline" "__inline" "virtual" "explicit" "typename" "char" "wchar_t"
// "bool" "short" "int" "_int8" "__int8" "int8_t" "_int16" "__int16" "int16_t"
// "_int32" "__int32" "int32_t" "_int64" "__int64" "int64_t" "uint8_t"
// "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64" "long" "signed" "unsigned"
// "float" "double" "void" "_declspec" "__declspec" "const" "__const" "volatile"
// "__volatile__" "operator" "this" "true" "false" TILDE STAR AMPERSAND
// "using" SCOPE "_cdecl" "__cdecl" "_near" "__near" "_far" "__far"
// "__interrupt"
// "pascal" "_pascal" "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_0(
    _tokenSet_0_data_, 12);
const unsigned long CPPParser::_tokenSet_1_data_[] = {2UL, 0UL, 0UL, 0UL,
                                                      0UL, 0UL, 0UL, 0UL};
// EOF
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_1(
    _tokenSet_1_data_, 8);
const unsigned long CPPParser::_tokenSet_2_data_[] = {
    4291835520UL, 4294967292UL, 65535UL, 2UL, 524288UL, 0UL,
    0UL,          0UL,          0UL,     0UL, 0UL,      0UL};
// "typedef" "enum" ID "inline" "friend" "extern" "struct" "union" "class"
// "_stdcall" "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID" "GFUNREAD"
// "GFARRAYSIZE"
// "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto" "register" "static" "mutable"
// "_inline" "__inline" "virtual" "explicit" "typename" "char" "wchar_t"
// "bool" "short" "int" "_int8" "__int8" "int8_t" "_int16" "__int16" "int16_t"
// "_int32" "__int32" "int32_t" "_int64" "__int64" "int64_t" "uint8_t"
// "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64" "long" "signed" "unsigned"
// "float" "double" "void" "_declspec" "__declspec" "const" "__const" "volatile"
// "__volatile__" "using" SCOPE
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_2(
    _tokenSet_2_data_, 12);
const unsigned long CPPParser::_tokenSet_3_data_[] = {
    4293949344UL, 4294967293UL, 1910571007UL, 0UL, 524288UL, 16368UL,
    0UL,          0UL,          0UL,          0UL, 0UL,      0UL};
// LESSTHAN "typedef" SEMICOLON "enum" ID "inline" "friend" "namespace"
// "extern" StringLiteral "struct" "union" "class" "_stdcall" "__stdcall"
// "GFEXCLUDE" "GFINCLUDE" "GFID" "GFUNREAD" "GFARRAYSIZE" LPAREN "GFARRAYSIZES"
// "GFARRAYELEMSIZE" "auto" "register" "static" "mutable" "_inline" "__inline"
// "virtual" "explicit" "typename" "char" "wchar_t" "bool" "short" "int"
// "_int8" "__int8" "int8_t" "_int16" "__int16" "int16_t" "_int32" "__int32"
// "int32_t" "_int64" "__int64" "int64_t" "uint8_t" "uint16_t" "uint32_t"
// "uint64_t" "_w64" "__w64" "long" "signed" "unsigned" "float" "double"
// "void" "_declspec" "__declspec" "const" "__const" "volatile" "__volatile__"
// "operator" "this" "true" "false" TILDE STAR AMPERSAND SCOPE "_cdecl"
// "__cdecl" "_near" "__near" "_far" "__far" "__interrupt" "pascal" "_pascal"
// "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_3(
    _tokenSet_3_data_, 12);
const unsigned long CPPParser::_tokenSet_4_data_[] = {4096UL, 3840UL, 0UL, 0UL,
                                                      0UL,    0UL,    0UL, 0UL};
// "inline" "_inline" "__inline" "virtual" "explicit"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_4(
    _tokenSet_4_data_, 8);
const unsigned long CPPParser::_tokenSet_5_data_[] = {
    3072UL, 0UL, 31457280UL, 0UL, 524288UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// ID LCURLY "operator" "this" "true" "false" SCOPE
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_5(
    _tokenSet_5_data_, 12);
const unsigned long CPPParser::_tokenSet_6_data_[] = {
    5136UL, 1792UL, 268435456UL, 0UL, 524288UL, 0UL,
    0UL,    0UL,    0UL,         0UL, 0UL,      0UL};
// "template" ID "inline" "_inline" "__inline" "virtual" TILDE SCOPE
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_6(
    _tokenSet_6_data_, 12);
const unsigned long CPPParser::_tokenSet_7_data_[] = {
    5152UL, 1792UL, 268435456UL, 0UL, 524288UL, 0UL,
    0UL,    0UL,    0UL,         0UL, 0UL,      0UL};
// LESSTHAN ID "inline" "_inline" "__inline" "virtual" TILDE SCOPE
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_7(
    _tokenSet_7_data_, 12);
const unsigned long CPPParser::_tokenSet_8_data_[] = {
    5120UL, 2816UL, 0UL, 0UL, 524288UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// ID "inline" "_inline" "__inline" "explicit" SCOPE
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_8(
    _tokenSet_8_data_, 12);
const unsigned long CPPParser::_tokenSet_9_data_[] = {
    5152UL, 2817UL, 0UL, 0UL, 524288UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// LESSTHAN ID "inline" LPAREN "_inline" "__inline" "explicit" SCOPE
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_9(
    _tokenSet_9_data_, 12);
const unsigned long CPPParser::_tokenSet_10_data_[] = {
    5120UL, 0UL, 2097152UL, 0UL, 524288UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// ID "inline" "operator" SCOPE
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_10(
    _tokenSet_10_data_, 12);
const unsigned long CPPParser::_tokenSet_11_data_[] = {
    4291835552UL, 4294967292UL, 2162687UL, 0UL, 524288UL, 0UL,
    0UL,          0UL,          0UL,       0UL, 0UL,      0UL};
// LESSTHAN "typedef" "enum" ID "inline" "friend" "extern" "struct" "union"
// "class" "_stdcall" "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID" "GFUNREAD"
// "GFARRAYSIZE" "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto" "register" "static"
// "mutable" "_inline" "__inline" "virtual" "explicit" "typename" "char"
// "wchar_t" "bool" "short" "int" "_int8" "__int8" "int8_t" "_int16" "__int16"
// "int16_t" "_int32" "__int32" "int32_t" "_int64" "__int64" "int64_t"
// "uint8_t" "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64" "long" "signed"
// "unsigned" "float" "double" "void" "_declspec" "__declspec" "const"
// "__const" "volatile" "__volatile__" "operator" SCOPE
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_11(
    _tokenSet_11_data_, 12);
const unsigned long CPPParser::_tokenSet_12_data_[] = {
    4291835520UL, 4294967292UL, 65535UL, 0UL, 524288UL, 0UL,
    0UL,          0UL,          0UL,     0UL, 0UL,      0UL};
// "typedef" "enum" ID "inline" "friend" "extern" "struct" "union" "class"
// "_stdcall" "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID" "GFUNREAD"
// "GFARRAYSIZE"
// "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto" "register" "static" "mutable"
// "_inline" "__inline" "virtual" "explicit" "typename" "char" "wchar_t"
// "bool" "short" "int" "_int8" "__int8" "int8_t" "_int16" "__int16" "int16_t"
// "_int32" "__int32" "int32_t" "_int64" "__int64" "int64_t" "uint8_t"
// "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64" "long" "signed" "unsigned"
// "float" "double" "void" "_declspec" "__declspec" "const" "__const" "volatile"
// "__volatile__" SCOPE
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_12(
    _tokenSet_12_data_, 12);
const unsigned long CPPParser::_tokenSet_13_data_[] = {
    4291835552UL, 4294967293UL, 1642135551UL, 0UL, 524288UL, 16368UL,
    0UL,          0UL,          0UL,          0UL, 0UL,      0UL};
// LESSTHAN "typedef" "enum" ID "inline" "friend" "extern" "struct" "union"
// "class" "_stdcall" "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID" "GFUNREAD"
// "GFARRAYSIZE" LPAREN "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto" "register"
// "static" "mutable" "_inline" "__inline" "virtual" "explicit" "typename"
// "char" "wchar_t" "bool" "short" "int" "_int8" "__int8" "int8_t" "_int16"
// "__int16" "int16_t" "_int32" "__int32" "int32_t" "_int64" "__int64"
// "int64_t" "uint8_t" "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64"
// "long" "signed" "unsigned" "float" "double" "void" "_declspec" "__declspec"
// "const" "__const" "volatile" "__volatile__" "operator" "this" "true"
// "false" STAR AMPERSAND SCOPE "_cdecl" "__cdecl" "_near" "__near" "_far"
// "__far" "__interrupt" "pascal" "_pascal" "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_13(
    _tokenSet_13_data_, 12);
const unsigned long CPPParser::_tokenSet_14_data_[] = {
    4291835520UL, 4294967293UL, 1642135551UL, 0UL, 524288UL, 16368UL,
    0UL,          0UL,          0UL,          0UL, 0UL,      0UL};
// "typedef" "enum" ID "inline" "friend" "extern" "struct" "union" "class"
// "_stdcall" "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID" "GFUNREAD"
// "GFARRAYSIZE"
// LPAREN "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto" "register" "static" "mutable"
// "_inline" "__inline" "virtual" "explicit" "typename" "char" "wchar_t"
// "bool" "short" "int" "_int8" "__int8" "int8_t" "_int16" "__int16" "int16_t"
// "_int32" "__int32" "int32_t" "_int64" "__int64" "int64_t" "uint8_t"
// "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64" "long" "signed" "unsigned"
// "float" "double" "void" "_declspec" "__declspec" "const" "__const" "volatile"
// "__volatile__" "operator" "this" "true" "false" STAR AMPERSAND SCOPE
// "_cdecl" "__cdecl" "_near" "__near" "_far" "__far" "__interrupt" "pascal"
// "_pascal" "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_14(
    _tokenSet_14_data_, 12);
const unsigned long CPPParser::_tokenSet_15_data_[] = {
    4292097760UL, 4294967293UL, 1977810943UL, 3220176896UL, 2744319UL, 32764UL,
    0UL,          0UL,          0UL,          0UL,          0UL,       0UL};
// LESSTHAN GREATERTHAN "typedef" "enum" ID "inline" "friend" ASSIGNEQUAL
// "extern" "struct" "union" "class" "_stdcall" "__stdcall" "GFEXCLUDE"
// "GFINCLUDE" "GFID" "GFUNREAD" "GFARRAYSIZE" LPAREN "GFARRAYSIZES"
// "GFARRAYELEMSIZE"
// "auto" "register" "static" "mutable" "_inline" "__inline" "virtual"
// "explicit" "typename" "char" "wchar_t" "bool" "short" "int" "_int8"
// "__int8" "int8_t" "_int16" "__int16" "int16_t" "_int32" "__int32" "int32_t"
// "_int64" "__int64" "int64_t" "uint8_t" "uint16_t" "uint32_t" "uint64_t"
// "_w64" "__w64" "long" "signed" "unsigned" "float" "double" "void" "_declspec"
// "__declspec" "const" "__const" "volatile" "__volatile__" COMMA "operator"
// "this" "true" "false" LSQUARE TILDE STAR AMPERSAND TIMESEQUAL DIVIDEEQUAL
// MINUSEQUAL PLUSEQUAL MODEQUAL SHIFTLEFTEQUAL SHIFTRIGHTEQUAL BITWISEANDEQUAL
// BITWISEXOREQUAL BITWISEOREQUAL OR AND BITWISEOR BITWISEXOR NOTEQUAL
// EQUAL LESSTHANOREQUALTO GREATERTHANOREQUALTO SHIFTLEFT SHIFTRIGHT PLUS
// MINUS DIVIDE MOD POINTERTOMBR PLUSPLUS MINUSMINUS SCOPE POINTERTO NOT
// "new" "_cdecl" "__cdecl" "_near" "__near" "_far" "__far" "__interrupt"
// "pascal" "_pascal" "__pascal" "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_15(
    _tokenSet_15_data_, 12);
const unsigned long CPPParser::_tokenSet_16_data_[] = {
    29372416UL, 3840UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// "inline" "friend" "struct" "union" "class" "_inline" "__inline" "virtual"
// "explicit"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_16(
    _tokenSet_16_data_, 8);
const unsigned long CPPParser::_tokenSet_17_data_[] = {
    29367296UL, 3840UL, 31525888UL, 0UL, 524288UL, 0UL,
    0UL,        0UL,    0UL,        0UL, 0UL,      0UL};
// ID LCURLY "inline" "struct" "union" "class" "_inline" "__inline" "virtual"
// "explicit" "_declspec" "__declspec" "GFIGNORE" "operator" "this" "true"
// "false" SCOPE
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_17(
    _tokenSet_17_data_, 12);
const unsigned long CPPParser::_tokenSet_18_data_[] = {
    4291835808UL, 4294967293UL, 1910571007UL, 0UL, 524288UL, 16368UL,
    0UL,          0UL,          0UL,          0UL, 0UL,      0UL};
// LESSTHAN "typedef" SEMICOLON "enum" ID "inline" "friend" "extern" "struct"
// "union" "class" "_stdcall" "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID"
// "GFUNREAD" "GFARRAYSIZE" LPAREN "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto"
// "register" "static" "mutable" "_inline" "__inline" "virtual" "explicit"
// "typename" "char" "wchar_t" "bool" "short" "int" "_int8" "__int8" "int8_t"
// "_int16" "__int16" "int16_t" "_int32" "__int32" "int32_t" "_int64" "__int64"
// "int64_t" "uint8_t" "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64"
// "long" "signed" "unsigned" "float" "double" "void" "_declspec" "__declspec"
// "const" "__const" "volatile" "__volatile__" "operator" "this" "true"
// "false" TILDE STAR AMPERSAND SCOPE "_cdecl" "__cdecl" "_near" "__near"
// "_far" "__far" "__interrupt" "pascal" "_pascal" "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_18(
    _tokenSet_18_data_, 12);
const unsigned long CPPParser::_tokenSet_19_data_[] = {
    4292081554UL, 4294967293UL, 1910571007UL, 2UL, 524288UL, 16368UL,
    0UL,          0UL,          0UL,          0UL, 0UL,      0UL};
// EOF "template" "typedef" SEMICOLON "enum" ID "inline" "friend" "namespace"
// "antlrTrace_on" "antlrTrace_off" RCURLY "extern" "struct" "union" "class"
// "_stdcall" "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID" "GFUNREAD"
// "GFARRAYSIZE"
// LPAREN "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto" "register" "static" "mutable"
// "_inline" "__inline" "virtual" "explicit" "typename" "char" "wchar_t"
// "bool" "short" "int" "_int8" "__int8" "int8_t" "_int16" "__int16" "int16_t"
// "_int32" "__int32" "int32_t" "_int64" "__int64" "int64_t" "uint8_t"
// "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64" "long" "signed" "unsigned"
// "float" "double" "void" "_declspec" "__declspec" "const" "__const" "volatile"
// "__volatile__" "operator" "this" "true" "false" TILDE STAR AMPERSAND
// "using" SCOPE "_cdecl" "__cdecl" "_near" "__near" "_far" "__far"
// "__interrupt"
// "pascal" "_pascal" "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_19(
    _tokenSet_19_data_, 12);
const unsigned long CPPParser::_tokenSet_20_data_[] = {
    4291934096UL, 4294967293UL, 1912406015UL, 2UL, 524288UL, 16368UL,
    0UL,          0UL,          0UL,          0UL, 0UL,      0UL};
// "template" "typedef" SEMICOLON "enum" ID "inline" "friend" "antlrTrace_on"
// "antlrTrace_off" "extern" "struct" "union" "class" "_stdcall" "__stdcall"
// "GFEXCLUDE" "GFINCLUDE" "GFID" "GFUNREAD" "GFARRAYSIZE" LPAREN "GFARRAYSIZES"
// "GFARRAYELEMSIZE" "auto" "register" "static" "mutable" "_inline" "__inline"
// "virtual" "explicit" "typename" "char" "wchar_t" "bool" "short" "int"
// "_int8" "__int8" "int8_t" "_int16" "__int16" "int16_t" "_int32" "__int32"
// "int32_t" "_int64" "__int64" "int64_t" "uint8_t" "uint16_t" "uint32_t"
// "uint64_t" "_w64" "__w64" "long" "signed" "unsigned" "float" "double"
// "void" "_declspec" "__declspec" "const" "__const" "volatile" "__volatile__"
// "public" "protected" "private" "operator" "this" "true" "false" TILDE
// STAR AMPERSAND "using" SCOPE "_cdecl" "__cdecl" "_near" "__near" "_far"
// "__far" "__interrupt" "pascal" "_pascal" "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_20(
    _tokenSet_20_data_, 12);
const unsigned long CPPParser::_tokenSet_21_data_[] = {
    100664576UL, 1UL, 1910505472UL, 0UL, 524288UL, 16368UL,
    0UL,         0UL, 0UL,          0UL, 0UL,      0UL};
// SEMICOLON ID "_stdcall" "__stdcall" LPAREN "operator" "this" "true"
// "false" TILDE STAR AMPERSAND SCOPE "_cdecl" "__cdecl" "_near" "__near"
// "_far" "__far" "__interrupt" "pascal" "_pascal" "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_21(
    _tokenSet_21_data_, 12);
const unsigned long CPPParser::_tokenSet_22_data_[] = {256UL, 0UL, 0UL, 0UL,
                                                       0UL,   0UL, 0UL, 0UL};
// SEMICOLON
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_22(
    _tokenSet_22_data_, 8);
const unsigned long CPPParser::_tokenSet_23_data_[] = {
    101188864UL, 1UL, 1910505472UL, 0UL, 524288UL, 16368UL,
    0UL,         0UL, 0UL,          0UL, 0UL,      0UL};
// SEMICOLON ID COLON "_stdcall" "__stdcall" LPAREN "operator" "this" "true"
// "false" TILDE STAR AMPERSAND SCOPE "_cdecl" "__cdecl" "_near" "__near"
// "_far" "__far" "__interrupt" "pascal" "_pascal" "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_23(
    _tokenSet_23_data_, 12);
const unsigned long CPPParser::_tokenSet_24_data_[] = {
    101582176UL, 3UL, 4259446784UL, 4293918720UL, 557055UL, 16368UL,
    0UL,         0UL, 0UL,          0UL,          0UL,      0UL};
// LESSTHAN GREATERTHAN SEMICOLON ID RCURLY ASSIGNEQUAL COLON "_stdcall"
// "__stdcall" LPAREN RPAREN COMMA "operator" "this" "true" "false" LSQUARE
// RSQUARE TILDE STAR AMPERSAND ELLIPSIS TIMESEQUAL DIVIDEEQUAL MINUSEQUAL
// PLUSEQUAL MODEQUAL SHIFTLEFTEQUAL SHIFTRIGHTEQUAL BITWISEANDEQUAL
// BITWISEXOREQUAL
// BITWISEOREQUAL QUESTIONMARK OR AND BITWISEOR BITWISEXOR NOTEQUAL EQUAL
// LESSTHANOREQUALTO GREATERTHANOREQUALTO SHIFTLEFT SHIFTRIGHT PLUS MINUS
// DIVIDE MOD DOTMBR POINTERTOMBR SCOPE "_cdecl" "__cdecl" "_near" "__near"
// "_far" "__far" "__interrupt" "pascal" "_pascal" "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_24(
    _tokenSet_24_data_, 12);
const unsigned long CPPParser::_tokenSet_25_data_[] = {
    100664320UL, 0UL, 1610612736UL, 0UL, 524288UL, 16368UL,
    0UL,         0UL, 0UL,          0UL, 0UL,      0UL};
// ID "_stdcall" "__stdcall" STAR AMPERSAND SCOPE "_cdecl" "__cdecl" "_near"
// "__near" "_far" "__far" "__interrupt" "pascal" "_pascal" "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_25(
    _tokenSet_25_data_, 12);
const unsigned long CPPParser::_tokenSet_26_data_[] = {
    100664352UL, 1UL, 1642131456UL, 0UL, 524288UL, 16368UL,
    0UL,         0UL, 0UL,          0UL, 0UL,      0UL};
// LESSTHAN ID "_stdcall" "__stdcall" LPAREN "const" "__const" "volatile"
// "__volatile__" "operator" "this" "true" "false" STAR AMPERSAND SCOPE
// "_cdecl" "__cdecl" "_near" "__near" "_far" "__far" "__interrupt" "pascal"
// "_pascal" "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_26(
    _tokenSet_26_data_, 12);
const unsigned long CPPParser::_tokenSet_27_data_[] = {
    1024UL, 1UL, 31457280UL, 0UL, 524288UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// ID LPAREN "operator" "this" "true" "false" SCOPE
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_27(
    _tokenSet_27_data_, 12);
const unsigned long CPPParser::_tokenSet_28_data_[] = {
    263264UL, 1UL, 1977745408UL, 3220176896UL, 2744319UL, 16396UL,
    0UL,      0UL, 0UL,          0UL,          0UL,       0UL};
// LESSTHAN GREATERTHAN ID ASSIGNEQUAL LPAREN COMMA "operator" "this" "true"
// "false" LSQUARE TILDE STAR AMPERSAND TIMESEQUAL DIVIDEEQUAL MINUSEQUAL
// PLUSEQUAL MODEQUAL SHIFTLEFTEQUAL SHIFTRIGHTEQUAL BITWISEANDEQUAL
// BITWISEXOREQUAL
// BITWISEOREQUAL OR AND BITWISEOR BITWISEXOR NOTEQUAL EQUAL LESSTHANOREQUALTO
// GREATERTHANOREQUALTO SHIFTLEFT SHIFTRIGHT PLUS MINUS DIVIDE MOD POINTERTOMBR
// PLUSPLUS MINUSMINUS SCOPE POINTERTO NOT "new" "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_28(
    _tokenSet_28_data_, 12);
const unsigned long CPPParser::_tokenSet_29_data_[] = {
    4291837824UL, 4294967292UL, 65535UL, 2UL, 524288UL, 0UL,
    0UL,          0UL,          0UL,     0UL, 0UL,      0UL};
// "typedef" SEMICOLON "enum" ID LCURLY "inline" "friend" "extern" "struct"
// "union" "class" "_stdcall" "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID"
// "GFUNREAD" "GFARRAYSIZE" "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto" "register"
// "static" "mutable" "_inline" "__inline" "virtual" "explicit" "typename"
// "char" "wchar_t" "bool" "short" "int" "_int8" "__int8" "int8_t" "_int16"
// "__int16" "int16_t" "_int32" "__int32" "int32_t" "_int64" "__int64"
// "int64_t" "uint8_t" "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64"
// "long" "signed" "unsigned" "float" "double" "void" "_declspec" "__declspec"
// "const" "__const" "volatile" "__volatile__" "using" SCOPE
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_29(
    _tokenSet_29_data_, 12);
const unsigned long CPPParser::_tokenSet_30_data_[] = {
    4294180754UL, 4294967293UL, 1945960447UL, 1015807UL, 4291790336UL, 32767UL,
    0UL,          0UL,          0UL,          0UL,       0UL,          0UL};
// EOF "template" "typedef" SEMICOLON "enum" ID LCURLY "inline" "friend"
// "namespace" "antlrTrace_on" "antlrTrace_off" RCURLY "extern" StringLiteral
// "struct" "union" "class" "_stdcall" "__stdcall" "GFEXCLUDE" "GFINCLUDE"
// "GFID" "GFUNREAD" "GFARRAYSIZE" LPAREN "GFARRAYSIZES" "GFARRAYELEMSIZE"
// "auto" "register" "static" "mutable" "_inline" "__inline" "virtual"
// "explicit" "typename" "char" "wchar_t" "bool" "short" "int" "_int8"
// "__int8" "int8_t" "_int16" "__int16" "int16_t" "_int32" "__int32" "int32_t"
// "_int64" "__int64" "int64_t" "uint8_t" "uint16_t" "uint32_t" "uint64_t"
// "_w64" "__w64" "long" "signed" "unsigned" "float" "double" "void" "_declspec"
// "__declspec" "const" "__const" "volatile" "__volatile__" "public" "protected"
// "private" "operator" "this" "true" "false" OCTALINT TILDE STAR AMPERSAND
// "throw" "using" "case" "default" "if" "else" "switch" "while" "do" "for"
// "goto" "continue" "break" "return" "try" "asm" "_asm" "__asm" "__asm__"
// PLUS MINUS PLUSPLUS MINUSMINUS "sizeof" "__alignof__" SCOPE "dynamic_cast"
// "static_cast" "reinterpret_cast" "const_cast" "typeid" DECIMALINT
// HEXADECIMALINT
// CharLiteral WCharLiteral WStringLiteral FLOATONE FLOATTWO NOT "new"
// "_cdecl" "__cdecl" "_near" "__near" "_far" "__far" "__interrupt" "pascal"
// "_pascal" "__pascal" "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_30(
    _tokenSet_30_data_, 12);
const unsigned long CPPParser::_tokenSet_31_data_[] = {
    4291835520UL, 4294967293UL, 1910571007UL, 2UL, 524288UL, 16368UL,
    0UL,          0UL,          0UL,          0UL, 0UL,      0UL};
// "typedef" "enum" ID "inline" "friend" "extern" "struct" "union" "class"
// "_stdcall" "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID" "GFUNREAD"
// "GFARRAYSIZE"
// LPAREN "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto" "register" "static" "mutable"
// "_inline" "__inline" "virtual" "explicit" "typename" "char" "wchar_t"
// "bool" "short" "int" "_int8" "__int8" "int8_t" "_int16" "__int16" "int16_t"
// "_int32" "__int32" "int32_t" "_int64" "__int64" "int64_t" "uint8_t"
// "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64" "long" "signed" "unsigned"
// "float" "double" "void" "_declspec" "__declspec" "const" "__const" "volatile"
// "__volatile__" "operator" "this" "true" "false" TILDE STAR AMPERSAND
// "using" SCOPE "_cdecl" "__cdecl" "_near" "__near" "_far" "__far"
// "__interrupt"
// "pascal" "_pascal" "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_31(
    _tokenSet_31_data_, 12);
const unsigned long CPPParser::_tokenSet_32_data_[] = {2304UL, 0UL, 0UL, 0UL,
                                                       0UL,    0UL, 0UL, 0UL};
// SEMICOLON LCURLY
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_32(
    _tokenSet_32_data_, 8);
const unsigned long CPPParser::_tokenSet_33_data_[] = {
    1024UL, 0UL, 0UL, 0UL, 524288UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// ID SCOPE
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_33(
    _tokenSet_33_data_, 12);
const unsigned long CPPParser::_tokenSet_34_data_[] = {
    1024UL, 0UL, 836763648UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// ID "operator" "this" "true" "false" TILDE STAR
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_34(
    _tokenSet_34_data_, 8);
const unsigned long CPPParser::_tokenSet_35_data_[] = {
    4291837568UL, 4294967292UL, 65535UL, 2UL, 524288UL, 0UL,
    0UL,          0UL,          0UL,     0UL, 0UL,      0UL};
// "typedef" "enum" ID LCURLY "inline" "friend" "extern" "struct" "union"
// "class" "_stdcall" "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID" "GFUNREAD"
// "GFARRAYSIZE" "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto" "register" "static"
// "mutable" "_inline" "__inline" "virtual" "explicit" "typename" "char"
// "wchar_t" "bool" "short" "int" "_int8" "__int8" "int8_t" "_int16" "__int16"
// "int16_t" "_int32" "__int32" "int32_t" "_int64" "__int64" "int64_t"
// "uint8_t" "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64" "long" "signed"
// "unsigned" "float" "double" "void" "_declspec" "__declspec" "const"
// "__const" "volatile" "__volatile__" "using" SCOPE
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_35(
    _tokenSet_35_data_, 12);
const unsigned long CPPParser::_tokenSet_36_data_[] = {
    4294180784UL, 4294967293UL, 1945960447UL, 1015775UL, 4291790336UL, 32767UL,
    0UL,          0UL,          0UL,          0UL,       0UL,          0UL};
// "template" LESSTHAN "typedef" SEMICOLON "enum" ID LCURLY "inline" "friend"
// "namespace" "antlrTrace_on" "antlrTrace_off" RCURLY "extern" StringLiteral
// "struct" "union" "class" "_stdcall" "__stdcall" "GFEXCLUDE" "GFINCLUDE"
// "GFID" "GFUNREAD" "GFARRAYSIZE" LPAREN "GFARRAYSIZES" "GFARRAYELEMSIZE"
// "auto" "register" "static" "mutable" "_inline" "__inline" "virtual"
// "explicit" "typename" "char" "wchar_t" "bool" "short" "int" "_int8"
// "__int8" "int8_t" "_int16" "__int16" "int16_t" "_int32" "__int32" "int32_t"
// "_int64" "__int64" "int64_t" "uint8_t" "uint16_t" "uint32_t" "uint64_t"
// "_w64" "__w64" "long" "signed" "unsigned" "float" "double" "void" "_declspec"
// "__declspec" "const" "__const" "volatile" "__volatile__" "public" "protected"
// "private" "operator" "this" "true" "false" OCTALINT TILDE STAR AMPERSAND
// "throw" "using" "case" "default" "if" "switch" "while" "do" "for" "goto"
// "continue" "break" "return" "try" "asm" "_asm" "__asm" "__asm__" PLUS
// MINUS PLUSPLUS MINUSMINUS "sizeof" "__alignof__" SCOPE "dynamic_cast"
// "static_cast" "reinterpret_cast" "const_cast" "typeid" DECIMALINT
// HEXADECIMALINT
// CharLiteral WCharLiteral WStringLiteral FLOATONE FLOATTWO NOT "new"
// "_cdecl" "__cdecl" "_near" "__near" "_far" "__far" "__interrupt" "pascal"
// "_pascal" "__pascal" "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_36(
    _tokenSet_36_data_, 12);
const unsigned long CPPParser::_tokenSet_37_data_[] = {
    4294180752UL, 4294967293UL, 1945960447UL, 1015775UL, 4291790336UL, 32767UL,
    0UL,          0UL,          0UL,          0UL,       0UL,          0UL};
// "template" "typedef" SEMICOLON "enum" ID LCURLY "inline" "friend" "namespace"
// "antlrTrace_on" "antlrTrace_off" RCURLY "extern" StringLiteral "struct"
// "union" "class" "_stdcall" "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID"
// "GFUNREAD" "GFARRAYSIZE" LPAREN "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto"
// "register" "static" "mutable" "_inline" "__inline" "virtual" "explicit"
// "typename" "char" "wchar_t" "bool" "short" "int" "_int8" "__int8" "int8_t"
// "_int16" "__int16" "int16_t" "_int32" "__int32" "int32_t" "_int64" "__int64"
// "int64_t" "uint8_t" "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64"
// "long" "signed" "unsigned" "float" "double" "void" "_declspec" "__declspec"
// "const" "__const" "volatile" "__volatile__" "public" "protected" "private"
// "operator" "this" "true" "false" OCTALINT TILDE STAR AMPERSAND "throw"
// "using" "case" "default" "if" "switch" "while" "do" "for" "goto" "continue"
// "break" "return" "try" "asm" "_asm" "__asm" "__asm__" PLUS MINUS PLUSPLUS
// MINUSMINUS "sizeof" "__alignof__" SCOPE "dynamic_cast" "static_cast"
// "reinterpret_cast" "const_cast" "typeid" DECIMALINT HEXADECIMALINT
// CharLiteral
// WCharLiteral WStringLiteral FLOATONE FLOATTWO NOT "new" "_cdecl" "__cdecl"
// "_near" "__near" "_far" "__far" "__interrupt" "pascal" "_pascal" "__pascal"
// "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_37(
    _tokenSet_37_data_, 12);
const unsigned long CPPParser::_tokenSet_38_data_[] = {
    100664320UL, 1UL, 1642070016UL, 0UL, 524288UL, 16368UL,
    0UL,         0UL, 0UL,          0UL, 0UL,      0UL};
// ID "_stdcall" "__stdcall" LPAREN "operator" "this" "true" "false" STAR
// AMPERSAND SCOPE "_cdecl" "__cdecl" "_near" "__near" "_far" "__far"
// "__interrupt"
// "pascal" "_pascal" "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_38(
    _tokenSet_38_data_, 12);
const unsigned long CPPParser::_tokenSet_39_data_[] = {
    100926560UL, 1UL, 1977806848UL, 3220176896UL, 2744319UL, 32764UL,
    0UL,         0UL, 0UL,          0UL,          0UL,       0UL};
// LESSTHAN GREATERTHAN ID ASSIGNEQUAL "_stdcall" "__stdcall" LPAREN "const"
// "__const" "volatile" "__volatile__" COMMA "operator" "this" "true" "false"
// LSQUARE TILDE STAR AMPERSAND TIMESEQUAL DIVIDEEQUAL MINUSEQUAL PLUSEQUAL
// MODEQUAL SHIFTLEFTEQUAL SHIFTRIGHTEQUAL BITWISEANDEQUAL BITWISEXOREQUAL
// BITWISEOREQUAL OR AND BITWISEOR BITWISEXOR NOTEQUAL EQUAL LESSTHANOREQUALTO
// GREATERTHANOREQUALTO SHIFTLEFT SHIFTRIGHT PLUS MINUS DIVIDE MOD POINTERTOMBR
// PLUSPLUS MINUSMINUS SCOPE POINTERTO NOT "new" "_cdecl" "__cdecl" "_near"
// "__near" "_far" "__far" "__interrupt" "pascal" "_pascal" "__pascal"
// "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_39(
    _tokenSet_39_data_, 12);
const unsigned long CPPParser::_tokenSet_40_data_[] = {
    4294180752UL, 4294967293UL, 1945960447UL, 1015807UL, 4291790336UL, 32767UL,
    0UL,          0UL,          0UL,          0UL,       0UL,          0UL};
// "template" "typedef" SEMICOLON "enum" ID LCURLY "inline" "friend" "namespace"
// "antlrTrace_on" "antlrTrace_off" RCURLY "extern" StringLiteral "struct"
// "union" "class" "_stdcall" "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID"
// "GFUNREAD" "GFARRAYSIZE" LPAREN "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto"
// "register" "static" "mutable" "_inline" "__inline" "virtual" "explicit"
// "typename" "char" "wchar_t" "bool" "short" "int" "_int8" "__int8" "int8_t"
// "_int16" "__int16" "int16_t" "_int32" "__int32" "int32_t" "_int64" "__int64"
// "int64_t" "uint8_t" "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64"
// "long" "signed" "unsigned" "float" "double" "void" "_declspec" "__declspec"
// "const" "__const" "volatile" "__volatile__" "public" "protected" "private"
// "operator" "this" "true" "false" OCTALINT TILDE STAR AMPERSAND "throw"
// "using" "case" "default" "if" "else" "switch" "while" "do" "for" "goto"
// "continue" "break" "return" "try" "asm" "_asm" "__asm" "__asm__" PLUS
// MINUS PLUSPLUS MINUSMINUS "sizeof" "__alignof__" SCOPE "dynamic_cast"
// "static_cast" "reinterpret_cast" "const_cast" "typeid" DECIMALINT
// HEXADECIMALINT
// CharLiteral WCharLiteral WStringLiteral FLOATONE FLOATTWO NOT "new"
// "_cdecl" "__cdecl" "_near" "__near" "_far" "__far" "__interrupt" "pascal"
// "_pascal" "__pascal" "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_40(
    _tokenSet_40_data_, 12);
const unsigned long CPPParser::_tokenSet_41_data_[] = {
    101453120UL, 3UL, 4125229056UL, 0UL, 524288UL, 16368UL,
    0UL,         0UL, 0UL,          0UL, 0UL,      0UL};
// GREATERTHAN SEMICOLON ID LCURLY ASSIGNEQUAL COLON "_stdcall" "__stdcall"
// LPAREN RPAREN COMMA "operator" "this" "true" "false" LSQUARE TILDE STAR
// AMPERSAND ELLIPSIS SCOPE "_cdecl" "__cdecl" "_near" "__near" "_far"
// "__far" "__interrupt" "pascal" "_pascal" "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_41(
    _tokenSet_41_data_, 12);
const unsigned long CPPParser::_tokenSet_42_data_[] = {
    5120UL, 1792UL, 268435456UL, 0UL, 524288UL, 0UL,
    0UL,    0UL,    0UL,         0UL, 0UL,      0UL};
// ID "inline" "_inline" "__inline" "virtual" TILDE SCOPE
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_42(
    _tokenSet_42_data_, 12);
const unsigned long CPPParser::_tokenSet_43_data_[] = {
    4291835520UL, 4294967292UL, 2162687UL, 0UL, 524288UL, 0UL,
    0UL,          0UL,          0UL,       0UL, 0UL,      0UL};
// "typedef" "enum" ID "inline" "friend" "extern" "struct" "union" "class"
// "_stdcall" "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID" "GFUNREAD"
// "GFARRAYSIZE"
// "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto" "register" "static" "mutable"
// "_inline" "__inline" "virtual" "explicit" "typename" "char" "wchar_t"
// "bool" "short" "int" "_int8" "__int8" "int8_t" "_int16" "__int16" "int16_t"
// "_int32" "__int32" "int32_t" "_int64" "__int64" "int64_t" "uint8_t"
// "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64" "long" "signed" "unsigned"
// "float" "double" "void" "_declspec" "__declspec" "const" "__const" "volatile"
// "__volatile__" "operator" SCOPE
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_43(
    _tokenSet_43_data_, 12);
const unsigned long CPPParser::_tokenSet_44_data_[] = {
    1024UL, 0UL, 31457280UL, 0UL, 524288UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// ID "operator" "this" "true" "false" SCOPE
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_44(
    _tokenSet_44_data_, 12);
const unsigned long CPPParser::_tokenSet_45_data_[] = {
    263520UL, 1UL, 1977745408UL, 3220176896UL, 2744319UL, 16396UL,
    0UL,      0UL, 0UL,          0UL,          0UL,       0UL};
// LESSTHAN GREATERTHAN SEMICOLON ID ASSIGNEQUAL LPAREN COMMA "operator"
// "this" "true" "false" LSQUARE TILDE STAR AMPERSAND TIMESEQUAL DIVIDEEQUAL
// MINUSEQUAL PLUSEQUAL MODEQUAL SHIFTLEFTEQUAL SHIFTRIGHTEQUAL BITWISEANDEQUAL
// BITWISEXOREQUAL BITWISEOREQUAL OR AND BITWISEOR BITWISEXOR NOTEQUAL
// EQUAL LESSTHANOREQUALTO GREATERTHANOREQUALTO SHIFTLEFT SHIFTRIGHT PLUS
// MINUS DIVIDE MOD POINTERTOMBR PLUSPLUS MINUSMINUS SCOPE POINTERTO NOT
// "new" "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_45(
    _tokenSet_45_data_, 12);
const unsigned long CPPParser::_tokenSet_46_data_[] = {
    100668416UL, 3841UL, 1642070016UL, 0UL, 524288UL, 16368UL,
    0UL,         0UL,    0UL,          0UL, 0UL,      0UL};
// ID "inline" "_stdcall" "__stdcall" LPAREN "_inline" "__inline" "virtual"
// "explicit" "operator" "this" "true" "false" STAR AMPERSAND SCOPE "_cdecl"
// "__cdecl" "_near" "__near" "_far" "__far" "__interrupt" "pascal" "_pascal"
// "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_46(
    _tokenSet_46_data_, 12);
const unsigned long CPPParser::_tokenSet_47_data_[] = {
    100930656UL, 3841UL, 1977806848UL, 3220176896UL, 2744319UL, 32764UL,
    0UL,         0UL,    0UL,          0UL,          0UL,       0UL};
// LESSTHAN GREATERTHAN ID "inline" ASSIGNEQUAL "_stdcall" "__stdcall"
// LPAREN "_inline" "__inline" "virtual" "explicit" "const" "__const" "volatile"
// "__volatile__" COMMA "operator" "this" "true" "false" LSQUARE TILDE
// STAR AMPERSAND TIMESEQUAL DIVIDEEQUAL MINUSEQUAL PLUSEQUAL MODEQUAL
// SHIFTLEFTEQUAL SHIFTRIGHTEQUAL BITWISEANDEQUAL BITWISEXOREQUAL BITWISEOREQUAL
// OR AND BITWISEOR BITWISEXOR NOTEQUAL EQUAL LESSTHANOREQUALTO
// GREATERTHANOREQUALTO
// SHIFTLEFT SHIFTRIGHT PLUS MINUS DIVIDE MOD POINTERTOMBR PLUSPLUS MINUSMINUS
// SCOPE POINTERTO NOT "new" "_cdecl" "__cdecl" "_near" "__near" "_far"
// "__far" "__interrupt" "pascal" "_pascal" "__pascal" "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_47(
    _tokenSet_47_data_, 12);
const unsigned long CPPParser::_tokenSet_48_data_[] = {526592UL, 0UL, 0UL, 0UL,
                                                       0UL,      0UL, 0UL, 0UL};
// SEMICOLON LCURLY COLON
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_48(
    _tokenSet_48_data_, 8);
const unsigned long CPPParser::_tokenSet_49_data_[] = {
    4294049680UL, 4294967293UL, 1945960447UL, 1015775UL, 4291790336UL, 32767UL,
    0UL,          0UL,          0UL,          0UL,       0UL,          0UL};
// "template" "typedef" SEMICOLON "enum" ID LCURLY "inline" "friend" "namespace"
// "antlrTrace_on" "antlrTrace_off" "extern" StringLiteral "struct" "union"
// "class" "_stdcall" "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID" "GFUNREAD"
// "GFARRAYSIZE" LPAREN "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto" "register"
// "static" "mutable" "_inline" "__inline" "virtual" "explicit" "typename"
// "char" "wchar_t" "bool" "short" "int" "_int8" "__int8" "int8_t" "_int16"
// "__int16" "int16_t" "_int32" "__int32" "int32_t" "_int64" "__int64"
// "int64_t" "uint8_t" "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64"
// "long" "signed" "unsigned" "float" "double" "void" "_declspec" "__declspec"
// "const" "__const" "volatile" "__volatile__" "public" "protected" "private"
// "operator" "this" "true" "false" OCTALINT TILDE STAR AMPERSAND "throw"
// "using" "case" "default" "if" "switch" "while" "do" "for" "goto" "continue"
// "break" "return" "try" "asm" "_asm" "__asm" "__asm__" PLUS MINUS PLUSPLUS
// MINUSMINUS "sizeof" "__alignof__" SCOPE "dynamic_cast" "static_cast"
// "reinterpret_cast" "const_cast" "typeid" DECIMALINT HEXADECIMALINT
// CharLiteral
// WCharLiteral WStringLiteral FLOATONE FLOATTWO NOT "new" "_cdecl" "__cdecl"
// "_near" "__near" "_far" "__far" "__interrupt" "pascal" "_pascal" "__pascal"
// "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_49(
    _tokenSet_49_data_, 12);
const unsigned long CPPParser::_tokenSet_50_data_[] = {
    4294180754UL, 4294967293UL, 1945960447UL, 1048575UL, 4291790336UL, 32767UL,
    0UL,          0UL,          0UL,          0UL,       0UL,          0UL};
// EOF "template" "typedef" SEMICOLON "enum" ID LCURLY "inline" "friend"
// "namespace" "antlrTrace_on" "antlrTrace_off" RCURLY "extern" StringLiteral
// "struct" "union" "class" "_stdcall" "__stdcall" "GFEXCLUDE" "GFINCLUDE"
// "GFID" "GFUNREAD" "GFARRAYSIZE" LPAREN "GFARRAYSIZES" "GFARRAYELEMSIZE"
// "auto" "register" "static" "mutable" "_inline" "__inline" "virtual"
// "explicit" "typename" "char" "wchar_t" "bool" "short" "int" "_int8"
// "__int8" "int8_t" "_int16" "__int16" "int16_t" "_int32" "__int32" "int32_t"
// "_int64" "__int64" "int64_t" "uint8_t" "uint16_t" "uint32_t" "uint64_t"
// "_w64" "__w64" "long" "signed" "unsigned" "float" "double" "void" "_declspec"
// "__declspec" "const" "__const" "volatile" "__volatile__" "public" "protected"
// "private" "operator" "this" "true" "false" OCTALINT TILDE STAR AMPERSAND
// "throw" "using" "case" "default" "if" "else" "switch" "while" "do" "for"
// "goto" "continue" "break" "return" "try" "catch" "asm" "_asm" "__asm"
// "__asm__" PLUS MINUS PLUSPLUS MINUSMINUS "sizeof" "__alignof__" SCOPE
// "dynamic_cast" "static_cast" "reinterpret_cast" "const_cast" "typeid"
// DECIMALINT HEXADECIMALINT CharLiteral WCharLiteral WStringLiteral FLOATONE
// FLOATTWO NOT "new" "_cdecl" "__cdecl" "_near" "__near" "_far" "__far"
// "__interrupt" "pascal" "_pascal" "__pascal" "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_50(
    _tokenSet_50_data_, 12);
const unsigned long CPPParser::_tokenSet_51_data_[] = {
    525312UL, 1024UL, 0UL, 0UL, 524288UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// ID COLON "virtual" SCOPE
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_51(
    _tokenSet_51_data_, 12);
const unsigned long CPPParser::_tokenSet_52_data_[] = {64UL, 0UL, 0UL, 0UL,
                                                       0UL,  0UL, 0UL, 0UL};
// GREATERTHAN
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_52(
    _tokenSet_52_data_, 8);
const unsigned long CPPParser::_tokenSet_53_data_[] = {2048UL, 0UL, 0UL, 0UL,
                                                       0UL,    0UL, 0UL, 0UL};
// LCURLY
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_53(
    _tokenSet_53_data_, 8);
const unsigned long CPPParser::_tokenSet_54_data_[] = {
    4292755424UL, 4294967295UL, 4259512319UL, 4293918723UL, 557055UL, 16368UL,
    0UL,          0UL,          0UL,          0UL,          0UL,      0UL};
// LESSTHAN GREATERTHAN "typedef" SEMICOLON "enum" ID LCURLY "inline" "friend"
// RCURLY ASSIGNEQUAL COLON "extern" "struct" "union" "class" "_stdcall"
// "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID" "GFUNREAD" "GFARRAYSIZE"
// LPAREN RPAREN "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto" "register" "static"
// "mutable" "_inline" "__inline" "virtual" "explicit" "typename" "char"
// "wchar_t" "bool" "short" "int" "_int8" "__int8" "int8_t" "_int16" "__int16"
// "int16_t" "_int32" "__int32" "int32_t" "_int64" "__int64" "int64_t"
// "uint8_t" "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64" "long" "signed"
// "unsigned" "float" "double" "void" "_declspec" "__declspec" "const"
// "__const" "volatile" "__volatile__" COMMA "operator" "this" "true" "false"
// LSQUARE RSQUARE TILDE STAR AMPERSAND ELLIPSIS "throw" "using" TIMESEQUAL
// DIVIDEEQUAL MINUSEQUAL PLUSEQUAL MODEQUAL SHIFTLEFTEQUAL SHIFTRIGHTEQUAL
// BITWISEANDEQUAL BITWISEXOREQUAL BITWISEOREQUAL QUESTIONMARK OR AND BITWISEOR
// BITWISEXOR NOTEQUAL EQUAL LESSTHANOREQUALTO GREATERTHANOREQUALTO SHIFTLEFT
// SHIFTRIGHT PLUS MINUS DIVIDE MOD DOTMBR POINTERTOMBR SCOPE "_cdecl"
// "__cdecl" "_near" "__near" "_far" "__far" "__interrupt" "pascal" "_pascal"
// "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_54(
    _tokenSet_54_data_, 12);
const unsigned long CPPParser::_tokenSet_55_data_[] = {
    4293932672UL, 4294967293UL, 1944125439UL, 0UL, 4291790336UL, 16399UL,
    0UL,          0UL,          0UL,          0UL, 0UL,          0UL};
// "typedef" "enum" ID "inline" "friend" "extern" StringLiteral "struct"
// "union" "class" "_stdcall" "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID"
// "GFUNREAD" "GFARRAYSIZE" LPAREN "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto"
// "register" "static" "mutable" "_inline" "__inline" "virtual" "explicit"
// "typename" "char" "wchar_t" "bool" "short" "int" "_int8" "__int8" "int8_t"
// "_int16" "__int16" "int16_t" "_int32" "__int32" "int32_t" "_int64" "__int64"
// "int64_t" "uint8_t" "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64"
// "long" "signed" "unsigned" "float" "double" "void" "_declspec" "__declspec"
// "const" "__const" "volatile" "__volatile__" "operator" "this" "true"
// "false" OCTALINT TILDE STAR AMPERSAND PLUS MINUS PLUSPLUS MINUSMINUS
// "sizeof" "__alignof__" SCOPE "dynamic_cast" "static_cast" "reinterpret_cast"
// "const_cast" "typeid" DECIMALINT HEXADECIMALINT CharLiteral WCharLiteral
// WStringLiteral FLOATONE FLOATTWO NOT "new" "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_55(
    _tokenSet_55_data_, 12);
const unsigned long CPPParser::_tokenSet_56_data_[] = {
    101584224UL, 3UL, 4259446784UL, 4293918720UL, 557055UL, 16368UL,
    0UL,         0UL, 0UL,          0UL,          0UL,      0UL};
// LESSTHAN GREATERTHAN SEMICOLON ID LCURLY RCURLY ASSIGNEQUAL COLON "_stdcall"
// "__stdcall" LPAREN RPAREN COMMA "operator" "this" "true" "false" LSQUARE
// RSQUARE TILDE STAR AMPERSAND ELLIPSIS TIMESEQUAL DIVIDEEQUAL MINUSEQUAL
// PLUSEQUAL MODEQUAL SHIFTLEFTEQUAL SHIFTRIGHTEQUAL BITWISEANDEQUAL
// BITWISEXOREQUAL
// BITWISEOREQUAL QUESTIONMARK OR AND BITWISEOR BITWISEXOR NOTEQUAL EQUAL
// LESSTHANOREQUALTO GREATERTHANOREQUALTO SHIFTLEFT SHIFTRIGHT PLUS MINUS
// DIVIDE MOD DOTMBR POINTERTOMBR SCOPE "_cdecl" "__cdecl" "_near" "__near"
// "_far" "__far" "__interrupt" "pascal" "_pascal" "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_56(
    _tokenSet_56_data_, 12);
const unsigned long CPPParser::_tokenSet_57_data_[] = {
    4294967282UL, 4294967295UL, 4294967295UL, 4294934527UL,
    4294967295UL, 32767UL,      0UL,          0UL,
    0UL,          0UL,          0UL,          0UL};
// EOF "template" LESSTHAN GREATERTHAN "typedef" SEMICOLON "enum" ID LCURLY
// "inline" "friend" "namespace" "antlrTrace_on" "antlrTrace_off" RCURLY
// ASSIGNEQUAL COLON "extern" StringLiteral "struct" "union" "class" "_stdcall"
// "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID" "GFUNREAD" "GFARRAYSIZE"
// LPAREN RPAREN "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto" "register" "static"
// "mutable" "_inline" "__inline" "virtual" "explicit" "typename" "char"
// "wchar_t" "bool" "short" "int" "_int8" "__int8" "int8_t" "_int16" "__int16"
// "int16_t" "_int32" "__int32" "int32_t" "_int64" "__int64" "int64_t"
// "uint8_t" "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64" "long" "signed"
// "unsigned" "float" "double" "void" "_declspec" "__declspec" "const"
// "__const" "volatile" "__volatile__" "GFIGNORE" COMMA "public" "protected"
// "private" "operator" "this" "true" "false" OCTALINT LSQUARE RSQUARE
// TILDE STAR AMPERSAND ELLIPSIS "throw" "using" "case" "default" "if"
// "else" "switch" "while" "do" "for" "goto" "continue" "break" "return"
// "try" "asm" "_asm" "__asm" "__asm__" TIMESEQUAL DIVIDEEQUAL MINUSEQUAL
// PLUSEQUAL MODEQUAL SHIFTLEFTEQUAL SHIFTRIGHTEQUAL BITWISEANDEQUAL
// BITWISEXOREQUAL
// BITWISEOREQUAL QUESTIONMARK OR AND BITWISEOR BITWISEXOR NOTEQUAL EQUAL
// LESSTHANOREQUALTO GREATERTHANOREQUALTO SHIFTLEFT SHIFTRIGHT PLUS MINUS
// DIVIDE MOD DOTMBR POINTERTOMBR PLUSPLUS MINUSMINUS "sizeof" "__alignof__"
// SCOPE DOT POINTERTO "dynamic_cast" "static_cast" "reinterpret_cast"
// "const_cast" "typeid" DECIMALINT HEXADECIMALINT CharLiteral WCharLiteral
// WStringLiteral FLOATONE FLOATTWO NOT "new" "_cdecl" "__cdecl" "_near"
// "__near" "_far" "__far" "__interrupt" "pascal" "_pascal" "__pascal"
// "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_57(
    _tokenSet_57_data_, 12);
const unsigned long CPPParser::_tokenSet_58_data_[] = {
    3072UL, 0UL, 31522816UL, 0UL, 524288UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// ID LCURLY "GFIGNORE" "operator" "this" "true" "false" SCOPE
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_58(
    _tokenSet_58_data_, 12);
const unsigned long CPPParser::_tokenSet_59_data_[] = {
    524544UL, 2UL, 134217728UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// SEMICOLON COLON RPAREN RSQUARE
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_59(
    _tokenSet_59_data_, 8);
const unsigned long CPPParser::_tokenSet_60_data_[] = {
    2048UL, 0UL, 131072UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// LCURLY COMMA
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_60(
    _tokenSet_60_data_, 8);
const unsigned long CPPParser::_tokenSet_61_data_[] = {131072UL, 0UL, 0UL, 0UL,
                                                       0UL,      0UL, 0UL, 0UL};
// RCURLY
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_61(
    _tokenSet_61_data_, 8);
const unsigned long CPPParser::_tokenSet_62_data_[] = {
    131072UL, 0UL, 131072UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// RCURLY COMMA
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_62(
    _tokenSet_62_data_, 8);
const unsigned long CPPParser::_tokenSet_63_data_[] = {
    917760UL, 0UL, 134348800UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// SEMICOLON RCURLY ASSIGNEQUAL COLON COMMA RSQUARE
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_63(
    _tokenSet_63_data_, 8);
const unsigned long CPPParser::_tokenSet_64_data_[] = {
    101584224UL, 3UL, 4259446784UL, 4293918720UL, 3801087UL, 16368UL,
    0UL,         0UL, 0UL,          0UL,          0UL,       0UL};
// LESSTHAN GREATERTHAN SEMICOLON ID LCURLY RCURLY ASSIGNEQUAL COLON "_stdcall"
// "__stdcall" LPAREN RPAREN COMMA "operator" "this" "true" "false" LSQUARE
// RSQUARE TILDE STAR AMPERSAND ELLIPSIS TIMESEQUAL DIVIDEEQUAL MINUSEQUAL
// PLUSEQUAL MODEQUAL SHIFTLEFTEQUAL SHIFTRIGHTEQUAL BITWISEANDEQUAL
// BITWISEXOREQUAL
// BITWISEOREQUAL QUESTIONMARK OR AND BITWISEOR BITWISEXOR NOTEQUAL EQUAL
// LESSTHANOREQUALTO GREATERTHANOREQUALTO SHIFTLEFT SHIFTRIGHT PLUS MINUS
// DIVIDE MOD DOTMBR POINTERTOMBR PLUSPLUS MINUSMINUS SCOPE DOT POINTERTO
// "_cdecl" "__cdecl" "_near" "__near" "_far" "__far" "__interrupt" "pascal"
// "_pascal" "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_64(
    _tokenSet_64_data_, 12);
const unsigned long CPPParser::_tokenSet_65_data_[] = {
    256UL, 0UL, 131072UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// SEMICOLON COMMA
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_65(
    _tokenSet_65_data_, 8);
const unsigned long CPPParser::_tokenSet_66_data_[] = {
    100664352UL, 1UL, 1910566912UL, 0UL, 524288UL, 16368UL,
    0UL,         0UL, 0UL,          0UL, 0UL,      0UL};
// LESSTHAN ID "_stdcall" "__stdcall" LPAREN "const" "__const" "volatile"
// "__volatile__" "operator" "this" "true" "false" TILDE STAR AMPERSAND
// SCOPE "_cdecl" "__cdecl" "_near" "__near" "_far" "__far" "__interrupt"
// "pascal" "_pascal" "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_66(
    _tokenSet_66_data_, 12);
const unsigned long CPPParser::_tokenSet_67_data_[] = {
    1024UL, 1UL, 299892736UL, 0UL, 524288UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// ID LPAREN "operator" "this" "true" "false" TILDE SCOPE
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_67(
    _tokenSet_67_data_, 12);
const unsigned long CPPParser::_tokenSet_68_data_[] = {
    100926816UL, 3UL, 4125229056UL, 3220176896UL, 2744319UL, 32764UL,
    0UL,         0UL, 0UL,          0UL,          0UL,       0UL};
// LESSTHAN GREATERTHAN SEMICOLON ID ASSIGNEQUAL "_stdcall" "__stdcall"
// LPAREN RPAREN COMMA "operator" "this" "true" "false" LSQUARE TILDE STAR
// AMPERSAND ELLIPSIS TIMESEQUAL DIVIDEEQUAL MINUSEQUAL PLUSEQUAL MODEQUAL
// SHIFTLEFTEQUAL SHIFTRIGHTEQUAL BITWISEANDEQUAL BITWISEXOREQUAL BITWISEOREQUAL
// OR AND BITWISEOR BITWISEXOR NOTEQUAL EQUAL LESSTHANOREQUALTO
// GREATERTHANOREQUALTO
// SHIFTLEFT SHIFTRIGHT PLUS MINUS DIVIDE MOD POINTERTOMBR PLUSPLUS MINUSMINUS
// SCOPE POINTERTO NOT "new" "_cdecl" "__cdecl" "_near" "__near" "_far"
// "__far" "__interrupt" "pascal" "_pascal" "__pascal" "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_68(
    _tokenSet_68_data_, 12);
const unsigned long CPPParser::_tokenSet_69_data_[] = {
    262464UL, 3UL, 2147614720UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// GREATERTHAN SEMICOLON ASSIGNEQUAL LPAREN RPAREN COMMA ELLIPSIS
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_69(
    _tokenSet_69_data_, 8);
const unsigned long CPPParser::_tokenSet_70_data_[] = {
    131328UL, 0UL, 131072UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// SEMICOLON RCURLY COMMA
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_70(
    _tokenSet_70_data_, 8);
const unsigned long CPPParser::_tokenSet_71_data_[] = {0UL, 2UL, 0UL, 0UL,
                                                       0UL, 0UL, 0UL, 0UL};
// RPAREN
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_71(
    _tokenSet_71_data_, 8);
const unsigned long CPPParser::_tokenSet_72_data_[] = {
    2098176UL, 4294963201UL, 1944063999UL, 0UL, 4291790336UL, 16399UL,
    0UL,       0UL,          0UL,          0UL, 0UL,          0UL};
// ID StringLiteral LPAREN "typename" "char" "wchar_t" "bool" "short" "int"
// "_int8" "__int8" "int8_t" "_int16" "__int16" "int16_t" "_int32" "__int32"
// "int32_t" "_int64" "__int64" "int64_t" "uint8_t" "uint16_t" "uint32_t"
// "uint64_t" "_w64" "__w64" "long" "signed" "unsigned" "float" "double"
// "void" "_declspec" "__declspec" "operator" "this" "true" "false" OCTALINT
// TILDE STAR AMPERSAND PLUS MINUS PLUSPLUS MINUSMINUS "sizeof" "__alignof__"
// SCOPE "dynamic_cast" "static_cast" "reinterpret_cast" "const_cast" "typeid"
// DECIMALINT HEXADECIMALINT CharLiteral WCharLiteral WStringLiteral FLOATONE
// FLOATTWO NOT "new" "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_72(
    _tokenSet_72_data_, 12);
const unsigned long CPPParser::_tokenSet_73_data_[] = {
    4294850528UL, 4294967295UL, 4293066751UL, 4293918720UL,
    4294967295UL, 16399UL,      0UL,          0UL,
    0UL,          0UL,          0UL,          0UL};
// LESSTHAN GREATERTHAN "typedef" SEMICOLON "enum" ID "inline" "friend"
// RCURLY ASSIGNEQUAL COLON "extern" StringLiteral "struct" "union" "class"
// "_stdcall" "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID" "GFUNREAD"
// "GFARRAYSIZE"
// LPAREN RPAREN "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto" "register" "static"
// "mutable" "_inline" "__inline" "virtual" "explicit" "typename" "char"
// "wchar_t" "bool" "short" "int" "_int8" "__int8" "int8_t" "_int16" "__int16"
// "int16_t" "_int32" "__int32" "int32_t" "_int64" "__int64" "int64_t"
// "uint8_t" "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64" "long" "signed"
// "unsigned" "float" "double" "void" "_declspec" "__declspec" "const"
// "__const" "volatile" "__volatile__" COMMA "operator" "this" "true" "false"
// OCTALINT LSQUARE RSQUARE TILDE STAR AMPERSAND ELLIPSIS TIMESEQUAL DIVIDEEQUAL
// MINUSEQUAL PLUSEQUAL MODEQUAL SHIFTLEFTEQUAL SHIFTRIGHTEQUAL BITWISEANDEQUAL
// BITWISEXOREQUAL BITWISEOREQUAL QUESTIONMARK OR AND BITWISEOR BITWISEXOR
// NOTEQUAL EQUAL LESSTHANOREQUALTO GREATERTHANOREQUALTO SHIFTLEFT SHIFTRIGHT
// PLUS MINUS DIVIDE MOD DOTMBR POINTERTOMBR PLUSPLUS MINUSMINUS "sizeof"
// "__alignof__" SCOPE DOT POINTERTO "dynamic_cast" "static_cast"
// "reinterpret_cast"
// "const_cast" "typeid" DECIMALINT HEXADECIMALINT CharLiteral WCharLiteral
// WStringLiteral FLOATONE FLOATTWO NOT "new" "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_73(
    _tokenSet_73_data_, 12);
const unsigned long CPPParser::_tokenSet_74_data_[] = {
    655680UL, 2UL, 2281832448UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// GREATERTHAN SEMICOLON RCURLY COLON RPAREN COMMA RSQUARE ELLIPSIS
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_74(
    _tokenSet_74_data_, 8);
const unsigned long CPPParser::_tokenSet_75_data_[] = {
    2622464UL, 4294963201UL, 1944063999UL, 0UL, 4291790336UL, 16399UL,
    0UL,       0UL,          0UL,          0UL, 0UL,          0UL};
// ID COLON StringLiteral LPAREN "typename" "char" "wchar_t" "bool" "short"
// "int" "_int8" "__int8" "int8_t" "_int16" "__int16" "int16_t" "_int32"
// "__int32" "int32_t" "_int64" "__int64" "int64_t" "uint8_t" "uint16_t"
// "uint32_t" "uint64_t" "_w64" "__w64" "long" "signed" "unsigned" "float"
// "double" "void" "_declspec" "__declspec" "operator" "this" "true" "false"
// OCTALINT TILDE STAR AMPERSAND PLUS MINUS PLUSPLUS MINUSMINUS "sizeof"
// "__alignof__" SCOPE "dynamic_cast" "static_cast" "reinterpret_cast"
// "const_cast" "typeid" DECIMALINT HEXADECIMALINT CharLiteral WCharLiteral
// WStringLiteral FLOATONE FLOATTWO NOT "new" "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_75(
    _tokenSet_75_data_, 12);
const unsigned long CPPParser::_tokenSet_76_data_[] = {
    100664320UL, 1UL, 1910505472UL, 0UL, 524288UL, 16368UL,
    0UL,         0UL, 0UL,          0UL, 0UL,      0UL};
// ID "_stdcall" "__stdcall" LPAREN "operator" "this" "true" "false" TILDE
// STAR AMPERSAND SCOPE "_cdecl" "__cdecl" "_near" "__near" "_far" "__far"
// "__interrupt" "pascal" "_pascal" "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_76(
    _tokenSet_76_data_, 12);
const unsigned long CPPParser::_tokenSet_77_data_[] = {
    100926816UL, 1UL, 1977806848UL, 3220176896UL, 2744319UL, 32764UL,
    0UL,         0UL, 0UL,          0UL,          0UL,       0UL};
// LESSTHAN GREATERTHAN SEMICOLON ID ASSIGNEQUAL "_stdcall" "__stdcall"
// LPAREN "const" "__const" "volatile" "__volatile__" COMMA "operator"
// "this" "true" "false" LSQUARE TILDE STAR AMPERSAND TIMESEQUAL DIVIDEEQUAL
// MINUSEQUAL PLUSEQUAL MODEQUAL SHIFTLEFTEQUAL SHIFTRIGHTEQUAL BITWISEANDEQUAL
// BITWISEXOREQUAL BITWISEOREQUAL OR AND BITWISEOR BITWISEXOR NOTEQUAL
// EQUAL LESSTHANOREQUALTO GREATERTHANOREQUALTO SHIFTLEFT SHIFTRIGHT PLUS
// MINUS DIVIDE MOD POINTERTOMBR PLUSPLUS MINUSMINUS SCOPE POINTERTO NOT
// "new" "_cdecl" "__cdecl" "_near" "__near" "_far" "__far" "__interrupt"
// "pascal" "_pascal" "__pascal" "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_77(
    _tokenSet_77_data_, 12);
const unsigned long CPPParser::_tokenSet_78_data_[] = {
    262400UL, 0UL, 131072UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// SEMICOLON ASSIGNEQUAL COMMA
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_78(
    _tokenSet_78_data_, 8);
const unsigned long CPPParser::_tokenSet_79_data_[] = {
    4293932672UL, 4294967295UL, 4225826815UL, 0UL, 4291790336UL, 32767UL,
    0UL,          0UL,          0UL,          0UL, 0UL,          0UL};
// "typedef" "enum" ID "inline" "friend" "extern" StringLiteral "struct"
// "union" "class" "_stdcall" "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID"
// "GFUNREAD" "GFARRAYSIZE" LPAREN RPAREN "GFARRAYSIZES" "GFARRAYELEMSIZE"
// "auto" "register" "static" "mutable" "_inline" "__inline" "virtual"
// "explicit" "typename" "char" "wchar_t" "bool" "short" "int" "_int8"
// "__int8" "int8_t" "_int16" "__int16" "int16_t" "_int32" "__int32" "int32_t"
// "_int64" "__int64" "int64_t" "uint8_t" "uint16_t" "uint32_t" "uint64_t"
// "_w64" "__w64" "long" "signed" "unsigned" "float" "double" "void" "_declspec"
// "__declspec" "const" "__const" "volatile" "__volatile__" "operator"
// "this" "true" "false" OCTALINT RSQUARE TILDE STAR AMPERSAND ELLIPSIS
// PLUS MINUS PLUSPLUS MINUSMINUS "sizeof" "__alignof__" SCOPE "dynamic_cast"
// "static_cast" "reinterpret_cast" "const_cast" "typeid" DECIMALINT
// HEXADECIMALINT
// CharLiteral WCharLiteral WStringLiteral FLOATONE FLOATTWO NOT "new"
// "_cdecl" "__cdecl" "_near" "__near" "_far" "__far" "__interrupt" "pascal"
// "_pascal" "__pascal" "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_79(
    _tokenSet_79_data_, 12);
const unsigned long CPPParser::_tokenSet_80_data_[] = {
    4294967250UL, 4294967295UL, 4160684031UL, 1015807UL, 4291790336UL, 32767UL,
    0UL,          0UL,          0UL,          0UL,       0UL,          0UL};
// EOF "template" GREATERTHAN "typedef" SEMICOLON "enum" ID LCURLY "inline"
// "friend" "namespace" "antlrTrace_on" "antlrTrace_off" RCURLY ASSIGNEQUAL
// COLON "extern" StringLiteral "struct" "union" "class" "_stdcall" "__stdcall"
// "GFEXCLUDE" "GFINCLUDE" "GFID" "GFUNREAD" "GFARRAYSIZE" LPAREN RPAREN
// "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto" "register" "static" "mutable"
// "_inline" "__inline" "virtual" "explicit" "typename" "char" "wchar_t"
// "bool" "short" "int" "_int8" "__int8" "int8_t" "_int16" "__int16" "int16_t"
// "_int32" "__int32" "int32_t" "_int64" "__int64" "int64_t" "uint8_t"
// "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64" "long" "signed" "unsigned"
// "float" "double" "void" "_declspec" "__declspec" "const" "__const" "volatile"
// "__volatile__" COMMA "public" "protected" "private" "operator" "this"
// "true" "false" OCTALINT LSQUARE TILDE STAR AMPERSAND ELLIPSIS "throw"
// "using" "case" "default" "if" "else" "switch" "while" "do" "for" "goto"
// "continue" "break" "return" "try" "asm" "_asm" "__asm" "__asm__" PLUS
// MINUS PLUSPLUS MINUSMINUS "sizeof" "__alignof__" SCOPE "dynamic_cast"
// "static_cast" "reinterpret_cast" "const_cast" "typeid" DECIMALINT
// HEXADECIMALINT
// CharLiteral WCharLiteral WStringLiteral FLOATONE FLOATTWO NOT "new"
// "_cdecl" "__cdecl" "_near" "__near" "_far" "__far" "__interrupt" "pascal"
// "_pascal" "__pascal" "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_80(
    _tokenSet_80_data_, 12);
const unsigned long CPPParser::_tokenSet_81_data_[] = {
    263520UL, 3UL, 4125229056UL, 3220176896UL, 2744319UL, 16396UL,
    0UL,      0UL, 0UL,          0UL,          0UL,       0UL};
// LESSTHAN GREATERTHAN SEMICOLON ID ASSIGNEQUAL LPAREN RPAREN COMMA "operator"
// "this" "true" "false" LSQUARE TILDE STAR AMPERSAND ELLIPSIS TIMESEQUAL
// DIVIDEEQUAL MINUSEQUAL PLUSEQUAL MODEQUAL SHIFTLEFTEQUAL SHIFTRIGHTEQUAL
// BITWISEANDEQUAL BITWISEXOREQUAL BITWISEOREQUAL OR AND BITWISEOR BITWISEXOR
// NOTEQUAL EQUAL LESSTHANOREQUALTO GREATERTHANOREQUALTO SHIFTLEFT SHIFTRIGHT
// PLUS MINUS DIVIDE MOD POINTERTOMBR PLUSPLUS MINUSMINUS SCOPE POINTERTO
// NOT "new" "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_81(
    _tokenSet_81_data_, 12);
const unsigned long CPPParser::_tokenSet_82_data_[] = {
    4292624320UL, 4294967295UL, 2147680255UL, 2UL, 524288UL, 0UL,
    0UL,          0UL,          0UL,          0UL, 0UL,      0UL};
// GREATERTHAN "typedef" SEMICOLON "enum" ID LCURLY "inline" "friend"
// ASSIGNEQUAL
// COLON "extern" "struct" "union" "class" "_stdcall" "__stdcall" "GFEXCLUDE"
// "GFINCLUDE" "GFID" "GFUNREAD" "GFARRAYSIZE" LPAREN RPAREN "GFARRAYSIZES"
// "GFARRAYELEMSIZE" "auto" "register" "static" "mutable" "_inline" "__inline"
// "virtual" "explicit" "typename" "char" "wchar_t" "bool" "short" "int"
// "_int8" "__int8" "int8_t" "_int16" "__int16" "int16_t" "_int32" "__int32"
// "int32_t" "_int64" "__int64" "int64_t" "uint8_t" "uint16_t" "uint32_t"
// "uint64_t" "_w64" "__w64" "long" "signed" "unsigned" "float" "double"
// "void" "_declspec" "__declspec" "const" "__const" "volatile" "__volatile__"
// COMMA ELLIPSIS "using" SCOPE
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_82(
    _tokenSet_82_data_, 12);
const unsigned long CPPParser::_tokenSet_83_data_[] = {
    4292099968UL, 4294967292UL, 65535UL, 3UL, 524288UL, 0UL,
    0UL,          0UL,          0UL,     0UL, 0UL,      0UL};
// "typedef" SEMICOLON "enum" ID LCURLY "inline" "friend" ASSIGNEQUAL "extern"
// "struct" "union" "class" "_stdcall" "__stdcall" "GFEXCLUDE" "GFINCLUDE"
// "GFID" "GFUNREAD" "GFARRAYSIZE" "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto"
// "register" "static" "mutable" "_inline" "__inline" "virtual" "explicit"
// "typename" "char" "wchar_t" "bool" "short" "int" "_int8" "__int8" "int8_t"
// "_int16" "__int16" "int16_t" "_int32" "__int32" "int32_t" "_int64" "__int64"
// "int64_t" "uint8_t" "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64"
// "long" "signed" "unsigned" "float" "double" "void" "_declspec" "__declspec"
// "const" "__const" "volatile" "__volatile__" "throw" "using" SCOPE
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_83(
    _tokenSet_83_data_, 12);
const unsigned long CPPParser::_tokenSet_84_data_[] = {526336UL, 0UL, 0UL, 0UL,
                                                       0UL,      0UL, 0UL, 0UL};
// LCURLY COLON
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_84(
    _tokenSet_84_data_, 8);
const unsigned long CPPParser::_tokenSet_85_data_[] = {0UL, 1UL, 0UL, 0UL,
                                                       0UL, 0UL, 0UL, 0UL};
// LPAREN
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_85(
    _tokenSet_85_data_, 8);
const unsigned long CPPParser::_tokenSet_86_data_[] = {
    1024UL, 0UL, 268435456UL, 0UL, 524288UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// ID TILDE SCOPE
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_86(
    _tokenSet_86_data_, 12);
const unsigned long CPPParser::_tokenSet_87_data_[] = {
    0UL, 2UL, 2147483648UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// RPAREN ELLIPSIS
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_87(
    _tokenSet_87_data_, 8);
const unsigned long CPPParser::_tokenSet_88_data_[] = {
    4292097760UL, 4294967295UL, 4125294591UL, 0UL, 524288UL, 16368UL,
    0UL,          0UL,          0UL,          0UL, 0UL,      0UL};
// LESSTHAN GREATERTHAN "typedef" "enum" ID "inline" "friend" ASSIGNEQUAL
// "extern" "struct" "union" "class" "_stdcall" "__stdcall" "GFEXCLUDE"
// "GFINCLUDE" "GFID" "GFUNREAD" "GFARRAYSIZE" LPAREN RPAREN "GFARRAYSIZES"
// "GFARRAYELEMSIZE" "auto" "register" "static" "mutable" "_inline" "__inline"
// "virtual" "explicit" "typename" "char" "wchar_t" "bool" "short" "int"
// "_int8" "__int8" "int8_t" "_int16" "__int16" "int16_t" "_int32" "__int32"
// "int32_t" "_int64" "__int64" "int64_t" "uint8_t" "uint16_t" "uint32_t"
// "uint64_t" "_w64" "__w64" "long" "signed" "unsigned" "float" "double"
// "void" "_declspec" "__declspec" "const" "__const" "volatile" "__volatile__"
// COMMA "operator" "this" "true" "false" LSQUARE TILDE STAR AMPERSAND
// ELLIPSIS SCOPE "_cdecl" "__cdecl" "_near" "__near" "_far" "__far"
// "__interrupt"
// "pascal" "_pascal" "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_88(
    _tokenSet_88_data_, 12);
const unsigned long CPPParser::_tokenSet_89_data_[] = {
    100926560UL, 3UL, 4125290496UL, 3220176896UL, 2744319UL, 32764UL,
    0UL,         0UL, 0UL,          0UL,          0UL,       0UL};
// LESSTHAN GREATERTHAN ID ASSIGNEQUAL "_stdcall" "__stdcall" LPAREN RPAREN
// "const" "__const" "volatile" "__volatile__" COMMA "operator" "this"
// "true" "false" LSQUARE TILDE STAR AMPERSAND ELLIPSIS TIMESEQUAL DIVIDEEQUAL
// MINUSEQUAL PLUSEQUAL MODEQUAL SHIFTLEFTEQUAL SHIFTRIGHTEQUAL BITWISEANDEQUAL
// BITWISEXOREQUAL BITWISEOREQUAL OR AND BITWISEOR BITWISEXOR NOTEQUAL
// EQUAL LESSTHANOREQUALTO GREATERTHANOREQUALTO SHIFTLEFT SHIFTRIGHT PLUS
// MINUS DIVIDE MOD POINTERTOMBR PLUSPLUS MINUSMINUS SCOPE POINTERTO NOT
// "new" "_cdecl" "__cdecl" "_near" "__near" "_far" "__far" "__interrupt"
// "pascal" "_pascal" "__pascal" "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_89(
    _tokenSet_89_data_, 12);
const unsigned long CPPParser::_tokenSet_90_data_[] = {
    100926528UL, 3UL, 3825336320UL, 0UL, 524288UL, 16368UL,
    0UL,         0UL, 0UL,          0UL, 0UL,      0UL};
// GREATERTHAN ID ASSIGNEQUAL "_stdcall" "__stdcall" LPAREN RPAREN COMMA
// LSQUARE STAR AMPERSAND ELLIPSIS SCOPE "_cdecl" "__cdecl" "_near" "__near"
// "_far" "__far" "__interrupt" "pascal" "_pascal" "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_90(
    _tokenSet_90_data_, 12);
const unsigned long CPPParser::_tokenSet_91_data_[] = {
    4294721520UL, 4294967295UL, 4293066751UL, 3UL, 4291790336UL, 32767UL,
    0UL,          0UL,          0UL,          0UL, 0UL,          0UL};
// "template" LESSTHAN GREATERTHAN "typedef" SEMICOLON "enum" ID LCURLY
// "inline" "friend" ASSIGNEQUAL COLON "extern" StringLiteral "struct"
// "union" "class" "_stdcall" "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID"
// "GFUNREAD" "GFARRAYSIZE" LPAREN RPAREN "GFARRAYSIZES" "GFARRAYELEMSIZE"
// "auto" "register" "static" "mutable" "_inline" "__inline" "virtual"
// "explicit" "typename" "char" "wchar_t" "bool" "short" "int" "_int8"
// "__int8" "int8_t" "_int16" "__int16" "int16_t" "_int32" "__int32" "int32_t"
// "_int64" "__int64" "int64_t" "uint8_t" "uint16_t" "uint32_t" "uint64_t"
// "_w64" "__w64" "long" "signed" "unsigned" "float" "double" "void" "_declspec"
// "__declspec" "const" "__const" "volatile" "__volatile__" COMMA "operator"
// "this" "true" "false" OCTALINT LSQUARE RSQUARE TILDE STAR AMPERSAND
// ELLIPSIS "throw" "using" PLUS MINUS PLUSPLUS MINUSMINUS "sizeof"
// "__alignof__"
// SCOPE "dynamic_cast" "static_cast" "reinterpret_cast" "const_cast" "typeid"
// DECIMALINT HEXADECIMALINT CharLiteral WCharLiteral WStringLiteral FLOATONE
// FLOATTWO NOT "new" "_cdecl" "__cdecl" "_near" "__near" "_far" "__far"
// "__interrupt" "pascal" "_pascal" "__pascal" "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_91(
    _tokenSet_91_data_, 12);
const unsigned long CPPParser::_tokenSet_92_data_[] = {
    64UL, 2UL, 2147614720UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// GREATERTHAN RPAREN COMMA ELLIPSIS
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_92(
    _tokenSet_92_data_, 8);
const unsigned long CPPParser::_tokenSet_93_data_[] = {
    262208UL, 3UL, 2214723584UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// GREATERTHAN ASSIGNEQUAL LPAREN RPAREN COMMA LSQUARE ELLIPSIS
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_93(
    _tokenSet_93_data_, 8);
const unsigned long CPPParser::_tokenSet_94_data_[] = {
    4294852594UL, 4294967295UL, 4293066751UL, 4293918723UL,
    4291821567UL, 32767UL,      0UL,          0UL,
    0UL,          0UL,          0UL,          0UL};
// EOF "template" LESSTHAN GREATERTHAN "typedef" SEMICOLON "enum" ID LCURLY
// "inline" "friend" RCURLY ASSIGNEQUAL COLON "extern" StringLiteral "struct"
// "union" "class" "_stdcall" "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID"
// "GFUNREAD" "GFARRAYSIZE" LPAREN RPAREN "GFARRAYSIZES" "GFARRAYELEMSIZE"
// "auto" "register" "static" "mutable" "_inline" "__inline" "virtual"
// "explicit" "typename" "char" "wchar_t" "bool" "short" "int" "_int8"
// "__int8" "int8_t" "_int16" "__int16" "int16_t" "_int32" "__int32" "int32_t"
// "_int64" "__int64" "int64_t" "uint8_t" "uint16_t" "uint32_t" "uint64_t"
// "_w64" "__w64" "long" "signed" "unsigned" "float" "double" "void" "_declspec"
// "__declspec" "const" "__const" "volatile" "__volatile__" COMMA "operator"
// "this" "true" "false" OCTALINT LSQUARE RSQUARE TILDE STAR AMPERSAND
// ELLIPSIS "throw" "using" TIMESEQUAL DIVIDEEQUAL MINUSEQUAL PLUSEQUAL
// MODEQUAL SHIFTLEFTEQUAL SHIFTRIGHTEQUAL BITWISEANDEQUAL BITWISEXOREQUAL
// BITWISEOREQUAL QUESTIONMARK OR AND BITWISEOR BITWISEXOR NOTEQUAL EQUAL
// LESSTHANOREQUALTO GREATERTHANOREQUALTO SHIFTLEFT SHIFTRIGHT PLUS MINUS
// DIVIDE MOD DOTMBR POINTERTOMBR PLUSPLUS MINUSMINUS "sizeof" "__alignof__"
// SCOPE "dynamic_cast" "static_cast" "reinterpret_cast" "const_cast" "typeid"
// DECIMALINT HEXADECIMALINT CharLiteral WCharLiteral WStringLiteral FLOATONE
// FLOATTWO NOT "new" "_cdecl" "__cdecl" "_near" "__near" "_far" "__far"
// "__interrupt" "pascal" "_pascal" "__pascal" "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_94(
    _tokenSet_94_data_, 12);
const unsigned long CPPParser::_tokenSet_95_data_[] = {
    262208UL, 2UL, 2147614720UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// GREATERTHAN ASSIGNEQUAL RPAREN COMMA ELLIPSIS
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_95(
    _tokenSet_95_data_, 8);
const unsigned long CPPParser::_tokenSet_96_data_[] = {
    64UL, 2UL, 131072UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// GREATERTHAN RPAREN COMMA
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_96(
    _tokenSet_96_data_, 8);
const unsigned long CPPParser::_tokenSet_97_data_[] = {
    101582176UL, 3UL, 4259446784UL, 4293918721UL, 557055UL, 16368UL,
    0UL,         0UL, 0UL,          0UL,          0UL,      0UL};
// LESSTHAN GREATERTHAN SEMICOLON ID RCURLY ASSIGNEQUAL COLON "_stdcall"
// "__stdcall" LPAREN RPAREN COMMA "operator" "this" "true" "false" LSQUARE
// RSQUARE TILDE STAR AMPERSAND ELLIPSIS "throw" TIMESEQUAL DIVIDEEQUAL
// MINUSEQUAL PLUSEQUAL MODEQUAL SHIFTLEFTEQUAL SHIFTRIGHTEQUAL BITWISEANDEQUAL
// BITWISEXOREQUAL BITWISEOREQUAL QUESTIONMARK OR AND BITWISEOR BITWISEXOR
// NOTEQUAL EQUAL LESSTHANOREQUALTO GREATERTHANOREQUALTO SHIFTLEFT SHIFTRIGHT
// PLUS MINUS DIVIDE MOD DOTMBR POINTERTOMBR SCOPE "_cdecl" "__cdecl" "_near"
// "__near" "_far" "__far" "__interrupt" "pascal" "_pascal" "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_97(
    _tokenSet_97_data_, 12);
const unsigned long CPPParser::_tokenSet_98_data_[] = {
    16777232UL, 4096UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// "template" "class" "typename"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_98(
    _tokenSet_98_data_, 8);
const unsigned long CPPParser::_tokenSet_99_data_[] = {
    1120UL, 0UL, 131072UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// LESSTHAN GREATERTHAN ID COMMA
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_99(
    _tokenSet_99_data_, 8);
const unsigned long CPPParser::_tokenSet_100_data_[] = {
    4291835520UL, 4294967293UL, 4058054655UL, 0UL, 524288UL, 16368UL,
    0UL,          0UL,          0UL,          0UL, 0UL,      0UL};
// "typedef" "enum" ID "inline" "friend" "extern" "struct" "union" "class"
// "_stdcall" "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID" "GFUNREAD"
// "GFARRAYSIZE"
// LPAREN "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto" "register" "static" "mutable"
// "_inline" "__inline" "virtual" "explicit" "typename" "char" "wchar_t"
// "bool" "short" "int" "_int8" "__int8" "int8_t" "_int16" "__int16" "int16_t"
// "_int32" "__int32" "int32_t" "_int64" "__int64" "int64_t" "uint8_t"
// "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64" "long" "signed" "unsigned"
// "float" "double" "void" "_declspec" "__declspec" "const" "__const" "volatile"
// "__volatile__" "operator" "this" "true" "false" TILDE STAR AMPERSAND
// ELLIPSIS SCOPE "_cdecl" "__cdecl" "_near" "__near" "_far" "__far"
// "__interrupt"
// "pascal" "_pascal" "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_100(
    _tokenSet_100_data_, 12);
const unsigned long CPPParser::_tokenSet_101_data_[] = {
    64UL, 0UL, 131072UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// GREATERTHAN COMMA
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_101(
    _tokenSet_101_data_, 8);
const unsigned long CPPParser::_tokenSet_102_data_[] = {
    100664416UL, 1UL, 1677852672UL, 0UL, 524288UL, 16368UL,
    0UL,         0UL, 0UL,          0UL, 0UL,      0UL};
// LESSTHAN GREATERTHAN ID "_stdcall" "__stdcall" LPAREN COMMA LSQUARE
// STAR AMPERSAND SCOPE "_cdecl" "__cdecl" "_near" "__near" "_far" "__far"
// "__interrupt" "pascal" "_pascal" "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_102(
    _tokenSet_102_data_, 12);
const unsigned long CPPParser::_tokenSet_103_data_[] = {
    1024UL, 4294963200UL, 4095UL, 0UL, 524288UL, 0UL,
    0UL,    0UL,          0UL,    0UL, 0UL,      0UL};
// ID "typename" "char" "wchar_t" "bool" "short" "int" "_int8" "__int8"
// "int8_t" "_int16" "__int16" "int16_t" "_int32" "__int32" "int32_t" "_int64"
// "__int64" "int64_t" "uint8_t" "uint16_t" "uint32_t" "uint64_t" "_w64"
// "__w64" "long" "signed" "unsigned" "float" "double" "void" "_declspec"
// "__declspec" SCOPE
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_103(
    _tokenSet_103_data_, 12);
const unsigned long CPPParser::_tokenSet_104_data_[] = {
    100664416UL, 4294959105UL, 1677856767UL, 0UL, 524288UL, 16368UL,
    0UL,         0UL,          0UL,          0UL, 0UL,      0UL};
// LESSTHAN GREATERTHAN ID "_stdcall" "__stdcall" LPAREN "char" "wchar_t"
// "bool" "short" "int" "_int8" "__int8" "int8_t" "_int16" "__int16" "int16_t"
// "_int32" "__int32" "int32_t" "_int64" "__int64" "int64_t" "uint8_t"
// "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64" "long" "signed" "unsigned"
// "float" "double" "void" "_declspec" "__declspec" COMMA LSQUARE STAR
// AMPERSAND SCOPE "_cdecl" "__cdecl" "_near" "__near" "_far" "__far"
// "__interrupt"
// "pascal" "_pascal" "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_104(
    _tokenSet_104_data_, 12);
const unsigned long CPPParser::_tokenSet_105_data_[] = {
    4291835616UL, 4294967293UL, 1677918207UL, 0UL, 524288UL, 16368UL,
    0UL,          0UL,          0UL,          0UL, 0UL,      0UL};
// LESSTHAN GREATERTHAN "typedef" "enum" ID "inline" "friend" "extern"
// "struct" "union" "class" "_stdcall" "__stdcall" "GFEXCLUDE" "GFINCLUDE"
// "GFID" "GFUNREAD" "GFARRAYSIZE" LPAREN "GFARRAYSIZES" "GFARRAYELEMSIZE"
// "auto" "register" "static" "mutable" "_inline" "__inline" "virtual"
// "explicit" "typename" "char" "wchar_t" "bool" "short" "int" "_int8"
// "__int8" "int8_t" "_int16" "__int16" "int16_t" "_int32" "__int32" "int32_t"
// "_int64" "__int64" "int64_t" "uint8_t" "uint16_t" "uint32_t" "uint64_t"
// "_w64" "__w64" "long" "signed" "unsigned" "float" "double" "void" "_declspec"
// "__declspec" "const" "__const" "volatile" "__volatile__" COMMA LSQUARE
// STAR AMPERSAND SCOPE "_cdecl" "__cdecl" "_near" "__near" "_far" "__far"
// "__interrupt" "pascal" "_pascal" "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_105(
    _tokenSet_105_data_, 12);
const unsigned long CPPParser::_tokenSet_106_data_[] = {
    4294194912UL, 4294967293UL, 2011365375UL, 3220176896UL,
    4294967295UL, 16399UL,      0UL,          0UL,
    0UL,          0UL,          0UL,          0UL};
// LESSTHAN GREATERTHAN "typedef" "enum" ID "inline" "friend" ASSIGNEQUAL
// "extern" StringLiteral "struct" "union" "class" "_stdcall" "__stdcall"
// "GFEXCLUDE" "GFINCLUDE" "GFID" "GFUNREAD" "GFARRAYSIZE" LPAREN "GFARRAYSIZES"
// "GFARRAYELEMSIZE" "auto" "register" "static" "mutable" "_inline" "__inline"
// "virtual" "explicit" "typename" "char" "wchar_t" "bool" "short" "int"
// "_int8" "__int8" "int8_t" "_int16" "__int16" "int16_t" "_int32" "__int32"
// "int32_t" "_int64" "__int64" "int64_t" "uint8_t" "uint16_t" "uint32_t"
// "uint64_t" "_w64" "__w64" "long" "signed" "unsigned" "float" "double"
// "void" "_declspec" "__declspec" "const" "__const" "volatile" "__volatile__"
// COMMA "operator" "this" "true" "false" OCTALINT LSQUARE TILDE STAR AMPERSAND
// TIMESEQUAL DIVIDEEQUAL MINUSEQUAL PLUSEQUAL MODEQUAL SHIFTLEFTEQUAL
// SHIFTRIGHTEQUAL BITWISEANDEQUAL BITWISEXOREQUAL BITWISEOREQUAL OR AND
// BITWISEOR BITWISEXOR NOTEQUAL EQUAL LESSTHANOREQUALTO GREATERTHANOREQUALTO
// SHIFTLEFT SHIFTRIGHT PLUS MINUS DIVIDE MOD DOTMBR POINTERTOMBR PLUSPLUS
// MINUSMINUS "sizeof" "__alignof__" SCOPE DOT POINTERTO "dynamic_cast"
// "static_cast" "reinterpret_cast" "const_cast" "typeid" DECIMALINT
// HEXADECIMALINT
// CharLiteral WCharLiteral WStringLiteral FLOATONE FLOATTWO NOT "new"
// "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_106(
    _tokenSet_106_data_, 12);
const unsigned long CPPParser::_tokenSet_107_data_[] = {
    917856UL, 2UL, 3355574272UL, 4293918720UL, 127UL, 0UL,
    0UL,      0UL, 0UL,          0UL,          0UL,   0UL};
// LESSTHAN GREATERTHAN SEMICOLON RCURLY ASSIGNEQUAL COLON RPAREN COMMA
// RSQUARE AMPERSAND ELLIPSIS TIMESEQUAL DIVIDEEQUAL MINUSEQUAL PLUSEQUAL
// MODEQUAL SHIFTLEFTEQUAL SHIFTRIGHTEQUAL BITWISEANDEQUAL BITWISEXOREQUAL
// BITWISEOREQUAL QUESTIONMARK OR AND BITWISEOR BITWISEXOR NOTEQUAL EQUAL
// LESSTHANOREQUALTO GREATERTHANOREQUALTO
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_107(
    _tokenSet_107_data_, 12);
const unsigned long CPPParser::_tokenSet_108_data_[] = {
    4291851904UL, 4294967292UL, 65535UL, 983042UL, 524288UL, 0UL,
    0UL,          0UL,          0UL,     0UL,      0UL,      0UL};
// "typedef" "enum" ID "inline" "friend" "namespace" "extern" "struct"
// "union" "class" "_stdcall" "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID"
// "GFUNREAD" "GFARRAYSIZE" "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto" "register"
// "static" "mutable" "_inline" "__inline" "virtual" "explicit" "typename"
// "char" "wchar_t" "bool" "short" "int" "_int8" "__int8" "int8_t" "_int16"
// "__int16" "int16_t" "_int32" "__int32" "int32_t" "_int64" "__int64"
// "int64_t" "uint8_t" "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64"
// "long" "signed" "unsigned" "float" "double" "void" "_declspec" "__declspec"
// "const" "__const" "volatile" "__volatile__" "using" "asm" "_asm" "__asm"
// "__asm__" SCOPE
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_108(
    _tokenSet_108_data_, 12);
const unsigned long CPPParser::_tokenSet_109_data_[] = {
    4291852192UL, 4294967293UL, 1910571007UL, 0UL, 524288UL, 16368UL,
    0UL,          0UL,          0UL,          0UL, 0UL,      0UL};
// LESSTHAN "typedef" SEMICOLON "enum" ID "inline" "friend" "namespace"
// "extern" "struct" "union" "class" "_stdcall" "__stdcall" "GFEXCLUDE"
// "GFINCLUDE" "GFID" "GFUNREAD" "GFARRAYSIZE" LPAREN "GFARRAYSIZES"
// "GFARRAYELEMSIZE"
// "auto" "register" "static" "mutable" "_inline" "__inline" "virtual"
// "explicit" "typename" "char" "wchar_t" "bool" "short" "int" "_int8"
// "__int8" "int8_t" "_int16" "__int16" "int16_t" "_int32" "__int32" "int32_t"
// "_int64" "__int64" "int64_t" "uint8_t" "uint16_t" "uint32_t" "uint64_t"
// "_w64" "__w64" "long" "signed" "unsigned" "float" "double" "void" "_declspec"
// "__declspec" "const" "__const" "volatile" "__volatile__" "operator"
// "this" "true" "false" TILDE STAR AMPERSAND SCOPE "_cdecl" "__cdecl"
// "_near" "__near" "_far" "__far" "__interrupt" "pascal" "_pascal" "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_109(
    _tokenSet_109_data_, 12);
const unsigned long CPPParser::_tokenSet_110_data_[] = {
    4294967280UL, 4294967293UL, 2013265919UL, 3221192703UL,
    4293910527UL, 32767UL,      0UL,          0UL,
    0UL,          0UL,          0UL,          0UL};
// "template" LESSTHAN GREATERTHAN "typedef" SEMICOLON "enum" ID LCURLY
// "inline" "friend" "namespace" "antlrTrace_on" "antlrTrace_off" RCURLY
// ASSIGNEQUAL COLON "extern" StringLiteral "struct" "union" "class" "_stdcall"
// "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID" "GFUNREAD" "GFARRAYSIZE"
// LPAREN "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto" "register" "static" "mutable"
// "_inline" "__inline" "virtual" "explicit" "typename" "char" "wchar_t"
// "bool" "short" "int" "_int8" "__int8" "int8_t" "_int16" "__int16" "int16_t"
// "_int32" "__int32" "int32_t" "_int64" "__int64" "int64_t" "uint8_t"
// "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64" "long" "signed" "unsigned"
// "float" "double" "void" "_declspec" "__declspec" "const" "__const" "volatile"
// "__volatile__" "GFIGNORE" COMMA "public" "protected" "private" "operator"
// "this" "true" "false" OCTALINT LSQUARE TILDE STAR AMPERSAND "throw"
// "using" "case" "default" "if" "else" "switch" "while" "do" "for" "goto"
// "continue" "break" "return" "try" "asm" "_asm" "__asm" "__asm__" TIMESEQUAL
// DIVIDEEQUAL MINUSEQUAL PLUSEQUAL MODEQUAL SHIFTLEFTEQUAL SHIFTRIGHTEQUAL
// BITWISEANDEQUAL BITWISEXOREQUAL BITWISEOREQUAL OR AND BITWISEOR BITWISEXOR
// NOTEQUAL EQUAL LESSTHANOREQUALTO GREATERTHANOREQUALTO SHIFTLEFT SHIFTRIGHT
// PLUS MINUS DIVIDE MOD POINTERTOMBR PLUSPLUS MINUSMINUS "sizeof" "__alignof__"
// SCOPE POINTERTO "dynamic_cast" "static_cast" "reinterpret_cast" "const_cast"
// "typeid" DECIMALINT HEXADECIMALINT CharLiteral WCharLiteral WStringLiteral
// FLOATONE FLOATTWO NOT "new" "_cdecl" "__cdecl" "_near" "__near" "_far"
// "__far" "__interrupt" "pascal" "_pascal" "__pascal" "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_110(
    _tokenSet_110_data_, 12);
const unsigned long CPPParser::_tokenSet_111_data_[] = {
    1056UL, 0UL, 568389632UL, 0UL, 524288UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// LESSTHAN ID "const" "__const" "volatile" "__volatile__" "operator" "this"
// "true" "false" STAR SCOPE
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_111(
    _tokenSet_111_data_, 12);
const unsigned long CPPParser::_tokenSet_112_data_[] = {
    4294195168UL, 4294967293UL, 2011365375UL, 4293918720UL,
    4294967295UL, 16399UL,      0UL,          0UL,
    0UL,          0UL,          0UL,          0UL};
// LESSTHAN GREATERTHAN "typedef" SEMICOLON "enum" ID "inline" "friend"
// ASSIGNEQUAL "extern" StringLiteral "struct" "union" "class" "_stdcall"
// "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID" "GFUNREAD" "GFARRAYSIZE"
// LPAREN "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto" "register" "static" "mutable"
// "_inline" "__inline" "virtual" "explicit" "typename" "char" "wchar_t"
// "bool" "short" "int" "_int8" "__int8" "int8_t" "_int16" "__int16" "int16_t"
// "_int32" "__int32" "int32_t" "_int64" "__int64" "int64_t" "uint8_t"
// "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64" "long" "signed" "unsigned"
// "float" "double" "void" "_declspec" "__declspec" "const" "__const" "volatile"
// "__volatile__" COMMA "operator" "this" "true" "false" OCTALINT LSQUARE
// TILDE STAR AMPERSAND TIMESEQUAL DIVIDEEQUAL MINUSEQUAL PLUSEQUAL MODEQUAL
// SHIFTLEFTEQUAL SHIFTRIGHTEQUAL BITWISEANDEQUAL BITWISEXOREQUAL BITWISEOREQUAL
// QUESTIONMARK OR AND BITWISEOR BITWISEXOR NOTEQUAL EQUAL LESSTHANOREQUALTO
// GREATERTHANOREQUALTO SHIFTLEFT SHIFTRIGHT PLUS MINUS DIVIDE MOD DOTMBR
// POINTERTOMBR PLUSPLUS MINUSMINUS "sizeof" "__alignof__" SCOPE DOT POINTERTO
// "dynamic_cast" "static_cast" "reinterpret_cast" "const_cast" "typeid"
// DECIMALINT HEXADECIMALINT CharLiteral WCharLiteral WStringLiteral FLOATONE
// FLOATTWO NOT "new" "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_112(
    _tokenSet_112_data_, 12);
const unsigned long CPPParser::_tokenSet_113_data_[] = {
    4294967282UL, 4294967293UL, 2013265919UL, 4294967295UL,
    4294967295UL, 32767UL,      0UL,          0UL,
    0UL,          0UL,          0UL,          0UL};
// EOF "template" LESSTHAN GREATERTHAN "typedef" SEMICOLON "enum" ID LCURLY
// "inline" "friend" "namespace" "antlrTrace_on" "antlrTrace_off" RCURLY
// ASSIGNEQUAL COLON "extern" StringLiteral "struct" "union" "class" "_stdcall"
// "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID" "GFUNREAD" "GFARRAYSIZE"
// LPAREN "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto" "register" "static" "mutable"
// "_inline" "__inline" "virtual" "explicit" "typename" "char" "wchar_t"
// "bool" "short" "int" "_int8" "__int8" "int8_t" "_int16" "__int16" "int16_t"
// "_int32" "__int32" "int32_t" "_int64" "__int64" "int64_t" "uint8_t"
// "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64" "long" "signed" "unsigned"
// "float" "double" "void" "_declspec" "__declspec" "const" "__const" "volatile"
// "__volatile__" "GFIGNORE" COMMA "public" "protected" "private" "operator"
// "this" "true" "false" OCTALINT LSQUARE TILDE STAR AMPERSAND "throw"
// "using" "case" "default" "if" "else" "switch" "while" "do" "for" "goto"
// "continue" "break" "return" "try" "catch" "asm" "_asm" "__asm" "__asm__"
// TIMESEQUAL DIVIDEEQUAL MINUSEQUAL PLUSEQUAL MODEQUAL SHIFTLEFTEQUAL
// SHIFTRIGHTEQUAL BITWISEANDEQUAL BITWISEXOREQUAL BITWISEOREQUAL QUESTIONMARK
// OR AND BITWISEOR BITWISEXOR NOTEQUAL EQUAL LESSTHANOREQUALTO
// GREATERTHANOREQUALTO
// SHIFTLEFT SHIFTRIGHT PLUS MINUS DIVIDE MOD DOTMBR POINTERTOMBR PLUSPLUS
// MINUSMINUS "sizeof" "__alignof__" SCOPE DOT POINTERTO "dynamic_cast"
// "static_cast" "reinterpret_cast" "const_cast" "typeid" DECIMALINT
// HEXADECIMALINT
// CharLiteral WCharLiteral WStringLiteral FLOATONE FLOATTWO NOT "new"
// "_cdecl" "__cdecl" "_near" "__near" "_far" "__far" "__interrupt" "pascal"
// "_pascal" "__pascal" "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_113(
    _tokenSet_113_data_, 12);
const unsigned long CPPParser::_tokenSet_114_data_[] = {
    4294180752UL, 4294967293UL, 1945960447UL, 1048575UL, 4291790336UL, 32767UL,
    0UL,          0UL,          0UL,          0UL,       0UL,          0UL};
// "template" "typedef" SEMICOLON "enum" ID LCURLY "inline" "friend" "namespace"
// "antlrTrace_on" "antlrTrace_off" RCURLY "extern" StringLiteral "struct"
// "union" "class" "_stdcall" "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID"
// "GFUNREAD" "GFARRAYSIZE" LPAREN "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto"
// "register" "static" "mutable" "_inline" "__inline" "virtual" "explicit"
// "typename" "char" "wchar_t" "bool" "short" "int" "_int8" "__int8" "int8_t"
// "_int16" "__int16" "int16_t" "_int32" "__int32" "int32_t" "_int64" "__int64"
// "int64_t" "uint8_t" "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64"
// "long" "signed" "unsigned" "float" "double" "void" "_declspec" "__declspec"
// "const" "__const" "volatile" "__volatile__" "public" "protected" "private"
// "operator" "this" "true" "false" OCTALINT TILDE STAR AMPERSAND "throw"
// "using" "case" "default" "if" "else" "switch" "while" "do" "for" "goto"
// "continue" "break" "return" "try" "catch" "asm" "_asm" "__asm" "__asm__"
// PLUS MINUS PLUSPLUS MINUSMINUS "sizeof" "__alignof__" SCOPE "dynamic_cast"
// "static_cast" "reinterpret_cast" "const_cast" "typeid" DECIMALINT
// HEXADECIMALINT
// CharLiteral WCharLiteral WStringLiteral FLOATONE FLOATTWO NOT "new"
// "_cdecl" "__cdecl" "_near" "__near" "_far" "__far" "__interrupt" "pascal"
// "_pascal" "__pascal" "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_114(
    _tokenSet_114_data_, 12);
const unsigned long CPPParser::_tokenSet_115_data_[] = {
    917824UL, 2UL, 2281832448UL, 1072693248UL, 0UL, 0UL, 0UL, 0UL};
// GREATERTHAN SEMICOLON RCURLY ASSIGNEQUAL COLON RPAREN COMMA RSQUARE
// ELLIPSIS TIMESEQUAL DIVIDEEQUAL MINUSEQUAL PLUSEQUAL MODEQUAL SHIFTLEFTEQUAL
// SHIFTRIGHTEQUAL BITWISEANDEQUAL BITWISEXOREQUAL BITWISEOREQUAL
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_115(
    _tokenSet_115_data_, 8);
const unsigned long CPPParser::_tokenSet_116_data_[] = {
    917824UL, 2UL, 2281832448UL, 2146435072UL, 0UL, 0UL, 0UL, 0UL};
// GREATERTHAN SEMICOLON RCURLY ASSIGNEQUAL COLON RPAREN COMMA RSQUARE
// ELLIPSIS TIMESEQUAL DIVIDEEQUAL MINUSEQUAL PLUSEQUAL MODEQUAL SHIFTLEFTEQUAL
// SHIFTRIGHTEQUAL BITWISEANDEQUAL BITWISEXOREQUAL BITWISEOREQUAL QUESTIONMARK
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_116(
    _tokenSet_116_data_, 8);
const unsigned long CPPParser::_tokenSet_117_data_[] = {
    917824UL, 2UL, 2281832448UL, 4293918720UL, 0UL, 0UL, 0UL, 0UL};
// GREATERTHAN SEMICOLON RCURLY ASSIGNEQUAL COLON RPAREN COMMA RSQUARE
// ELLIPSIS TIMESEQUAL DIVIDEEQUAL MINUSEQUAL PLUSEQUAL MODEQUAL SHIFTLEFTEQUAL
// SHIFTRIGHTEQUAL BITWISEANDEQUAL BITWISEXOREQUAL BITWISEOREQUAL QUESTIONMARK
// OR
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_117(
    _tokenSet_117_data_, 8);
const unsigned long CPPParser::_tokenSet_118_data_[] = {
    917824UL, 2UL, 2281832448UL, 4293918720UL, 1UL, 0UL,
    0UL,      0UL, 0UL,          0UL,          0UL, 0UL};
// GREATERTHAN SEMICOLON RCURLY ASSIGNEQUAL COLON RPAREN COMMA RSQUARE
// ELLIPSIS TIMESEQUAL DIVIDEEQUAL MINUSEQUAL PLUSEQUAL MODEQUAL SHIFTLEFTEQUAL
// SHIFTRIGHTEQUAL BITWISEANDEQUAL BITWISEXOREQUAL BITWISEOREQUAL QUESTIONMARK
// OR AND
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_118(
    _tokenSet_118_data_, 12);
const unsigned long CPPParser::_tokenSet_119_data_[] = {
    917824UL, 2UL, 2281832448UL, 4293918720UL, 3UL, 0UL,
    0UL,      0UL, 0UL,          0UL,          0UL, 0UL};
// GREATERTHAN SEMICOLON RCURLY ASSIGNEQUAL COLON RPAREN COMMA RSQUARE
// ELLIPSIS TIMESEQUAL DIVIDEEQUAL MINUSEQUAL PLUSEQUAL MODEQUAL SHIFTLEFTEQUAL
// SHIFTRIGHTEQUAL BITWISEANDEQUAL BITWISEXOREQUAL BITWISEOREQUAL QUESTIONMARK
// OR AND BITWISEOR
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_119(
    _tokenSet_119_data_, 12);
const unsigned long CPPParser::_tokenSet_120_data_[] = {
    917824UL, 2UL, 2281832448UL, 4293918720UL, 7UL, 0UL,
    0UL,      0UL, 0UL,          0UL,          0UL, 0UL};
// GREATERTHAN SEMICOLON RCURLY ASSIGNEQUAL COLON RPAREN COMMA RSQUARE
// ELLIPSIS TIMESEQUAL DIVIDEEQUAL MINUSEQUAL PLUSEQUAL MODEQUAL SHIFTLEFTEQUAL
// SHIFTRIGHTEQUAL BITWISEANDEQUAL BITWISEXOREQUAL BITWISEOREQUAL QUESTIONMARK
// OR AND BITWISEOR BITWISEXOR
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_120(
    _tokenSet_120_data_, 12);
const unsigned long CPPParser::_tokenSet_121_data_[] = {
    917824UL, 2UL, 3355574272UL, 4293918720UL, 7UL, 0UL,
    0UL,      0UL, 0UL,          0UL,          0UL, 0UL};
// GREATERTHAN SEMICOLON RCURLY ASSIGNEQUAL COLON RPAREN COMMA RSQUARE
// AMPERSAND ELLIPSIS TIMESEQUAL DIVIDEEQUAL MINUSEQUAL PLUSEQUAL MODEQUAL
// SHIFTLEFTEQUAL SHIFTRIGHTEQUAL BITWISEANDEQUAL BITWISEXOREQUAL BITWISEOREQUAL
// QUESTIONMARK OR AND BITWISEOR BITWISEXOR
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_121(
    _tokenSet_121_data_, 12);
const unsigned long CPPParser::_tokenSet_122_data_[] = {
    96UL, 0UL, 0UL, 0UL, 96UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// LESSTHAN GREATERTHAN LESSTHANOREQUALTO GREATERTHANOREQUALTO
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_122(
    _tokenSet_122_data_, 12);
const unsigned long CPPParser::_tokenSet_123_data_[] = {
    917824UL, 2UL, 3355574272UL, 4293918720UL, 31UL, 0UL,
    0UL,      0UL, 0UL,          0UL,          0UL,  0UL};
// GREATERTHAN SEMICOLON RCURLY ASSIGNEQUAL COLON RPAREN COMMA RSQUARE
// AMPERSAND ELLIPSIS TIMESEQUAL DIVIDEEQUAL MINUSEQUAL PLUSEQUAL MODEQUAL
// SHIFTLEFTEQUAL SHIFTRIGHTEQUAL BITWISEANDEQUAL BITWISEXOREQUAL BITWISEOREQUAL
// QUESTIONMARK OR AND BITWISEOR BITWISEXOR NOTEQUAL EQUAL
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_123(
    _tokenSet_123_data_, 12);
const unsigned long CPPParser::_tokenSet_124_data_[] = {
    917856UL, 2UL, 3355574272UL, 4293918720UL, 511UL, 0UL,
    0UL,      0UL, 0UL,          0UL,          0UL,   0UL};
// LESSTHAN GREATERTHAN SEMICOLON RCURLY ASSIGNEQUAL COLON RPAREN COMMA
// RSQUARE AMPERSAND ELLIPSIS TIMESEQUAL DIVIDEEQUAL MINUSEQUAL PLUSEQUAL
// MODEQUAL SHIFTLEFTEQUAL SHIFTRIGHTEQUAL BITWISEANDEQUAL BITWISEXOREQUAL
// BITWISEOREQUAL QUESTIONMARK OR AND BITWISEOR BITWISEXOR NOTEQUAL EQUAL
// LESSTHANOREQUALTO GREATERTHANOREQUALTO SHIFTLEFT SHIFTRIGHT
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_124(
    _tokenSet_124_data_, 12);
const unsigned long CPPParser::_tokenSet_125_data_[] = {
    0UL, 0UL, 536870912UL, 0UL, 6144UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// STAR DIVIDE MOD
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_125(
    _tokenSet_125_data_, 12);
const unsigned long CPPParser::_tokenSet_126_data_[] = {
    917856UL, 2UL, 3355574272UL, 4293918720UL, 2047UL, 0UL,
    0UL,      0UL, 0UL,          0UL,          0UL,    0UL};
// LESSTHAN GREATERTHAN SEMICOLON RCURLY ASSIGNEQUAL COLON RPAREN COMMA
// RSQUARE AMPERSAND ELLIPSIS TIMESEQUAL DIVIDEEQUAL MINUSEQUAL PLUSEQUAL
// MODEQUAL SHIFTLEFTEQUAL SHIFTRIGHTEQUAL BITWISEANDEQUAL BITWISEXOREQUAL
// BITWISEOREQUAL QUESTIONMARK OR AND BITWISEOR BITWISEXOR NOTEQUAL EQUAL
// LESSTHANOREQUALTO GREATERTHANOREQUALTO SHIFTLEFT SHIFTRIGHT PLUS MINUS
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_126(
    _tokenSet_126_data_, 12);
const unsigned long CPPParser::_tokenSet_127_data_[] = {
    917856UL, 2UL, 3892445184UL, 4293918720UL, 8191UL, 0UL,
    0UL,      0UL, 0UL,          0UL,          0UL,    0UL};
// LESSTHAN GREATERTHAN SEMICOLON RCURLY ASSIGNEQUAL COLON RPAREN COMMA
// RSQUARE STAR AMPERSAND ELLIPSIS TIMESEQUAL DIVIDEEQUAL MINUSEQUAL PLUSEQUAL
// MODEQUAL SHIFTLEFTEQUAL SHIFTRIGHTEQUAL BITWISEANDEQUAL BITWISEXOREQUAL
// BITWISEOREQUAL QUESTIONMARK OR AND BITWISEOR BITWISEXOR NOTEQUAL EQUAL
// LESSTHANOREQUALTO GREATERTHANOREQUALTO SHIFTLEFT SHIFTRIGHT PLUS MINUS
// DIVIDE MOD
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_127(
    _tokenSet_127_data_, 12);
const unsigned long CPPParser::_tokenSet_128_data_[] = {
    1024UL, 4294963200UL, 65535UL, 0UL, 524288UL, 0UL,
    0UL,    0UL,          0UL,     0UL, 0UL,      0UL};
// ID "typename" "char" "wchar_t" "bool" "short" "int" "_int8" "__int8"
// "int8_t" "_int16" "__int16" "int16_t" "_int32" "__int32" "int32_t" "_int64"
// "__int64" "int64_t" "uint8_t" "uint16_t" "uint32_t" "uint64_t" "_w64"
// "__w64" "long" "signed" "unsigned" "float" "double" "void" "_declspec"
// "__declspec" "const" "__const" "volatile" "__volatile__" SCOPE
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_128(
    _tokenSet_128_data_, 12);
const unsigned long CPPParser::_tokenSet_129_data_[] = {
    917856UL, 2UL, 3892445184UL, 4293918720UL, 32767UL, 0UL,
    0UL,      0UL, 0UL,          0UL,          0UL,     0UL};
// LESSTHAN GREATERTHAN SEMICOLON RCURLY ASSIGNEQUAL COLON RPAREN COMMA
// RSQUARE STAR AMPERSAND ELLIPSIS TIMESEQUAL DIVIDEEQUAL MINUSEQUAL PLUSEQUAL
// MODEQUAL SHIFTLEFTEQUAL SHIFTRIGHTEQUAL BITWISEANDEQUAL BITWISEXOREQUAL
// BITWISEOREQUAL QUESTIONMARK OR AND BITWISEOR BITWISEXOR NOTEQUAL EQUAL
// LESSTHANOREQUALTO GREATERTHANOREQUALTO SHIFTLEFT SHIFTRIGHT PLUS MINUS
// DIVIDE MOD DOTMBR POINTERTOMBR
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_129(
    _tokenSet_129_data_, 12);
const unsigned long CPPParser::_tokenSet_130_data_[] = {
    2098176UL, 4294963201UL, 333451263UL, 0UL, 4291297280UL, 3UL,
    0UL,       0UL,          0UL,         0UL, 0UL,          0UL};
// ID StringLiteral LPAREN "typename" "char" "wchar_t" "bool" "short" "int"
// "_int8" "__int8" "int8_t" "_int16" "__int16" "int16_t" "_int32" "__int32"
// "int32_t" "_int64" "__int64" "int64_t" "uint8_t" "uint16_t" "uint32_t"
// "uint64_t" "_w64" "__w64" "long" "signed" "unsigned" "float" "double"
// "void" "_declspec" "__declspec" "operator" "this" "true" "false" OCTALINT
// TILDE SCOPE "dynamic_cast" "static_cast" "reinterpret_cast" "const_cast"
// "typeid" DECIMALINT HEXADECIMALINT CharLiteral WCharLiteral WStringLiteral
// FLOATONE FLOATTWO
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_130(
    _tokenSet_130_data_, 12);
const unsigned long CPPParser::_tokenSet_131_data_[] = {
    3016032UL, 4294963203UL, 4293005311UL, 4293918720UL, 4294967295UL, 16399UL,
    0UL,       0UL,          0UL,          0UL,          0UL,          0UL};
// LESSTHAN GREATERTHAN SEMICOLON ID RCURLY ASSIGNEQUAL COLON StringLiteral
// LPAREN RPAREN "typename" "char" "wchar_t" "bool" "short" "int" "_int8"
// "__int8" "int8_t" "_int16" "__int16" "int16_t" "_int32" "__int32" "int32_t"
// "_int64" "__int64" "int64_t" "uint8_t" "uint16_t" "uint32_t" "uint64_t"
// "_w64" "__w64" "long" "signed" "unsigned" "float" "double" "void" "_declspec"
// "__declspec" COMMA "operator" "this" "true" "false" OCTALINT LSQUARE
// RSQUARE TILDE STAR AMPERSAND ELLIPSIS TIMESEQUAL DIVIDEEQUAL MINUSEQUAL
// PLUSEQUAL MODEQUAL SHIFTLEFTEQUAL SHIFTRIGHTEQUAL BITWISEANDEQUAL
// BITWISEXOREQUAL
// BITWISEOREQUAL QUESTIONMARK OR AND BITWISEOR BITWISEXOR NOTEQUAL EQUAL
// LESSTHANOREQUALTO GREATERTHANOREQUALTO SHIFTLEFT SHIFTRIGHT PLUS MINUS
// DIVIDE MOD DOTMBR POINTERTOMBR PLUSPLUS MINUSMINUS "sizeof" "__alignof__"
// SCOPE DOT POINTERTO "dynamic_cast" "static_cast" "reinterpret_cast"
// "const_cast" "typeid" DECIMALINT HEXADECIMALINT CharLiteral WCharLiteral
// WStringLiteral FLOATONE FLOATTWO NOT "new" "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_131(
    _tokenSet_131_data_, 12);
const unsigned long CPPParser::_tokenSet_132_data_[] = {
    0UL, 0UL, 1879048192UL, 0UL, 1536UL, 4UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// TILDE STAR AMPERSAND PLUS MINUS NOT
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_132(
    _tokenSet_132_data_, 12);
const unsigned long CPPParser::_tokenSet_133_data_[] = {
    0UL, 0UL, 0UL, 0UL, 524288UL, 16392UL, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL};
// SCOPE "new" "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_133(
    _tokenSet_133_data_, 12);
const unsigned long CPPParser::_tokenSet_134_data_[] = {
    4293932672UL, 4294967293UL, 2011234303UL, 0UL, 4291790336UL, 16399UL,
    0UL,          0UL,          0UL,          0UL, 0UL,          0UL};
// "typedef" "enum" ID "inline" "friend" "extern" StringLiteral "struct"
// "union" "class" "_stdcall" "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID"
// "GFUNREAD" "GFARRAYSIZE" LPAREN "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto"
// "register" "static" "mutable" "_inline" "__inline" "virtual" "explicit"
// "typename" "char" "wchar_t" "bool" "short" "int" "_int8" "__int8" "int8_t"
// "_int16" "__int16" "int16_t" "_int32" "__int32" "int32_t" "_int64" "__int64"
// "int64_t" "uint8_t" "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64"
// "long" "signed" "unsigned" "float" "double" "void" "_declspec" "__declspec"
// "const" "__const" "volatile" "__volatile__" "operator" "this" "true"
// "false" OCTALINT LSQUARE TILDE STAR AMPERSAND PLUS MINUS PLUSPLUS MINUSMINUS
// "sizeof" "__alignof__" SCOPE "dynamic_cast" "static_cast" "reinterpret_cast"
// "const_cast" "typeid" DECIMALINT HEXADECIMALINT CharLiteral WCharLiteral
// WStringLiteral FLOATONE FLOATTWO NOT "new" "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_134(
    _tokenSet_134_data_, 12);
const unsigned long CPPParser::_tokenSet_135_data_[] = {
    4291835552UL, 4294967295UL, 1677787135UL, 0UL, 524288UL, 16368UL,
    0UL,          0UL,          0UL,          0UL, 0UL,      0UL};
// LESSTHAN "typedef" "enum" ID "inline" "friend" "extern" "struct" "union"
// "class" "_stdcall" "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID" "GFUNREAD"
// "GFARRAYSIZE" LPAREN RPAREN "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto"
// "register" "static" "mutable" "_inline" "__inline" "virtual" "explicit"
// "typename" "char" "wchar_t" "bool" "short" "int" "_int8" "__int8" "int8_t"
// "_int16" "__int16" "int16_t" "_int32" "__int32" "int32_t" "_int64" "__int64"
// "int64_t" "uint8_t" "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64"
// "long" "signed" "unsigned" "float" "double" "void" "_declspec" "__declspec"
// "const" "__const" "volatile" "__volatile__" LSQUARE STAR AMPERSAND SCOPE
// "_cdecl" "__cdecl" "_near" "__near" "_far" "__far" "__interrupt" "pascal"
// "_pascal" "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_135(
    _tokenSet_135_data_, 12);
const unsigned long CPPParser::_tokenSet_136_data_[] = {
    4294194912UL, 4294967295UL, 2011365375UL, 4293918720UL,
    4294967295UL, 16399UL,      0UL,          0UL,
    0UL,          0UL,          0UL,          0UL};
// LESSTHAN GREATERTHAN "typedef" "enum" ID "inline" "friend" ASSIGNEQUAL
// "extern" StringLiteral "struct" "union" "class" "_stdcall" "__stdcall"
// "GFEXCLUDE" "GFINCLUDE" "GFID" "GFUNREAD" "GFARRAYSIZE" LPAREN RPAREN
// "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto" "register" "static" "mutable"
// "_inline" "__inline" "virtual" "explicit" "typename" "char" "wchar_t"
// "bool" "short" "int" "_int8" "__int8" "int8_t" "_int16" "__int16" "int16_t"
// "_int32" "__int32" "int32_t" "_int64" "__int64" "int64_t" "uint8_t"
// "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64" "long" "signed" "unsigned"
// "float" "double" "void" "_declspec" "__declspec" "const" "__const" "volatile"
// "__volatile__" COMMA "operator" "this" "true" "false" OCTALINT LSQUARE
// TILDE STAR AMPERSAND TIMESEQUAL DIVIDEEQUAL MINUSEQUAL PLUSEQUAL MODEQUAL
// SHIFTLEFTEQUAL SHIFTRIGHTEQUAL BITWISEANDEQUAL BITWISEXOREQUAL BITWISEOREQUAL
// QUESTIONMARK OR AND BITWISEOR BITWISEXOR NOTEQUAL EQUAL LESSTHANOREQUALTO
// GREATERTHANOREQUALTO SHIFTLEFT SHIFTRIGHT PLUS MINUS DIVIDE MOD DOTMBR
// POINTERTOMBR PLUSPLUS MINUSMINUS "sizeof" "__alignof__" SCOPE DOT POINTERTO
// "dynamic_cast" "static_cast" "reinterpret_cast" "const_cast" "typeid"
// DECIMALINT HEXADECIMALINT CharLiteral WCharLiteral WStringLiteral FLOATONE
// FLOATTWO NOT "new" "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_136(
    _tokenSet_136_data_, 12);
const unsigned long CPPParser::_tokenSet_137_data_[] = {
    1056UL, 4294959105UL, 4095UL, 0UL, 524288UL, 0UL,
    0UL,    0UL,          0UL,    0UL, 0UL,      0UL};
// LESSTHAN ID LPAREN "char" "wchar_t" "bool" "short" "int" "_int8" "__int8"
// "int8_t" "_int16" "__int16" "int16_t" "_int32" "__int32" "int32_t" "_int64"
// "__int64" "int64_t" "uint8_t" "uint16_t" "uint32_t" "uint64_t" "_w64"
// "__w64" "long" "signed" "unsigned" "float" "double" "void" "_declspec"
// "__declspec" SCOPE
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_137(
    _tokenSet_137_data_, 12);
const unsigned long CPPParser::_tokenSet_138_data_[] = {
    2098176UL, 1UL, 333447168UL, 0UL, 4161273856UL, 3UL,
    0UL,       0UL, 0UL,         0UL, 0UL,          0UL};
// ID StringLiteral LPAREN "operator" "this" "true" "false" OCTALINT TILDE
// SCOPE DECIMALINT HEXADECIMALINT CharLiteral WCharLiteral WStringLiteral
// FLOATONE FLOATTWO
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_138(
    _tokenSet_138_data_, 12);
const unsigned long CPPParser::_tokenSet_139_data_[] = {
    4291835520UL, 4294967293UL, 65535UL, 0UL, 524288UL, 0UL,
    0UL,          0UL,          0UL,     0UL, 0UL,      0UL};
// "typedef" "enum" ID "inline" "friend" "extern" "struct" "union" "class"
// "_stdcall" "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID" "GFUNREAD"
// "GFARRAYSIZE"
// LPAREN "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto" "register" "static" "mutable"
// "_inline" "__inline" "virtual" "explicit" "typename" "char" "wchar_t"
// "bool" "short" "int" "_int8" "__int8" "int8_t" "_int16" "__int16" "int16_t"
// "_int32" "__int32" "int32_t" "_int64" "__int64" "int64_t" "uint8_t"
// "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64" "long" "signed" "unsigned"
// "float" "double" "void" "_declspec" "__declspec" "const" "__const" "volatile"
// "__volatile__" SCOPE
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_139(
    _tokenSet_139_data_, 12);
const unsigned long CPPParser::_tokenSet_140_data_[] = {
    4292753376UL, 4294967295UL, 3959619583UL, 4293918720UL, 557055UL, 16368UL,
    0UL,          0UL,          0UL,          0UL,          0UL,      0UL};
// LESSTHAN GREATERTHAN "typedef" SEMICOLON "enum" ID "inline" "friend"
// RCURLY ASSIGNEQUAL COLON "extern" "struct" "union" "class" "_stdcall"
// "__stdcall" "GFEXCLUDE" "GFINCLUDE" "GFID" "GFUNREAD" "GFARRAYSIZE"
// LPAREN RPAREN "GFARRAYSIZES" "GFARRAYELEMSIZE" "auto" "register" "static"
// "mutable" "_inline" "__inline" "virtual" "explicit" "typename" "char"
// "wchar_t" "bool" "short" "int" "_int8" "__int8" "int8_t" "_int16" "__int16"
// "int16_t" "_int32" "__int32" "int32_t" "_int64" "__int64" "int64_t"
// "uint8_t" "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64" "long" "signed"
// "unsigned" "float" "double" "void" "_declspec" "__declspec" "const"
// "__const" "volatile" "__volatile__" COMMA LSQUARE RSQUARE STAR AMPERSAND
// ELLIPSIS TIMESEQUAL DIVIDEEQUAL MINUSEQUAL PLUSEQUAL MODEQUAL SHIFTLEFTEQUAL
// SHIFTRIGHTEQUAL BITWISEANDEQUAL BITWISEXOREQUAL BITWISEOREQUAL QUESTIONMARK
// OR AND BITWISEOR BITWISEXOR NOTEQUAL EQUAL LESSTHANOREQUALTO
// GREATERTHANOREQUALTO
// SHIFTLEFT SHIFTRIGHT PLUS MINUS DIVIDE MOD DOTMBR POINTERTOMBR SCOPE
// "_cdecl" "__cdecl" "_near" "__near" "_far" "__far" "__interrupt" "pascal"
// "_pascal" "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_140(
    _tokenSet_140_data_, 12);
const unsigned long CPPParser::_tokenSet_141_data_[] = {
    917856UL, 3UL, 3959554048UL, 4293918720UL, 3276799UL, 0UL,
    0UL,      0UL, 0UL,          0UL,          0UL,       0UL};
// LESSTHAN GREATERTHAN SEMICOLON RCURLY ASSIGNEQUAL COLON LPAREN RPAREN
// COMMA LSQUARE RSQUARE STAR AMPERSAND ELLIPSIS TIMESEQUAL DIVIDEEQUAL
// MINUSEQUAL PLUSEQUAL MODEQUAL SHIFTLEFTEQUAL SHIFTRIGHTEQUAL BITWISEANDEQUAL
// BITWISEXOREQUAL BITWISEOREQUAL QUESTIONMARK OR AND BITWISEOR BITWISEXOR
// NOTEQUAL EQUAL LESSTHANOREQUALTO GREATERTHANOREQUALTO SHIFTLEFT SHIFTRIGHT
// PLUS MINUS DIVIDE MOD DOTMBR POINTERTOMBR PLUSPLUS MINUSMINUS DOT POINTERTO
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_141(
    _tokenSet_141_data_, 12);
const unsigned long CPPParser::_tokenSet_142_data_[] = {
    100664320UL, 0UL, 1677721600UL, 0UL, 524288UL, 16368UL,
    0UL,         0UL, 0UL,          0UL, 0UL,      0UL};
// ID "_stdcall" "__stdcall" LSQUARE STAR AMPERSAND SCOPE "_cdecl" "__cdecl"
// "_near" "__near" "_far" "__far" "__interrupt" "pascal" "_pascal" "__pascal"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_142(
    _tokenSet_142_data_, 12);
const unsigned long CPPParser::_tokenSet_143_data_[] = {
    103679328UL,  4294963203UL, 4293066751UL, 4293918720UL,
    4291821567UL, 32767UL,      0UL,          0UL,
    0UL,          0UL,          0UL,          0UL};
// LESSTHAN GREATERTHAN SEMICOLON ID RCURLY ASSIGNEQUAL COLON StringLiteral
// "_stdcall" "__stdcall" LPAREN RPAREN "typename" "char" "wchar_t" "bool"
// "short" "int" "_int8" "__int8" "int8_t" "_int16" "__int16" "int16_t"
// "_int32" "__int32" "int32_t" "_int64" "__int64" "int64_t" "uint8_t"
// "uint16_t" "uint32_t" "uint64_t" "_w64" "__w64" "long" "signed" "unsigned"
// "float" "double" "void" "_declspec" "__declspec" "const" "__const" "volatile"
// "__volatile__" COMMA "operator" "this" "true" "false" OCTALINT LSQUARE
// RSQUARE TILDE STAR AMPERSAND ELLIPSIS TIMESEQUAL DIVIDEEQUAL MINUSEQUAL
// PLUSEQUAL MODEQUAL SHIFTLEFTEQUAL SHIFTRIGHTEQUAL BITWISEANDEQUAL
// BITWISEXOREQUAL
// BITWISEOREQUAL QUESTIONMARK OR AND BITWISEOR BITWISEXOR NOTEQUAL EQUAL
// LESSTHANOREQUALTO GREATERTHANOREQUALTO SHIFTLEFT SHIFTRIGHT PLUS MINUS
// DIVIDE MOD DOTMBR POINTERTOMBR PLUSPLUS MINUSMINUS "sizeof" "__alignof__"
// SCOPE "dynamic_cast" "static_cast" "reinterpret_cast" "const_cast" "typeid"
// DECIMALINT HEXADECIMALINT CharLiteral WCharLiteral WStringLiteral FLOATONE
// FLOATTWO NOT "new" "_cdecl" "__cdecl" "_near" "__near" "_far" "__far"
// "__interrupt" "pascal" "_pascal" "__pascal" "delete"
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_143(
    _tokenSet_143_data_, 12);
const unsigned long CPPParser::_tokenSet_144_data_[] = {
    917856UL, 3UL, 3892445184UL, 4293918720UL, 32767UL, 0UL,
    0UL,      0UL, 0UL,          0UL,          0UL,     0UL};
// LESSTHAN GREATERTHAN SEMICOLON RCURLY ASSIGNEQUAL COLON LPAREN RPAREN
// COMMA RSQUARE STAR AMPERSAND ELLIPSIS TIMESEQUAL DIVIDEEQUAL MINUSEQUAL
// PLUSEQUAL MODEQUAL SHIFTLEFTEQUAL SHIFTRIGHTEQUAL BITWISEANDEQUAL
// BITWISEXOREQUAL
// BITWISEOREQUAL QUESTIONMARK OR AND BITWISEOR BITWISEXOR NOTEQUAL EQUAL
// LESSTHANOREQUALTO GREATERTHANOREQUALTO SHIFTLEFT SHIFTRIGHT PLUS MINUS
// DIVIDE MOD DOTMBR POINTERTOMBR
const ANTLR_USE_NAMESPACE(antlr) BitSet CPPParser::_tokenSet_144(
    _tokenSet_144_data_, 12);
