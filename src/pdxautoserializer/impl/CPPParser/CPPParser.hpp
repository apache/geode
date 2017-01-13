/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef INC_CPPParser_hpp_
#define INC_CPPParser_hpp_

#include <antlr/config.hpp>
/* $ANTLR 2.7.7 (20130409): "CPP_parser.g" -> "CPPParser.hpp"$ */
#include <antlr/TokenStream.hpp>
#include <antlr/TokenBuffer.hpp>
#include "STDCTokenTypes.hpp"
#include <antlr/LLkParser.hpp>

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
extern void process_line_directive(const char *, const char *);

// Declared in main.cpp
extern int deferredLineCount;

#line 30 "CPPParser.hpp"
class CUSTOM_API CPPParser : public ANTLR_USE_NAMESPACE(antlr) LLkParser,
                             public STDCTokenTypes {
#line 191 "CPP_parser.g"

 public:
#define CPPParser_MaxQualifiedItemSize 500

  // These codes are not stored with symbol names in CPPSymbol,
  //   but they are available for development
  // Can't bitwise-OR enum elements together, this must be an int
  typedef unsigned long TypeSpecifier;  // note: must be at least 16 bits
  typedef unsigned long TypeQualifier;

#define tsInvalid 0x0
#define tsVOID 0x1
#define tsCHAR 0x2
#define tsSHORT 0x4
#define tsINT 0x8
#define tsLONG 0x10
#define tsFLOAT 0x20
#define tsDOUBLE 0x40
#define tsSIGNED 0x80
#define tsUNSIGNED 0x100
#define tsTYPEID 0x200
#define tsSTRUCT 0x400
#define tsENUM 0x800
#define tsUNION 0x1000
#define tsCLASS 0x2000
#define tsWCHAR_T 0x4000
#define tsBOOL 0x8000

#define tqInvalid 0x0
#define tqCONST 0x1
#define tqVOLATILE 0x2
#define tqGFEXCLUDE 0x4
#define tqGFINCLUDE 0x8
#define tqGFID 0x10
#define tqGFUNREAD 0x20

  enum StorageClass {
    scInvalid = 0,
    scAUTO = 1,
    scREGISTER,
    scSTATIC,
    scEXTERN,
    scMUTABLE
  };

  enum FunctionSpecifier {
    fsInvalid = 0,
    fsVIRTUAL,
    fsINLINE,
    fsEXPLICIT,
    fsFRIEND
  };

  // JEL 3/26/96 changed to allow ORing of values
  // DW 10/04/05 These codes are stored with the item in CPPSymbol in the
  // dictionary See declaratorID
  typedef int QualifiedItem;
#define qiInvalid 0x0
#define qiType 0x1  // includes enum, class, typedefs, namespace
#define qiDtor 0x2
#define qiCtor 0x4
#define qiOperator 0x8
#define qiPtrMember 0x10
#define qiVar 0x20
#define qiFun 0x40
#define qiNamespace 0x50

 protected:
  // Symbol table management stuff
  CPPDictionary *symbols;
  int templateParameterScope;
  int externalScope;
  int anyType;
  int anyNonType;

  bool _td;               // For typedef
  bool _fd;               // For friend
  StorageClass _sc;       // For storage class
  TypeQualifier _tq;      // For type qualifier
  TypeSpecifier _ts;      // For type specifier
  FunctionSpecifier _fs;  // For declaration specifier

  int functionDefinition;  // 0 = Function definition not being parsed
  // 1 = Parsing function name
  // 2 = Parsing function parameter list
  // 3 = Parsing function block

  char qualifierPrefix[CPPParser_MaxQualifiedItemSize + 1];
  char *enclosingClass;
  int assign_stmt_RHS_found;
  bool in_parameter_list;
  bool K_and_R;  // used to distinguish old K & R parameter definitions
  bool in_return;
  bool is_address;
  bool is_pointer;

  // Limit lookahead for qualifiedItemIs()
  enum { MaxTemplateTokenScan = 200 };

 public:
  void init();

 protected:
  // Semantic interface in Support.cpp;
  // You could subclass and redefine these functions
  // so you don't have to mess with the grammar itself.

  // Symbol stuff
  virtual int qualifiedItemIsOneOf(QualifiedItem qiFlags,
                                   int lookahead_offset = 0);
  virtual QualifiedItem qualifiedItemIs(int lookahead_offset = 0);
  virtual int skipTemplateQualifiers(int &kInOut);
  virtual int skipNestedParens(int &kInOut);
  virtual int scopedItem(int k = 1);
  virtual int finalQualifier(int k = 1);
  virtual int isTypeName(const char *s);
  virtual int isClassName(const char *s);
  virtual void end_of_stmt();

  // Scoping stuff
  virtual void enterNewLocalScope();
  virtual void exitLocalScope();
  virtual void enterExternalScope();
  virtual void exitExternalScope();

  // namespaceEnd stuff
  virtual void exitNamespaceScope();

  // Aggregate stuff
  virtual void classForwardDeclaration(TypeSpecifier, FunctionSpecifier,
                                       const char *);
  virtual void beginClassDefinition(TypeSpecifier, const char *);
  virtual void endClassDefinition();
  virtual void beginEnumDefinition(const char *);
  virtual void endEnumDefinition();
  virtual void enumElement(const char *);

  // Declaration and definition stuff
  virtual void declarationSpecifier(bool, bool, StorageClass, TypeQualifier,
                                    TypeSpecifier, FunctionSpecifier);
  virtual void beginDeclaration();
  virtual void endDeclaration();
  virtual void beginConstructorDeclaration(const char *);
  virtual void endConstructorDeclaration();
  virtual void beginDestructorDeclaration(const char *);
  virtual void endDestructorDeclaration();
  virtual void beginParameterDeclaration();
  virtual void beginFieldDeclaration();
  virtual void beginFunctionDefinition();
  virtual void endFunctionDefinition();
  virtual void functionParameterList();
  virtual void functionEndParameterList(int def);
  virtual void beginConstructorDefinition();
  virtual void endConstructorDefinition();
  virtual void beginDestructorDefinition();
  virtual void endDestructorDefinition();

  // Declarator stuff
  virtual void declaratorID(
      const char *, QualifiedItem);  // This stores new symbol with its type.
  virtual void declaratorArray();
  virtual void declaratorParameterList(int def);
  virtual void declaratorEndParameterList(int def);

  // template stuff
  virtual void templateTypeParameter(const char *);
  virtual void beginTemplateDeclaration();
  virtual void endTemplateDeclaration();
  virtual void beginTemplateDefinition();
  virtual void endTemplateDefinition();
  virtual void beginTemplateParameterList();
  virtual void endTemplateParameterList();

  // exception stuff
  virtual void exceptionBeginHandler();
  virtual void exceptionEndHandler();
  virtual void panic(const char *);

  // myCode functions ready for overriding in MyCode subclass
  // Include application code functions here
  virtual void myCode_pre_processing(int, char *[]);
  virtual void myCode_post_processing();
  virtual void myCode_end_of_stmt();
  virtual void myCode_function_direct_declarator(const char *);

  // Auto-serializer specific stuff
  virtual void gfArraySize(const char *id) {}
  virtual void gfArrayElemSize(const char *id) {}
#line 34 "CPPParser.hpp"
 public:
  void initializeASTFactory(ANTLR_USE_NAMESPACE(antlr) ASTFactory &factory);

 protected:
  CPPParser(ANTLR_USE_NAMESPACE(antlr) TokenBuffer &tokenBuf, int k);

 public:
  CPPParser(ANTLR_USE_NAMESPACE(antlr) TokenBuffer &tokenBuf);

 protected:
  CPPParser(ANTLR_USE_NAMESPACE(antlr) TokenStream &lexer, int k);

 public:
  CPPParser(ANTLR_USE_NAMESPACE(antlr) TokenStream &lexer);
  CPPParser(const ANTLR_USE_NAMESPACE(antlr) ParserSharedInputState &state);
  int getNumTokens() const { return CPPParser::NUM_TOKENS; }
  const char *getTokenName(int type) const {
    if (type > getNumTokens()) return 0;
    return CPPParser::tokenNames[type];
  }
  const char *const *getTokenNames() const { return CPPParser::tokenNames; }

 public:
  void translation_unit();

 public:
  void external_declaration();

 public:
  CPPParser::TypeSpecifier class_specifier();

 public:
  void class_decl_or_def(FunctionSpecifier fs);

 public:
  void init_declarator_list();

 public:
  void enum_specifier();

 public:
  void declaration_specifiers();

 public:
  void function_declarator(int definition);

 public:
  void declaration();

 public:
  void template_head();

 public:
  CPPParser::FunctionSpecifier function_specifier();

 public:
  void dtor_head(int definition);

 public:
  void dtor_body();

 public:
  void ctor_decl_spec();

 public:
  void ctor_definition();

 public:
  char *scope_override();

 public:
  void conversion_function_decl_or_def();

 public:
  void function_definition();

 public:
  void namespace_definition();

 public:
  void namespace_alias_definition();

 public:
  char *qualified_id();

 public:
  void member_declaration();

 public:
  void member_declarator_list();

 public:
  void ctor_declarator(int definition);

 public:
  void compound_statement();

 public:
  void class_head();

 public:
  void access_specifier();

 public:
  void linkage_specification();

 public:
  void using_statement();

 public:
  void template_argument_list();

 public:
  void base_clause();

 public:
  CPPParser::StorageClass storage_class_specifier();

 public:
  CPPParser::TypeQualifier type_qualifier();

 public:
  CPPParser::TypeSpecifier type_specifier();

 public:
  CPPParser::TypeSpecifier simple_type_specifier();

 public:
  char *qualified_type();

 public:
  void class_prefix();

 public:
  void expression();

 public:
  void base_specifier();

 public:
  void enumerator_list();

 public:
  void enumerator();

 public:
  void constant_expression();

 public:
  void optor();

 public:
  void typeID();

 public:
  void init_declarator();

 public:
  void declarator();

 public:
  void initializer();

 public:
  void expression_list();

 public:
  void remainder_expression();

 public:
  void member_declarator();

 public:
  void ptr_operator();

 public:
  void direct_declarator();

 public:
  void parameter_list();

 public:
  void exception_specification();

 public:
  void declarator_suffix();

 public:
  void template_parameter_list();

 public:
  void function_direct_declarator(int definition);

 public:
  void ctor_head();

 public:
  void ctor_body();

 public:
  char *qualified_ctor_id();

 public:
  void ctor_initializer();

 public:
  void superclass_init();

 public:
  void dtor_decl_spec();

 public:
  void dtor_declarator(int definition);

 public:
  void parameter_declaration_list();

 public:
  void parameter_declaration();

 public:
  void abstract_declarator();

 public:
  void type_id();

 public:
  void abstract_declarator_suffix();

 public:
  void cv_qualifier_seq();

 public:
  void template_parameter();

 public:
  void type_parameter();

 public:
  void assigned_type_name();

 public:
  void template_id();

 public:
  void template_argument();

 public:
  void shift_expression();

 public:
  void statement_list();

 public:
  void statement();

 public:
  void block_declaration();

 public:
  void labeled_statement();

 public:
  void case_statement();

 public:
  void default_statement();

 public:
  void selection_statement();

 public:
  void iteration_statement();

 public:
  void jump_statement();

 public:
  void try_block();

 public:
  void throw_statement();

 public:
  void simple_declaration();

 public:
  void asm_definition();

 public:
  void handler();

 public:
  void exception_declaration();

 public:
  void assignment_expression();

 public:
  void conditional_expression();

 public:
  void logical_or_expression();

 public:
  void logical_and_expression();

 public:
  void inclusive_or_expression();

 public:
  void exclusive_or_expression();

 public:
  void and_expression();

 public:
  void equality_expression();

 public:
  void relational_expression();

 public:
  void additive_expression();

 public:
  void multiplicative_expression();

 public:
  void pm_expression();

 public:
  void cast_expression();

 public:
  void unary_expression();

 public:
  void postfix_expression();

 public:
  void unary_operator();

 public:
  void new_expression();

 public:
  void delete_expression();

 public:
  void primary_expression();

 public:
  void id_expression();

 public:
  void constant();

 public:
  void new_type_id();

 public:
  void new_initializer();

 public:
  void new_declarator();

 public:
  void direct_new_declarator();

 public:
  void ptr_to_member();

 public:
  void optor_simple_tokclass();

 public:
  ANTLR_USE_NAMESPACE(antlr) RefAST getAST() { return returnAST; }

 protected:
  ANTLR_USE_NAMESPACE(antlr) RefAST returnAST;

 private:
  static const char *tokenNames[];
#ifndef NO_STATIC_CONSTS
  static const int NUM_TOKENS = 193;
#else
  enum { NUM_TOKENS = 193 };
#endif

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
  static const unsigned long _tokenSet_9_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_9;
  static const unsigned long _tokenSet_10_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_10;
  static const unsigned long _tokenSet_11_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_11;
  static const unsigned long _tokenSet_12_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_12;
  static const unsigned long _tokenSet_13_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_13;
  static const unsigned long _tokenSet_14_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_14;
  static const unsigned long _tokenSet_15_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_15;
  static const unsigned long _tokenSet_16_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_16;
  static const unsigned long _tokenSet_17_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_17;
  static const unsigned long _tokenSet_18_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_18;
  static const unsigned long _tokenSet_19_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_19;
  static const unsigned long _tokenSet_20_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_20;
  static const unsigned long _tokenSet_21_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_21;
  static const unsigned long _tokenSet_22_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_22;
  static const unsigned long _tokenSet_23_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_23;
  static const unsigned long _tokenSet_24_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_24;
  static const unsigned long _tokenSet_25_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_25;
  static const unsigned long _tokenSet_26_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_26;
  static const unsigned long _tokenSet_27_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_27;
  static const unsigned long _tokenSet_28_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_28;
  static const unsigned long _tokenSet_29_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_29;
  static const unsigned long _tokenSet_30_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_30;
  static const unsigned long _tokenSet_31_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_31;
  static const unsigned long _tokenSet_32_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_32;
  static const unsigned long _tokenSet_33_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_33;
  static const unsigned long _tokenSet_34_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_34;
  static const unsigned long _tokenSet_35_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_35;
  static const unsigned long _tokenSet_36_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_36;
  static const unsigned long _tokenSet_37_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_37;
  static const unsigned long _tokenSet_38_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_38;
  static const unsigned long _tokenSet_39_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_39;
  static const unsigned long _tokenSet_40_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_40;
  static const unsigned long _tokenSet_41_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_41;
  static const unsigned long _tokenSet_42_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_42;
  static const unsigned long _tokenSet_43_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_43;
  static const unsigned long _tokenSet_44_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_44;
  static const unsigned long _tokenSet_45_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_45;
  static const unsigned long _tokenSet_46_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_46;
  static const unsigned long _tokenSet_47_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_47;
  static const unsigned long _tokenSet_48_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_48;
  static const unsigned long _tokenSet_49_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_49;
  static const unsigned long _tokenSet_50_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_50;
  static const unsigned long _tokenSet_51_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_51;
  static const unsigned long _tokenSet_52_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_52;
  static const unsigned long _tokenSet_53_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_53;
  static const unsigned long _tokenSet_54_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_54;
  static const unsigned long _tokenSet_55_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_55;
  static const unsigned long _tokenSet_56_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_56;
  static const unsigned long _tokenSet_57_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_57;
  static const unsigned long _tokenSet_58_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_58;
  static const unsigned long _tokenSet_59_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_59;
  static const unsigned long _tokenSet_60_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_60;
  static const unsigned long _tokenSet_61_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_61;
  static const unsigned long _tokenSet_62_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_62;
  static const unsigned long _tokenSet_63_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_63;
  static const unsigned long _tokenSet_64_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_64;
  static const unsigned long _tokenSet_65_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_65;
  static const unsigned long _tokenSet_66_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_66;
  static const unsigned long _tokenSet_67_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_67;
  static const unsigned long _tokenSet_68_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_68;
  static const unsigned long _tokenSet_69_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_69;
  static const unsigned long _tokenSet_70_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_70;
  static const unsigned long _tokenSet_71_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_71;
  static const unsigned long _tokenSet_72_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_72;
  static const unsigned long _tokenSet_73_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_73;
  static const unsigned long _tokenSet_74_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_74;
  static const unsigned long _tokenSet_75_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_75;
  static const unsigned long _tokenSet_76_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_76;
  static const unsigned long _tokenSet_77_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_77;
  static const unsigned long _tokenSet_78_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_78;
  static const unsigned long _tokenSet_79_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_79;
  static const unsigned long _tokenSet_80_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_80;
  static const unsigned long _tokenSet_81_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_81;
  static const unsigned long _tokenSet_82_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_82;
  static const unsigned long _tokenSet_83_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_83;
  static const unsigned long _tokenSet_84_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_84;
  static const unsigned long _tokenSet_85_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_85;
  static const unsigned long _tokenSet_86_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_86;
  static const unsigned long _tokenSet_87_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_87;
  static const unsigned long _tokenSet_88_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_88;
  static const unsigned long _tokenSet_89_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_89;
  static const unsigned long _tokenSet_90_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_90;
  static const unsigned long _tokenSet_91_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_91;
  static const unsigned long _tokenSet_92_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_92;
  static const unsigned long _tokenSet_93_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_93;
  static const unsigned long _tokenSet_94_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_94;
  static const unsigned long _tokenSet_95_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_95;
  static const unsigned long _tokenSet_96_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_96;
  static const unsigned long _tokenSet_97_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_97;
  static const unsigned long _tokenSet_98_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_98;
  static const unsigned long _tokenSet_99_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_99;
  static const unsigned long _tokenSet_100_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_100;
  static const unsigned long _tokenSet_101_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_101;
  static const unsigned long _tokenSet_102_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_102;
  static const unsigned long _tokenSet_103_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_103;
  static const unsigned long _tokenSet_104_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_104;
  static const unsigned long _tokenSet_105_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_105;
  static const unsigned long _tokenSet_106_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_106;
  static const unsigned long _tokenSet_107_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_107;
  static const unsigned long _tokenSet_108_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_108;
  static const unsigned long _tokenSet_109_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_109;
  static const unsigned long _tokenSet_110_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_110;
  static const unsigned long _tokenSet_111_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_111;
  static const unsigned long _tokenSet_112_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_112;
  static const unsigned long _tokenSet_113_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_113;
  static const unsigned long _tokenSet_114_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_114;
  static const unsigned long _tokenSet_115_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_115;
  static const unsigned long _tokenSet_116_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_116;
  static const unsigned long _tokenSet_117_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_117;
  static const unsigned long _tokenSet_118_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_118;
  static const unsigned long _tokenSet_119_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_119;
  static const unsigned long _tokenSet_120_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_120;
  static const unsigned long _tokenSet_121_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_121;
  static const unsigned long _tokenSet_122_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_122;
  static const unsigned long _tokenSet_123_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_123;
  static const unsigned long _tokenSet_124_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_124;
  static const unsigned long _tokenSet_125_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_125;
  static const unsigned long _tokenSet_126_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_126;
  static const unsigned long _tokenSet_127_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_127;
  static const unsigned long _tokenSet_128_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_128;
  static const unsigned long _tokenSet_129_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_129;
  static const unsigned long _tokenSet_130_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_130;
  static const unsigned long _tokenSet_131_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_131;
  static const unsigned long _tokenSet_132_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_132;
  static const unsigned long _tokenSet_133_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_133;
  static const unsigned long _tokenSet_134_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_134;
  static const unsigned long _tokenSet_135_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_135;
  static const unsigned long _tokenSet_136_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_136;
  static const unsigned long _tokenSet_137_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_137;
  static const unsigned long _tokenSet_138_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_138;
  static const unsigned long _tokenSet_139_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_139;
  static const unsigned long _tokenSet_140_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_140;
  static const unsigned long _tokenSet_141_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_141;
  static const unsigned long _tokenSet_142_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_142;
  static const unsigned long _tokenSet_143_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_143;
  static const unsigned long _tokenSet_144_data_[];
  static const ANTLR_USE_NAMESPACE(antlr) BitSet _tokenSet_144;
};

#endif /*INC_CPPParser_hpp_*/
