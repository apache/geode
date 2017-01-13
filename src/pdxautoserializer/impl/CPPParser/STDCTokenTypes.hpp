/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef INC_STDCTokenTypes_hpp_
#define INC_STDCTokenTypes_hpp_

/* $ANTLR 2.7.7 (20130409): "CPP_parser.g" -> "STDCTokenTypes.hpp"$ */

#ifndef CUSTOM_API
#define CUSTOM_API
#endif

#ifdef __cplusplus
struct CUSTOM_API STDCTokenTypes {
#endif
  enum {
    EOF_ = 1,
    LITERAL_template = 4,
    LESSTHAN = 5,
    GREATERTHAN = 6,
    LITERAL_typedef = 7,
    SEMICOLON = 8,
    LITERAL_enum = 9,
    ID = 10,
    LCURLY = 11,
    LITERAL_inline = 12,
    LITERAL_friend = 13,
    LITERAL_namespace = 14,
    LITERAL_antlrTrace_on = 15,
    LITERAL_antlrTrace_off = 16,
    RCURLY = 17,
    ASSIGNEQUAL = 18,
    COLON = 19,
    LITERAL_extern = 20,
    StringLiteral = 21,
    LITERAL_struct = 22,
    LITERAL_union = 23,
    LITERAL_class = 24,
    LITERAL__stdcall = 25,
    LITERAL___stdcall = 26,
    LITERAL_GFEXCLUDE = 27,
    LITERAL_GFINCLUDE = 28,
    LITERAL_GFID = 29,
    LITERAL_GFUNREAD = 30,
    LITERAL_GFARRAYSIZE = 31,
    LPAREN = 32,
    RPAREN = 33,
    LITERAL_GFARRAYSIZES = 34,
    LITERAL_GFARRAYELEMSIZE = 35,
    LITERAL_auto = 36,
    LITERAL_register = 37,
    LITERAL_static = 38,
    LITERAL_mutable = 39,
    LITERAL__inline = 40,
    LITERAL___inline = 41,
    LITERAL_virtual = 42,
    LITERAL_explicit = 43,
    LITERAL_typename = 44,
    LITERAL_char = 45,
    LITERAL_wchar_t = 46,
    LITERAL_bool = 47,
    LITERAL_short = 48,
    LITERAL_int = 49,
    // "_int8" = 50
    // "__int8" = 51
    // "int8_t" = 52
    // "_int16" = 53
    // "__int16" = 54
    // "int16_t" = 55
    // "_int32" = 56
    // "__int32" = 57
    // "int32_t" = 58
    // "_int64" = 59
    // "__int64" = 60
    // "int64_t" = 61
    // "uint8_t" = 62
    // "uint16_t" = 63
    // "uint32_t" = 64
    // "uint64_t" = 65
    // "_w64" = 66
    // "__w64" = 67
    LITERAL_long = 68,
    LITERAL_signed = 69,
    LITERAL_unsigned = 70,
    LITERAL_float = 71,
    LITERAL_double = 72,
    LITERAL_void = 73,
    LITERAL__declspec = 74,
    LITERAL___declspec = 75,
    LITERAL_const = 76,
    LITERAL___const = 77,
    LITERAL_volatile = 78,
    LITERAL___volatile__ = 79,
    LITERAL_GFIGNORE = 80,
    COMMA = 81,
    LITERAL_public = 82,
    LITERAL_protected = 83,
    LITERAL_private = 84,
    OPERATOR = 85,
    LITERAL_this = 86,
    LITERAL_true = 87,
    LITERAL_false = 88,
    OCTALINT = 89,
    LSQUARE = 90,
    RSQUARE = 91,
    TILDE = 92,
    STAR = 93,
    AMPERSAND = 94,
    ELLIPSIS = 95,
    LITERAL_throw = 96,
    LITERAL_using = 97,
    LITERAL_case = 98,
    LITERAL_default = 99,
    LITERAL_if = 100,
    LITERAL_else = 101,
    LITERAL_switch = 102,
    LITERAL_while = 103,
    LITERAL_do = 104,
    LITERAL_for = 105,
    LITERAL_goto = 106,
    LITERAL_continue = 107,
    LITERAL_break = 108,
    LITERAL_return = 109,
    LITERAL_try = 110,
    LITERAL_catch = 111,
    LITERAL_asm = 112,
    LITERAL__asm = 113,
    LITERAL___asm = 114,
    LITERAL___asm__ = 115,
    TIMESEQUAL = 116,
    DIVIDEEQUAL = 117,
    MINUSEQUAL = 118,
    PLUSEQUAL = 119,
    MODEQUAL = 120,
    SHIFTLEFTEQUAL = 121,
    SHIFTRIGHTEQUAL = 122,
    BITWISEANDEQUAL = 123,
    BITWISEXOREQUAL = 124,
    BITWISEOREQUAL = 125,
    QUESTIONMARK = 126,
    OR = 127,
    AND = 128,
    BITWISEOR = 129,
    BITWISEXOR = 130,
    NOTEQUAL = 131,
    EQUAL = 132,
    LESSTHANOREQUALTO = 133,
    GREATERTHANOREQUALTO = 134,
    SHIFTLEFT = 135,
    SHIFTRIGHT = 136,
    PLUS = 137,
    MINUS = 138,
    DIVIDE = 139,
    MOD = 140,
    DOTMBR = 141,
    POINTERTOMBR = 142,
    PLUSPLUS = 143,
    MINUSMINUS = 144,
    LITERAL_sizeof = 145,
    LITERAL___alignof__ = 146,
    SCOPE = 147,
    DOT = 148,
    POINTERTO = 149,
    LITERAL_dynamic_cast = 150,
    LITERAL_static_cast = 151,
    LITERAL_reinterpret_cast = 152,
    LITERAL_const_cast = 153,
    LITERAL_typeid = 154,
    DECIMALINT = 155,
    HEXADECIMALINT = 156,
    CharLiteral = 157,
    WCharLiteral = 158,
    WStringLiteral = 159,
    FLOATONE = 160,
    FLOATTWO = 161,
    NOT = 162,
    LITERAL_new = 163,
    LITERAL__cdecl = 164,
    LITERAL___cdecl = 165,
    LITERAL__near = 166,
    LITERAL___near = 167,
    LITERAL__far = 168,
    LITERAL___far = 169,
    LITERAL___interrupt = 170,
    LITERAL_pascal = 171,
    LITERAL__pascal = 172,
    LITERAL___pascal = 173,
    LITERAL_delete = 174,
    Whitespace = 175,
    Comment = 176,
    CPPComment = 177,
    PREPROC_DIRECTIVE = 178,
    LineDirective = 179,
    Space = 180,
    Pragma = 181,
    Error = 182,
    EndOfLine = 183,
    Escape = 184,
    Digit = 185,
    Decimal = 186,
    LongSuffix = 187,
    UnsignedSuffix = 188,
    FloatSuffix = 189,
    Exponent = 190,
    Vocabulary = 191,
    Number = 192,
    NULL_TREE_LOOKAHEAD = 3
  };
#ifdef __cplusplus
};
#endif
#endif /*INC_STDCTokenTypes_hpp_*/
