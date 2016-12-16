/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
**  oql.g
**
** Built with Antlr 2.7.4
**   java antlr.Tool oql.g
*/

header {
package org.apache.geode.cache.query.internal.parse;
import java.util.*;
import org.apache.geode.cache.query.internal.types.*;
}

options {
    language="Java";
}

class OQLLexer extends Lexer ;
options {
    k=2;
    charVocabulary = '\u0000'..'\uFFFE';
    testLiterals=false;     // don't automatically test for literals
    caseSensitive=false;
    caseSensitiveLiterals = false;
    defaultErrorHandler = false;
}

{
    static {
        // Set up type names for verbose AST printing
        antlr.BaseAST.setVerboseStringConversion(true, OQLParser._tokenNames);
    }

    protected Token makeToken(int t) {
        Token tok = super.makeToken(t);
        if (tok.getType() == EOF) tok.setText("EOF");
        if (tok.getText() == null) {
          tok.setText("(no text)");
        }
        return tok;
    }
}    


        /* punctuation */
TOK_RPAREN  :       ')'             ;
TOK_LPAREN  :       '('             ;
TOK_COMMA   :       ','             ;
TOK_SEMIC   :       ';'             ;
TOK_DOTDOT  :       ".."            ;
TOK_COLON   :       ':'             ;
TOK_DOT     :       '.'             ;
TOK_INDIRECT:       '-' '>'         { $setType(TOK_DOT); } ;
TOK_CONCAT  :       '|' '|'         ;
TOK_EQ      :       '='             ;
TOK_PLUS    :       '+'             ;
TOK_MINUS   :       '-'             ;
TOK_SLASH   :       '/'             ;
TOK_STAR    :       '*'             ;
TOK_LE      :       '<' '='         ;
TOK_GE      :       '>' '='         ;
TOK_NE      :       '<' '>'         ;
TOK_NE_ALT  :       '!' '='         { $setType(TOK_NE); }  ;
TOK_LT      :       '<'             ;
TOK_GT      :       '>'             ;
TOK_LBRACK  :       '['             ;
TOK_RBRACK  :       ']'             ;
TOK_DOLLAR  :       '$'             ;

/* Character Classes */
protected
LETTER : ('\u0061'..'\u007a' |
       '\u00c0'..'\u00d6' |
       '\u00a1'..'\u00bf' |
       '\u00d8'..'\u00f6' |
       '\u00f8'..'\u00ff' |
       '\u0100'..'\u065f' |
       '\u066a'..'\u06ef' |
       '\u06fa'..'\u0965' |
       '\u0970'..'\u09e5' |
       '\u09f0'..'\u0a65' |
       '\u0a70'..'\u0ae5' |
       '\u0af0'..'\u0b65' |
       '\u0b70'..'\u0be6' |
       '\u0bf0'..'\u0c65' |
       '\u0c70'..'\u0ce5' |
       '\u0cf0'..'\u0d65' |
       '\u0d70'..'\u0e4f' |
       '\u0e5a'..'\u0ecf' |
       '\u0eda'..'\u103f' |
       '\u104a'..'\u1fff' |
       '\u2000'..'\u206f' |       
       '\u3040'..'\u318f' |
       '\u3300'..'\u337f' |
       '\u3400'..'\u3d2d' |
       '\u4e00'..'\u9fff' |
       '\uf900'..'\ufaff')
    ;

protected
DIGIT : ('\u0030'..'\u0039' |
       '\u0660'..'\u0669' |
       '\u06f0'..'\u06f9' |
       '\u0966'..'\u096f' |
       '\u09e6'..'\u09ef' |
       '\u0a66'..'\u0a6f' |
       '\u0ae6'..'\u0aef' |
       '\u0b66'..'\u0b6f' |
       '\u0be7'..'\u0bef' |
       '\u0c66'..'\u0c6f' |
       '\u0ce6'..'\u0cef' |
       '\u0d66'..'\u0d6f' |
       '\u0e50'..'\u0e59' |
       '\u0ed0'..'\u0ed9' |
       '\u1040'..'\u1049')
    ;

protected
ALL_UNICODE : ('\u0061'..'\ufffd')	
    ;

/*
 * Names
 */
protected
NameFirstCharacter:
        ( LETTER | '_' )
    ;

protected
NameCharacter:
        ( LETTER | '_' | DIGIT | '$') // an internal $ is used for internal use identifiers
    ;

protected
RegionNameCharacter:
        ( ALL_UNICODE | '_' | DIGIT | '+' | '-' | ':' | '#' | '@' | '$') // an internal $ is used for internal use identifiers
    ;

QuotedIdentifier
       options {testLiterals=false;}
            :
    
            '"'! NameFirstCharacter ( NameCharacter )* '"'!
    ;



Identifier
        options {testLiterals=true;}
            :

            NameFirstCharacter ( NameCharacter )*
    ;



RegionPath :  ( ( (TOK_SLASH ( RegionNameCharacter )+ )+ ) | 
			  ( (TOK_SLASH StringLiteral )+ ) ) 
    ;


/* Numbers */
// a numeric literal
NUM_INT
	{boolean isDecimal=false; Token t=null;}
    :   (	'0' {isDecimal = true;} // special case for just '0'
			(	'x'
				(						// hex
					// the 'e'|'E' and float suffix stuff look
					// like hex digits, hence the (...)+ doesn't
					// know when to stop: ambig.  ANTLR resolves
					// it correctly by matching immediately.  It
					// is therefor ok to hush warning.
					options {
						warnWhenFollowAmbig=false;
					}
				:	HEX_DIGIT
				)+

			|	//float or double with leading zero
				(('0'..'9')+ ('.'|EXPONENT|FLOAT_SUFFIX)) => ('0'..'9')+

			|	('0'..'7')+					// octal
			)?
		|	('1'..'9') ('0'..'9')*  {isDecimal=true;}		// non-zero decimal
		)
		(	'l' { _ttype = NUM_LONG; }

		// only check to see if it's a float if looks like decimal so far
		|	{isDecimal}?
            (   '.' ('0'..'9')* (EXPONENT)? (f2:FLOAT_SUFFIX {t=f2;})?
            |   EXPONENT (f3:FLOAT_SUFFIX {t=f3;})?
            |   f4:FLOAT_SUFFIX {t=f4;}
            )
            {
			if (t != null && t.getText().toUpperCase() .indexOf('F') >= 0) {
                _ttype = NUM_FLOAT;
			}
            else {
	           	_ttype = NUM_DOUBLE; // assume double
			}
			}
        )?
	;


// a couple protected methods to assist in matching floating point numbers
protected
EXPONENT
	:	'e' ('+'|'-')? ('0'..'9')+
	;


protected
FLOAT_SUFFIX
	:	'f'|'d'
	;

// hexadecimal digit
protected
HEX_DIGIT
	:	('0'..'9'| 'a'..'f')
	;


protected
QUOTE
        :       '\''
        ;

StringLiteral :
        QUOTE!
        (
            QUOTE QUOTE!
        |   '\n'                { newline(); }
        |   ~( '\'' | '\n' )
        )*
        QUOTE!

    ;


// Whitespace -- ignored
WS	:	(	' '
		|	'\t'
		|	'\f'
			// handle newlines
		|	(	options {generateAmbigWarnings=false;}
			:	"\r\n"  // Evil DOS
			|	'\r'    // Macintosh
			|	'\n'    // Unix (the right way)
			)
			{ newline(); }
		)+
		{ _ttype = Token.SKIP; }
	;


// Single-line comments
SL_COMMENT
	:	"--"
		(~('\n'|'\r'))* ('\n'|'\r'('\n')?)?
		{$setType(Token.SKIP); newline();}
	;

// multiple-line comments
ML_COMMENT
	:	"/*"
		(	/*	'\r' '\n' can be matched in one alternative or by matching
				'\r' in one iteration and '\n' in another.  I am trying to
				handle any flavor of newline that comes in, but the language
				that allows both "\r\n" and "\r" and "\n" to all be valid
				newline is ambiguous.  Consequently, the resulting grammar
				must be ambiguous.  I'm shutting this warning off.
			 */
			options {
				generateAmbigWarnings=false;
			}
		:
			{ LA(2)!='/' }? '*'
		|	'\r' '\n'		{newline();}
		|	'\r'			{newline();}
		|	'\n'			{newline();}
		|	~('*'|'\n'|'\r')
		)*
		"*/"
		{$setType(Token.SKIP);}
	;




/***************************** OQL PARSER *************************************/

class OQLParser
extends Parser("org.apache.geode.cache.query.internal.parse.UtilParser");

options {
    buildAST = true;
    k = 2;
    codeGenMakeSwitchThreshold = 3;
    codeGenBitsetTestThreshold = 4;
    defaultErrorHandler = false;
}

tokens {
    QUERY_PROGRAM;
    QUALIFIED_NAME;
    QUERY_PARAM;
    ITERATOR_DEF;
    PROJECTION_ATTRS;
    PROJECTION;
    TYPECAST;
    COMBO;
    METHOD_INV;
    POSTFIX; 
    OBJ_CONSTRUCTOR;
    IMPORTS;
    SORT_CRITERION;
    LIMIT;
    HINT;
    AGG_FUNC;
    SUM;
    AVG;
    COUNT;
    MAX;
    MIN;
}

queryProgram :
		( traceCommand )?
       	( 
       
       		( declaration ( TOK_SEMIC! declaration )* ( TOK_SEMIC! query ) ) (TOK_SEMIC!)?
        	|   
        	query (TOK_SEMIC!)?
       	)
       	EOF!
        { #queryProgram =
          #([QUERY_PROGRAM, "queryProgram",
            "org.apache.geode.cache.query.internal.parse.GemFireAST"],
            #queryProgram); }
    ;

traceCommand:
		( 	
			TOK_LT!
			"trace"^<AST=org.apache.geode.cache.query.internal.parse.ASTTrace>
			TOK_GT!
		)
	;
     

loneFromClause :
    iteratorDef
        (
            TOK_COMMA!
            iteratorDef
        )* EOF!
    { #loneFromClause =
          #([LITERAL_from, "from",
            "org.apache.geode.cache.query.internal.parse.ASTCombination"],
            #loneFromClause); }
    ;

loneProjectionAttributes :
    projectionAttributes EOF!
    ;

declaration :

            defineQuery
        |   importQuery
        |   undefineQuery
        |   paramTypeDecl

    ;

importQuery :

        "import"^<AST=org.apache.geode.cache.query.internal.parse.ASTImport>
        qualifiedName
        (
            "as" identifier
        )?

    ;

loneImports :
        importQuery ( TOK_SEMIC! importQuery )*
        (TOK_SEMIC!)?
        EOF!
        // combine into a single node
        { #loneImports = #([IMPORTS, "imports",
            "org.apache.geode.cache.query.internal.parse.GemFireAST"],
                #loneImports); }
    ;

paramTypeDecl :
    
        "declare"^ (queryParam) (TOK_COMMA! queryParam)* type
    ;
        

qualifiedName :

        identifier
        (
            TOK_DOT!
            identifier
        )*
    ;


defineQuery :

        "define"^
            ( "query"! )?
        identifier

        (
            TOK_LPAREN!
            type
            identifier
            (
                TOK_COMMA!
                type
                identifier
            )*
            TOK_RPAREN!
        )?

        "as"!
        query
    ;

undefineQuery :

        "undefine"^
        ( "query"! )?
        identifier
    ;

query :

        (
            selectExpr
        |   expr
        )
    ;

selectExpr :
		( hintCommand )?
        "select"^<AST=org.apache.geode.cache.query.internal.parse.ASTSelect>

        (
            // ambig. betweed this distinct and a distinct conversion expr
            // the first distinct keyword here must be for the select,
            // so take adv. of greedy and turn off warning
            options {
                warnWhenFollowAmbig = false;
            } :

            "distinct" <AST=org.apache.geode.cache.query.internal.parse.ASTDummy>
            | "all" <AST=org.apache.geode.cache.query.internal.parse.ASTDummy>
        )?

        
		(			
          projectionAttributes
		)
        fromClause
        ( whereClause )?
        ( groupClause )?
        ( orderClause )?
        ( limitClause )?
    ;

fromClause :

        "from"^<AST=org.apache.geode.cache.query.internal.parse.ASTCombination>
        iteratorDef
        (
            TOK_COMMA!
            iteratorDef
        )*
    ;

iteratorDef! :

            (identifier "in" ) =>

            id1:identifier
            "in"!
            ex1:expr ( "type"! t1:type )?
            { #iteratorDef = #([ITERATOR_DEF, "iterDef", "org.apache.geode.cache.query.internal.parse.ASTIteratorDef"], #ex1, #id1, #t1); }

        |   ex2:expr
            (
                ( "as"! )?
                id2:identifier
            )?
            ( "type"! t2:type )?

            { #iteratorDef = #([ITERATOR_DEF, "iterDef", "org.apache.geode.cache.query.internal.parse.ASTIteratorDef"], #ex2, #id2, #t2); }
    ;

whereClause :

        "where"! expr
    ;
    
limitClause! :

        "limit" (TOK_DOLLAR NUM_INT
        { #limitClause = #([LIMIT, "limitParam",
               "org.apache.geode.cache.query.internal.parse.ASTParameter"],
                   #NUM_INT); }
         | n:NUM_INT{ #limitClause =#[LIMIT,n.getText(),"org.apache.geode.cache.query.internal.parse.ASTLimit"] ; })
    ;

projectionAttributes :

        (
            projection
            (
                TOK_COMMA!
                projection
            )*
            { #projectionAttributes = #([PROJECTION_ATTRS, "projectionAttrs",
                "org.apache.geode.cache.query.internal.parse.ASTCombination"],
                #projectionAttributes); }

        |   TOK_STAR <AST=org.apache.geode.cache.query.internal.parse.ASTDummy>
        )
    ;

projection!{ AST node  = null;}:
        
            lb1:identifier TOK_COLON!  ( tok1:aggregateExpr{node = #tok1;} | tok2:expr{node = #tok2;})
            { #projection = #([PROJECTION, "projection",
            "org.apache.geode.cache.query.internal.parse.ASTProjection"],  node, #lb1); } 
        |
            (tok3:aggregateExpr{node = #tok3;} | tok4:expr{node = #tok4;})
            (
                "as"
                lb2: identifier
            )?
            { #projection = #([PROJECTION, "projection",
            "org.apache.geode.cache.query.internal.parse.ASTProjection"], node, #lb2); }
    ;


            

groupClause :

        "group"^<AST=org.apache.geode.cache.query.internal.parse.ASTGroupBy>
        "by"!  groupByList

        (
            "having"!
            expr
        )?
    ;
    
hintCommand :
		 	
			TOK_LT!
			"hint"^<AST=org.apache.geode.cache.query.internal.parse.ASTHint>
			hintIdentifier
			(
			  TOK_COMMA! hintIdentifier
			)*
			TOK_GT!
		
	;

      
hintIdentifier :
 	n:StringLiteral{ #hintIdentifier =#[HINT,n.getText(),"org.apache.geode.cache.query.internal.parse.ASTHintIdentifier"] ; }       
;

orderClause :

        "order"^<AST=org.apache.geode.cache.query.internal.parse.ASTOrderBy>
        "by"!
        sortCriterion
        (
            TOK_COMMA! sortCriterion
        )*
    ;

sortCriterion :

        tok:expr { #sortCriterion = #([SORT_CRITERION, "asc", "org.apache.geode.cache.query.internal.parse.ASTSortCriterion"],
            tok); }        
        (
            "asc"! {#sortCriterion.setText("asc");}
        |   "desc"! {#sortCriterion.setText("desc");}

        )?
    ;

expr :

        castExpr

     ;


castExpr
	:	

        // Have to backtrack to see if operator follows.  If no operator
        // follows, it's a typecast.  No semantic checking needed to parse.
        // if it _looks_ like a cast, it _is_ a cast; else it's a "(expr)"
    	(TOK_LPAREN type TOK_RPAREN castExpr)=>
        lp:TOK_LPAREN^<AST=org.apache.geode.cache.query.internal.parse.ASTTypeCast>
                {#lp.setType(TYPECAST); #lp.setText("typecast");}
            type TOK_RPAREN!
        castExpr

    |	orExpr
	;

orExpr
      { boolean cmplx = false; } :
       
            orelseExpr ( "or"! orelseExpr  { cmplx = true; } )*
       { if (cmplx) {
            #orExpr = #([LITERAL_or, "or",
                    "org.apache.geode.cache.query.internal.parse.ASTOr"],
                #orExpr); } }
      ;

orelseExpr 
       { boolean cmplx = false; } :
            andExpr ( "orelse"! andExpr { cmplx = true; } )*
       { if (cmplx) { #orelseExpr = #([LITERAL_orelse, "or"], #orelseExpr); } }
      ;


andExpr 
        { boolean cmplx = false; } :
           quantifierExpr ( "and"! quantifierExpr { cmplx = true; } )*
        { if (cmplx) {
            #andExpr = #([LITERAL_and, "and",
                    "org.apache.geode.cache.query.internal.parse.ASTAnd"],
                    #andExpr); } }
    ;

quantifierExpr :

            "for"^
            "all"!
            inClause TOK_COLON! andthenExpr

        |   
            ("exists" identifier "in" ) => 
            "exists"^
            inClause
            TOK_COLON! andthenExpr

        |   andthenExpr

    ;


inClause :
            identifier "in"! expr
         ;



andthenExpr 
         { boolean cmplx = false; } :
            equalityExpr ( "andthen"! equalityExpr { cmplx = true; } )*
            { if (cmplx) { #andthenExpr = #([LITERAL_andthen, "andthen"], #andthenExpr); } }
         ;


equalityExpr :

            relationalExpr

            (   
                (   (
                        TOK_EQ^<AST=org.apache.geode.cache.query.internal.parse.ASTCompareOp>
                    |   TOK_NE^<AST=org.apache.geode.cache.query.internal.parse.ASTCompareOp>
                    )
                    (   "all"^
                    |   "any"^
                    |   "some"^
                    )?
                relationalExpr )+
    
            |   (  "like"^<AST=org.apache.geode.cache.query.internal.parse.ASTLike>
                relationalExpr )*
            )
    ;

relationalExpr :

        additiveExpr
        (
            (
                TOK_LT^<AST=org.apache.geode.cache.query.internal.parse.ASTCompareOp>
            |   TOK_GT^<AST=org.apache.geode.cache.query.internal.parse.ASTCompareOp>
            |   TOK_LE^<AST=org.apache.geode.cache.query.internal.parse.ASTCompareOp>
            |   TOK_GE^<AST=org.apache.geode.cache.query.internal.parse.ASTCompareOp>
            )

            (
                // Simple comparison of expressions
                additiveExpr

                // Comparison of queries
            |   (
                    "all"^
                |   "any"^
                |   "some"^
                )
                additiveExpr
            )
        )*
    ;

additiveExpr :

        multiplicativeExpr
        (
            (
                TOK_PLUS^
            |   TOK_MINUS^
            |   TOK_CONCAT^
            |   "union"^
            |   "except"^
            )
            multiplicativeExpr
        )*
    ;

multiplicativeExpr :

        inExpr
        (
            (
                TOK_STAR^
            |   TOK_SLASH^
            |   "mod"^
            |   "intersect"^
            )
            inExpr
        )*
    ;

inExpr :

        unaryExpr
        (
            "in"^<AST=org.apache.geode.cache.query.internal.parse.ASTIn> unaryExpr
        )?
    ;

unaryExpr :

        (
            (
                TOK_PLUS!
            |   TOK_MINUS^<AST=org.apache.geode.cache.query.internal.parse.ASTUnary>
            |   "abs"^
            |   "not"^<AST=org.apache.geode.cache.query.internal.parse.ASTUnary>
            )
        )*
        postfixExpr

    ;

postfixExpr
         { boolean cmplx = false; } :
            primaryExpr

            (   
                TOK_LBRACK!
                    index TOK_RBRACK!
                { cmplx = true; }
    
            |   ( TOK_DOT! | TOK_INDIRECT! ) 
                  (  methodInvocation[false]
                   | identifier )
                { cmplx = true; }
            )*
            { if (cmplx) {
                #postfixExpr = #([POSTFIX, "postfix",
                   "org.apache.geode.cache.query.internal.parse.ASTPostfix"],
                    #postfixExpr);
              }
            }

    ;

methodInvocation! [boolean implicitReceiver] :
    
            methodName:identifier args:argList  
            { #methodInvocation =
                #([METHOD_INV, "methodInv",
                    "org.apache.geode.cache.query.internal.parse.ASTMethodInvocation"],
                  #methodName, #args); 
              ((ASTMethodInvocation)#methodInvocation).setImplicitReceiver(implicitReceiver); }
        ;

index :

        // single element
        (
          expr
          (
            // multiple elements: OQL defines a comma-delimited list of
            // indexes in its grammar, but seemingly fails to define the
            // semantics.
            // This grammar is left in just in case someone figures out what
            // this means, but for now this will throw an UnsupportedOperationException
            // at query compilation time
            // (throw new UnsupportedOperationException("Only one index expression supported"))
            (
                TOK_COMMA!
                expr 
            )*

            // Range of elements
            |   TOK_COLON^ expr
          ) 
          | TOK_STAR
        )
        { #index = #([TOK_LBRACK, "index",
            "org.apache.geode.cache.query.internal.parse.ASTCombination"],
                #index);
        }
    ;

argList :

        t:TOK_LPAREN^<AST=org.apache.geode.cache.query.internal.parse.ASTCombination>
        (
            expr
            (
                TOK_COMMA!
                expr
            )*
        )?
        TOK_RPAREN!
    ;

primaryExpr :

        (
            conversionExpr
        |   collectionExpr
        |   aggregateExpr
        |   undefinedExpr
        |   ( identifier TOK_LPAREN identifier TOK_COLON ) =>
                objectConstruction
        |   structConstruction
        |   collectionConstruction
        |   methodInvocation[true]
        |   identifier
        |   queryParam
        |   literal
        |   TOK_LPAREN! query TOK_RPAREN!
        |   RegionPath<AST=org.apache.geode.cache.query.internal.parse.ASTRegionPath>
        )
    ;


conversionExpr :
	(

	    (
                  "listtoset"^
              |   "element"^<AST=org.apache.geode.cache.query.internal.parse.ASTConversionExpr>
            
              |   "distinct"^
              |   "flatten"^
            )
            TOK_LPAREN! query TOK_RPAREN!
        |
           (  
              (
               "nvl"^<AST=org.apache.geode.cache.query.internal.parse.ASTConversionExpr>
              )
              TOK_LPAREN!
              expr TOK_COMMA! expr
              TOK_RPAREN! 
           ) 
        |
           (  
              (
               "to_date"^<AST=org.apache.geode.cache.query.internal.parse.ASTConversionExpr>
              )
              TOK_LPAREN!
              stringLiteral TOK_COMMA! stringLiteral
              TOK_RPAREN! 
           )    
	)
    ;

collectionExpr :

        (
            "first"^
        |   "last"^
        |   "unique"^
        |   "exists"^
        )
        TOK_LPAREN! query TOK_RPAREN!

    ;



            
aggregateExpr  { int aggFunc = -1; boolean distinctOnly = false; }:

             !("sum" {aggFunc = SUM;} | "avg" {aggFunc = AVG;} )
              TOK_LPAREN ("distinct"! {distinctOnly = true;} ) ? tokExpr1:expr TOK_RPAREN 
              { #aggregateExpr = #([AGG_FUNC, "aggregate", "org.apache.geode.cache.query.internal.parse.ASTAggregateFunc"],
              #tokExpr1); 
                ((ASTAggregateFunc)#aggregateExpr).setAggregateFunctionType(aggFunc);
                ((ASTAggregateFunc)#aggregateExpr).setDistinctOnly(distinctOnly);
               }
             
             |
             !("min" {aggFunc = MIN;} | "max" {aggFunc = MAX;} )
              TOK_LPAREN  tokExpr2:expr TOK_RPAREN 
              { #aggregateExpr = #([AGG_FUNC, "aggregate", "org.apache.geode.cache.query.internal.parse.ASTAggregateFunc"],
              #tokExpr2); 
                ((ASTAggregateFunc)#aggregateExpr).setAggregateFunctionType(aggFunc);               
               }
             
             |
              "count"^<AST=org.apache.geode.cache.query.internal.parse.ASTAggregateFunc>
              TOK_LPAREN!  ( TOK_STAR <AST=org.apache.geode.cache.query.internal.parse.ASTDummy>
              | ("distinct"! {distinctOnly = true;} ) ? expr ) TOK_RPAREN! 
              {  
                 ((ASTAggregateFunc)#aggregateExpr).setAggregateFunctionType(COUNT);
                 #aggregateExpr.setText("aggregate");
                 ((ASTAggregateFunc)#aggregateExpr).setDistinctOnly(distinctOnly);
              }
    ;

undefinedExpr :

        (
            "is_undefined"^<AST=org.apache.geode.cache.query.internal.parse.ASTUndefinedExpr>
        |   "is_defined"^<AST=org.apache.geode.cache.query.internal.parse.ASTUndefinedExpr>
        )
        TOK_LPAREN!
        query
        TOK_RPAREN!
    ;

objectConstruction :

        identifier
        t:TOK_LPAREN^
            {#t.setType(OBJ_CONSTRUCTOR);}
        fieldList
        TOK_RPAREN!
    ;

structConstruction :

        "struct"^
        TOK_LPAREN!
        fieldList
        TOK_RPAREN!
    ;


groupByList :

        expr
        (
            TOK_COMMA!
            expr
        )*
        /*{ #groupByList = #([COMBO, "groupByList",
            "org.apache.geode.cache.query.internal.parse.ASTCombination"],
                #groupByList); }*/
  ;

    
fieldList :

        identifier TOK_COLON! expr
        (
            TOK_COMMA!
            identifier
            TOK_COLON! expr
        )*
        { #fieldList = #([COMBO, "fieldList",
            "org.apache.geode.cache.query.internal.parse.ASTCombination"],
                #fieldList); }
    ;

collectionConstruction :
        (
            (
                "array"^
            |   "set"^<AST=org.apache.geode.cache.query.internal.parse.ASTConstruction>
            |   "bag"^
            )
            argList

        |   "list"^
            TOK_LPAREN
            (
                expr
                (
                    TOK_DOTDOT expr
                |   (
                        TOK_COMMA expr
                    )*
                )
            )?
            TOK_RPAREN
        )
    ;

queryParam! :

        TOK_DOLLAR NUM_INT
        { #queryParam = #([QUERY_PARAM, "queryParam",
               "org.apache.geode.cache.query.internal.parse.ASTParameter"],
                   #NUM_INT); }
        
    ;

type :
    
      (
//            ( "unsigned" )? // ignored
//            (
                typ00:"short"<AST=org.apache.geode.cache.query.internal.parse.ASTType>
                    { #typ00.setJavaType(TypeUtils.getObjectType(Short.class)); }
            |   typ01:"long"<AST=org.apache.geode.cache.query.internal.parse.ASTType>
                    { #typ01.setJavaType(TypeUtils.getObjectType(Long.class)); }
//            )
        |   typ02:"int"<AST=org.apache.geode.cache.query.internal.parse.ASTType>
                    { #typ02.setJavaType(TypeUtils.getObjectType(Integer.class)); }
        |   typ03:"float"<AST=org.apache.geode.cache.query.internal.parse.ASTType>
                    { #typ03.setJavaType(TypeUtils.getObjectType(Float.class)); }
        |   typ04:"double"<AST=org.apache.geode.cache.query.internal.parse.ASTType>
                    { #typ04.setJavaType(TypeUtils.getObjectType(Double.class)); }
        |   typ05:"char"<AST=org.apache.geode.cache.query.internal.parse.ASTType>
                    { #typ05.setJavaType(TypeUtils.getObjectType(Character.class)); }
        |   typ06:"string"<AST=org.apache.geode.cache.query.internal.parse.ASTType>
                    { #typ06.setJavaType(TypeUtils.getObjectType(String.class)); }
        |   typ07:"boolean"<AST=org.apache.geode.cache.query.internal.parse.ASTType>
                    { #typ07.setJavaType(TypeUtils.getObjectType(Boolean.class)); }
        |   typ08:"byte"<AST=org.apache.geode.cache.query.internal.parse.ASTType>
                    { #typ08.setJavaType(TypeUtils.getObjectType(Byte.class)); } 
        |   typ09:"octet"<AST=org.apache.geode.cache.query.internal.parse.ASTType>
                    { #typ09.setJavaType(TypeUtils.getObjectType(Byte.class)); }
        |   "enum"^ // unsupported
            (
                identifier
                TOK_DOT
            )?
            identifier
        |   typ10:"date"<AST=org.apache.geode.cache.query.internal.parse.ASTType>
                    { #typ10.setJavaType(TypeUtils.getObjectType(java.sql.Date.class)); }
        |   typ11:"time"<AST=org.apache.geode.cache.query.internal.parse.ASTType>
                    { #typ11.setJavaType(TypeUtils.getObjectType(java.sql.Time.class)); }
        |   "interval" // unsupported
        |   typ12:"timestamp"<AST=org.apache.geode.cache.query.internal.parse.ASTType>
                    { #typ12.setJavaType(TypeUtils.getObjectType(java.sql.Timestamp.class)); }
        |   typ13:"set"^<AST=org.apache.geode.cache.query.internal.parse.ASTType>
                TOK_LT! type TOK_GT!
                    { #typ13.setJavaType(new CollectionTypeImpl(Set.class, TypeUtils.OBJECT_TYPE /*resolved later*/)); } 
        |   typ14:"collection"^<AST=org.apache.geode.cache.query.internal.parse.ASTType>
                TOK_LT! type TOK_GT!
                    { #typ14.setJavaType(new CollectionTypeImpl(Collection.class, TypeUtils.OBJECT_TYPE /*resolved later*/)); } 
        |   "bag"^ TOK_LT! type TOK_GT! // not supported
        |   typ15:"list"^<AST=org.apache.geode.cache.query.internal.parse.ASTType> TOK_LT! type TOK_GT!
                    { #typ15.setJavaType(new CollectionTypeImpl(List.class, TypeUtils.OBJECT_TYPE /*resolved later*/)); }
        |   typ16:"array"^<AST=org.apache.geode.cache.query.internal.parse.ASTType> TOK_LT! type TOK_GT!
                    { #typ16.setJavaType(new CollectionTypeImpl(Object[].class, TypeUtils.OBJECT_TYPE /*resolved later*/)); }
        |   (typ17:"dictionary"^<AST=org.apache.geode.cache.query.internal.parse.ASTType>
                    { #typ17.setJavaType(new MapTypeImpl(Map.class, TypeUtils.OBJECT_TYPE , TypeUtils.OBJECT_TYPE /*resolved later*/)); }
                | typ18:"map"^<AST=org.apache.geode.cache.query.internal.parse.ASTType>
                    { #typ18.setJavaType(new MapTypeImpl(Map.class, TypeUtils.OBJECT_TYPE , TypeUtils.OBJECT_TYPE /*resolved later*/)); }
            )
            TOK_LT! type TOK_COMMA! type TOK_GT!
        |!   id:identifier { String txt = #id.getText();
                #type = #[Identifier, txt, "org.apache.geode.cache.query.internal.parse.ASTType"];
                 ((ASTType)#type).setTypeName(txt); }
      )
      

    ;

literal :

            objectLiteral
        |   booleanLiteral
        |   numericLiteral
        |   charLiteral
        |   stringLiteral
        |   dateLiteral
        |   timeLiteral
        |   timestampLiteral
    ;

objectLiteral :

        "nil"<AST=org.apache.geode.cache.query.internal.parse.ASTLiteral>
      | "null"<AST=org.apache.geode.cache.query.internal.parse.ASTLiteral>
      | "undefined"<AST=org.apache.geode.cache.query.internal.parse.ASTLiteral>
    ;

booleanLiteral :

        (
            "true"<AST=org.apache.geode.cache.query.internal.parse.ASTLiteral>
        |   "false"<AST=org.apache.geode.cache.query.internal.parse.ASTLiteral>
        )
    ;

numericLiteral :

        (   NUM_INT<AST=org.apache.geode.cache.query.internal.parse.ASTLiteral>
          | NUM_LONG<AST=org.apache.geode.cache.query.internal.parse.ASTLiteral>
          | NUM_FLOAT<AST=org.apache.geode.cache.query.internal.parse.ASTLiteral>
          | NUM_DOUBLE<AST=org.apache.geode.cache.query.internal.parse.ASTLiteral>
        )
        
    ;


charLiteral :

        "char"^<AST=org.apache.geode.cache.query.internal.parse.ASTLiteral>
            StringLiteral
    ;


stringLiteral :

        StringLiteral<AST=org.apache.geode.cache.query.internal.parse.ASTLiteral>
    ;

dateLiteral :

        "date"^<AST=org.apache.geode.cache.query.internal.parse.ASTLiteral>
            StringLiteral
    ;

timeLiteral :

        "time"^<AST=org.apache.geode.cache.query.internal.parse.ASTLiteral>
            StringLiteral
    ;

timestampLiteral :

        "timestamp"^<AST=org.apache.geode.cache.query.internal.parse.ASTLiteral>
           StringLiteral
    ;

identifier :
        Identifier<AST=org.apache.geode.cache.query.internal.parse.ASTIdentifier>
    |   q:QuotedIdentifier<AST=org.apache.geode.cache.query.internal.parse.ASTIdentifier> 
            { #q.setType(Identifier); }
    ;

