// $ANTLR 2.7.4: "oql.g" -> "OQLLexer.java"$

package com.gemstone.gemfire.cache.query.internal.parse;
import java.util.*;
import com.gemstone.gemfire.cache.query.internal.types.*;

import java.io.InputStream;
import antlr.TokenStreamException;
import antlr.TokenStreamIOException;
import antlr.TokenStreamRecognitionException;
import antlr.CharStreamException;
import antlr.CharStreamIOException;
import antlr.ANTLRException;
import java.io.Reader;
import java.util.Hashtable;
import antlr.CharScanner;
import antlr.InputBuffer;
import antlr.ByteBuffer;
import antlr.CharBuffer;
import antlr.Token;
import antlr.CommonToken;
import antlr.RecognitionException;
import antlr.NoViableAltForCharException;
import antlr.MismatchedCharException;
import antlr.TokenStream;
import antlr.ANTLRHashString;
import antlr.LexerSharedInputState;
import antlr.collections.impl.BitSet;
import antlr.SemanticException;

public class OQLLexer extends antlr.CharScanner implements OQLLexerTokenTypes, TokenStream
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
public OQLLexer(InputStream in) {
	this(new ByteBuffer(in));
}
public OQLLexer(Reader in) {
	this(new CharBuffer(in));
}
public OQLLexer(InputBuffer ib) {
	this(new LexerSharedInputState(ib));
}
public OQLLexer(LexerSharedInputState state) {
	super(state);
	caseSensitiveLiterals = false;
	setCaseSensitive(false);
	literals = new Hashtable();
	literals.put(new ANTLRHashString("type", this), new Integer(78));
	literals.put(new ANTLRHashString("byte", this), new Integer(131));
	literals.put(new ANTLRHashString("list", this), new Integer(122));
	literals.put(new ANTLRHashString("undefine", this), new Integer(72));
	literals.put(new ANTLRHashString("time", this), new Integer(135));
	literals.put(new ANTLRHashString("short", this), new Integer(123));
	literals.put(new ANTLRHashString("dictionary", this), new Integer(139));
	literals.put(new ANTLRHashString("listtoset", this), new Integer(103));
	literals.put(new ANTLRHashString("abs", this), new Integer(101));
	literals.put(new ANTLRHashString("timestamp", this), new Integer(137));
	literals.put(new ANTLRHashString("limit", this), new Integer(80));
	literals.put(new ANTLRHashString("distinct", this), new Integer(74));
	literals.put(new ANTLRHashString("octet", this), new Integer(132));
	literals.put(new ANTLRHashString("where", this), new Integer(79));
	literals.put(new ANTLRHashString("orelse", this), new Integer(89));
	literals.put(new ANTLRHashString("select", this), new Integer(73));
	literals.put(new ANTLRHashString("and", this), new Integer(90));
	literals.put(new ANTLRHashString("float", this), new Integer(126));
	literals.put(new ANTLRHashString("not", this), new Integer(102));
	literals.put(new ANTLRHashString("interval", this), new Integer(136));
	literals.put(new ANTLRHashString("date", this), new Integer(134));
	literals.put(new ANTLRHashString("from", this), new Integer(76));
	literals.put(new ANTLRHashString("null", this), new Integer(142));
	literals.put(new ANTLRHashString("flatten", this), new Integer(105));
	literals.put(new ANTLRHashString("count", this), new Integer(115));
	literals.put(new ANTLRHashString("last", this), new Integer(109));
	literals.put(new ANTLRHashString("query", this), new Integer(71));
	literals.put(new ANTLRHashString("mod", this), new Integer(99));
	literals.put(new ANTLRHashString("trace", this), new Integer(66));
	literals.put(new ANTLRHashString("nvl", this), new Integer(106));
	literals.put(new ANTLRHashString("like", this), new Integer(96));
	literals.put(new ANTLRHashString("except", this), new Integer(98));
	literals.put(new ANTLRHashString("set", this), new Integer(120));
	literals.put(new ANTLRHashString("to_date", this), new Integer(107));
	literals.put(new ANTLRHashString("intersect", this), new Integer(100));
	literals.put(new ANTLRHashString("map", this), new Integer(140));
	literals.put(new ANTLRHashString("array", this), new Integer(119));
	literals.put(new ANTLRHashString("or", this), new Integer(88));
	literals.put(new ANTLRHashString("any", this), new Integer(94));
	literals.put(new ANTLRHashString("double", this), new Integer(127));
	literals.put(new ANTLRHashString("min", this), new Integer(113));
	literals.put(new ANTLRHashString("as", this), new Integer(68));
	literals.put(new ANTLRHashString("first", this), new Integer(108));
	literals.put(new ANTLRHashString("by", this), new Integer(82));
	literals.put(new ANTLRHashString("all", this), new Integer(75));
	literals.put(new ANTLRHashString("union", this), new Integer(97));
	literals.put(new ANTLRHashString("order", this), new Integer(85));
	literals.put(new ANTLRHashString("is_defined", this), new Integer(117));
	literals.put(new ANTLRHashString("collection", this), new Integer(138));
	literals.put(new ANTLRHashString("some", this), new Integer(95));
	literals.put(new ANTLRHashString("enum", this), new Integer(133));
	literals.put(new ANTLRHashString("declare", this), new Integer(69));
	literals.put(new ANTLRHashString("int", this), new Integer(125));
	literals.put(new ANTLRHashString("for", this), new Integer(91));
	literals.put(new ANTLRHashString("is_undefined", this), new Integer(116));
	literals.put(new ANTLRHashString("boolean", this), new Integer(130));
	literals.put(new ANTLRHashString("char", this), new Integer(128));
	literals.put(new ANTLRHashString("define", this), new Integer(70));
	literals.put(new ANTLRHashString("element", this), new Integer(104));
	literals.put(new ANTLRHashString("string", this), new Integer(129));
	literals.put(new ANTLRHashString("hint", this), new Integer(84));
	literals.put(new ANTLRHashString("false", this), new Integer(145));
	literals.put(new ANTLRHashString("exists", this), new Integer(92));
	literals.put(new ANTLRHashString("asc", this), new Integer(86));
	literals.put(new ANTLRHashString("undefined", this), new Integer(143));
	literals.put(new ANTLRHashString("desc", this), new Integer(87));
	literals.put(new ANTLRHashString("bag", this), new Integer(121));
	literals.put(new ANTLRHashString("max", this), new Integer(114));
	literals.put(new ANTLRHashString("sum", this), new Integer(111));
	literals.put(new ANTLRHashString("struct", this), new Integer(118));
	literals.put(new ANTLRHashString("import", this), new Integer(67));
	literals.put(new ANTLRHashString("in", this), new Integer(77));
	literals.put(new ANTLRHashString("avg", this), new Integer(112));
	literals.put(new ANTLRHashString("true", this), new Integer(144));
	literals.put(new ANTLRHashString("long", this), new Integer(124));
	literals.put(new ANTLRHashString("nil", this), new Integer(141));
	literals.put(new ANTLRHashString("group", this), new Integer(81));
	literals.put(new ANTLRHashString("having", this), new Integer(83));
	literals.put(new ANTLRHashString("unique", this), new Integer(110));
	literals.put(new ANTLRHashString("andthen", this), new Integer(93));
}

public Token nextToken() throws TokenStreamException {
	Token theRetToken=null;
tryAgain:
	for (;;) {
		Token _token = null;
		int _ttype = Token.INVALID_TYPE;
		resetText();
		try {   // for char stream error handling
			try {   // for lexical error handling
				switch ( LA(1)) {
				case ')':
				{
					mTOK_RPAREN(true);
					theRetToken=_returnToken;
					break;
				}
				case '(':
				{
					mTOK_LPAREN(true);
					theRetToken=_returnToken;
					break;
				}
				case ',':
				{
					mTOK_COMMA(true);
					theRetToken=_returnToken;
					break;
				}
				case ';':
				{
					mTOK_SEMIC(true);
					theRetToken=_returnToken;
					break;
				}
				case ':':
				{
					mTOK_COLON(true);
					theRetToken=_returnToken;
					break;
				}
				case '|':
				{
					mTOK_CONCAT(true);
					theRetToken=_returnToken;
					break;
				}
				case '=':
				{
					mTOK_EQ(true);
					theRetToken=_returnToken;
					break;
				}
				case '+':
				{
					mTOK_PLUS(true);
					theRetToken=_returnToken;
					break;
				}
				case '*':
				{
					mTOK_STAR(true);
					theRetToken=_returnToken;
					break;
				}
				case '!':
				{
					mTOK_NE_ALT(true);
					theRetToken=_returnToken;
					break;
				}
				case '[':
				{
					mTOK_LBRACK(true);
					theRetToken=_returnToken;
					break;
				}
				case ']':
				{
					mTOK_RBRACK(true);
					theRetToken=_returnToken;
					break;
				}
				case '$':
				{
					mTOK_DOLLAR(true);
					theRetToken=_returnToken;
					break;
				}
				case '"':
				{
					mQuotedIdentifier(true);
					theRetToken=_returnToken;
					break;
				}
				case '\'':
				{
					mStringLiteral(true);
					theRetToken=_returnToken;
					break;
				}
				case '0':  case '1':  case '2':  case '3':
				case '4':  case '5':  case '6':  case '7':
				case '8':  case '9':
				{
					mNUM_INT(true);
					theRetToken=_returnToken;
					break;
				}
				case '\t':  case '\n':  case '\u000c':  case '\r':
				case ' ':
				{
					mWS(true);
					theRetToken=_returnToken;
					break;
				}
				default:
					if ((LA(1)=='.') && (LA(2)=='.')) {
						mTOK_DOTDOT(true);
						theRetToken=_returnToken;
					}
					else if ((LA(1)=='-') && (LA(2)=='>')) {
						mTOK_INDIRECT(true);
						theRetToken=_returnToken;
					}
					else if ((LA(1)=='<') && (LA(2)=='=')) {
						mTOK_LE(true);
						theRetToken=_returnToken;
					}
					else if ((LA(1)=='>') && (LA(2)=='=')) {
						mTOK_GE(true);
						theRetToken=_returnToken;
					}
					else if ((LA(1)=='<') && (LA(2)=='>')) {
						mTOK_NE(true);
						theRetToken=_returnToken;
					}
					else if ((LA(1)=='/') && (_tokenSet_0.member(LA(2)))) {
						mRegionPath(true);
						theRetToken=_returnToken;
					}
					else if ((LA(1)=='-') && (LA(2)=='-')) {
						mSL_COMMENT(true);
						theRetToken=_returnToken;
					}
					else if ((LA(1)=='/') && (LA(2)=='*')) {
						mML_COMMENT(true);
						theRetToken=_returnToken;
					}
					else if ((LA(1)=='.') && (true)) {
						mTOK_DOT(true);
						theRetToken=_returnToken;
					}
					else if ((LA(1)=='-') && (true)) {
						mTOK_MINUS(true);
						theRetToken=_returnToken;
					}
					else if ((LA(1)=='/') && (true)) {
						mTOK_SLASH(true);
						theRetToken=_returnToken;
					}
					else if ((LA(1)=='<') && (true)) {
						mTOK_LT(true);
						theRetToken=_returnToken;
					}
					else if ((LA(1)=='>') && (true)) {
						mTOK_GT(true);
						theRetToken=_returnToken;
					}
					else if ((_tokenSet_1.member(LA(1)))) {
						mIdentifier(true);
						theRetToken=_returnToken;
					}
				else {
					if (LA(1)==EOF_CHAR) {uponEOF(); _returnToken = makeToken(Token.EOF_TYPE);}
				else {throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());}
				}
				}
				if ( _returnToken==null ) continue tryAgain; // found SKIP token
				_ttype = _returnToken.getType();
				_returnToken.setType(_ttype);
				return _returnToken;
			}
			catch (RecognitionException e) {
				throw new TokenStreamRecognitionException(e);
			}
		}
		catch (CharStreamException cse) {
			if ( cse instanceof CharStreamIOException ) {
				throw new TokenStreamIOException(((CharStreamIOException)cse).io);
			}
			else {
				throw new TokenStreamException(cse.getMessage());
			}
		}
	}
}

	public final void mTOK_RPAREN(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = TOK_RPAREN;
		int _saveIndex;
		
		match(')');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mTOK_LPAREN(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = TOK_LPAREN;
		int _saveIndex;
		
		match('(');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mTOK_COMMA(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = TOK_COMMA;
		int _saveIndex;
		
		match(',');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mTOK_SEMIC(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = TOK_SEMIC;
		int _saveIndex;
		
		match(';');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mTOK_DOTDOT(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = TOK_DOTDOT;
		int _saveIndex;
		
		match("..");
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mTOK_COLON(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = TOK_COLON;
		int _saveIndex;
		
		match(':');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mTOK_DOT(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = TOK_DOT;
		int _saveIndex;
		
		match('.');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mTOK_INDIRECT(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = TOK_INDIRECT;
		int _saveIndex;
		
		match('-');
		match('>');
		if ( inputState.guessing==0 ) {
			_ttype = TOK_DOT;
		}
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mTOK_CONCAT(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = TOK_CONCAT;
		int _saveIndex;
		
		match('|');
		match('|');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mTOK_EQ(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = TOK_EQ;
		int _saveIndex;
		
		match('=');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mTOK_PLUS(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = TOK_PLUS;
		int _saveIndex;
		
		match('+');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mTOK_MINUS(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = TOK_MINUS;
		int _saveIndex;
		
		match('-');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mTOK_SLASH(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = TOK_SLASH;
		int _saveIndex;
		
		match('/');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mTOK_STAR(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = TOK_STAR;
		int _saveIndex;
		
		match('*');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mTOK_LE(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = TOK_LE;
		int _saveIndex;
		
		match('<');
		match('=');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mTOK_GE(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = TOK_GE;
		int _saveIndex;
		
		match('>');
		match('=');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mTOK_NE(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = TOK_NE;
		int _saveIndex;
		
		match('<');
		match('>');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mTOK_NE_ALT(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = TOK_NE_ALT;
		int _saveIndex;
		
		match('!');
		match('=');
		if ( inputState.guessing==0 ) {
			_ttype = TOK_NE;
		}
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mTOK_LT(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = TOK_LT;
		int _saveIndex;
		
		match('<');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mTOK_GT(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = TOK_GT;
		int _saveIndex;
		
		match('>');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mTOK_LBRACK(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = TOK_LBRACK;
		int _saveIndex;
		
		match('[');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mTOK_RBRACK(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = TOK_RBRACK;
		int _saveIndex;
		
		match(']');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mTOK_DOLLAR(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = TOK_DOLLAR;
		int _saveIndex;
		
		match('$');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	protected final void mLETTER(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = LETTER;
		int _saveIndex;
		
		{
		switch ( LA(1)) {
		case 'a':  case 'b':  case 'c':  case 'd':
		case 'e':  case 'f':  case 'g':  case 'h':
		case 'i':  case 'j':  case 'k':  case 'l':
		case 'm':  case 'n':  case 'o':  case 'p':
		case 'q':  case 'r':  case 's':  case 't':
		case 'u':  case 'v':  case 'w':  case 'x':
		case 'y':  case 'z':
		{
			matchRange('\u0061','\u007a');
			break;
		}
		case '\u00c0':  case '\u00c1':  case '\u00c2':  case '\u00c3':
		case '\u00c4':  case '\u00c5':  case '\u00c6':  case '\u00c7':
		case '\u00c8':  case '\u00c9':  case '\u00ca':  case '\u00cb':
		case '\u00cc':  case '\u00cd':  case '\u00ce':  case '\u00cf':
		case '\u00d0':  case '\u00d1':  case '\u00d2':  case '\u00d3':
		case '\u00d4':  case '\u00d5':  case '\u00d6':
		{
			matchRange('\u00c0','\u00d6');
			break;
		}
		case '\u00a1':  case '\u00a2':  case '\u00a3':  case '\u00a4':
		case '\u00a5':  case '\u00a6':  case '\u00a7':  case '\u00a8':
		case '\u00a9':  case '\u00aa':  case '\u00ab':  case '\u00ac':
		case '\u00ad':  case '\u00ae':  case '\u00af':  case '\u00b0':
		case '\u00b1':  case '\u00b2':  case '\u00b3':  case '\u00b4':
		case '\u00b5':  case '\u00b6':  case '\u00b7':  case '\u00b8':
		case '\u00b9':  case '\u00ba':  case '\u00bb':  case '\u00bc':
		case '\u00bd':  case '\u00be':  case '\u00bf':
		{
			matchRange('\u00a1','\u00bf');
			break;
		}
		case '\u00d8':  case '\u00d9':  case '\u00da':  case '\u00db':
		case '\u00dc':  case '\u00dd':  case '\u00de':  case '\u00df':
		case '\u00e0':  case '\u00e1':  case '\u00e2':  case '\u00e3':
		case '\u00e4':  case '\u00e5':  case '\u00e6':  case '\u00e7':
		case '\u00e8':  case '\u00e9':  case '\u00ea':  case '\u00eb':
		case '\u00ec':  case '\u00ed':  case '\u00ee':  case '\u00ef':
		case '\u00f0':  case '\u00f1':  case '\u00f2':  case '\u00f3':
		case '\u00f4':  case '\u00f5':  case '\u00f6':
		{
			matchRange('\u00d8','\u00f6');
			break;
		}
		case '\u00f8':  case '\u00f9':  case '\u00fa':  case '\u00fb':
		case '\u00fc':  case '\u00fd':  case '\u00fe':  case '\u00ff':
		{
			matchRange('\u00f8','\u00ff');
			break;
		}
		case '\u0970':  case '\u0971':  case '\u0972':  case '\u0973':
		case '\u0974':  case '\u0975':  case '\u0976':  case '\u0977':
		case '\u0978':  case '\u0979':  case '\u097a':  case '\u097b':
		case '\u097c':  case '\u097d':  case '\u097e':  case '\u097f':
		case '\u0980':  case '\u0981':  case '\u0982':  case '\u0983':
		case '\u0984':  case '\u0985':  case '\u0986':  case '\u0987':
		case '\u0988':  case '\u0989':  case '\u098a':  case '\u098b':
		case '\u098c':  case '\u098d':  case '\u098e':  case '\u098f':
		case '\u0990':  case '\u0991':  case '\u0992':  case '\u0993':
		case '\u0994':  case '\u0995':  case '\u0996':  case '\u0997':
		case '\u0998':  case '\u0999':  case '\u099a':  case '\u099b':
		case '\u099c':  case '\u099d':  case '\u099e':  case '\u099f':
		case '\u09a0':  case '\u09a1':  case '\u09a2':  case '\u09a3':
		case '\u09a4':  case '\u09a5':  case '\u09a6':  case '\u09a7':
		case '\u09a8':  case '\u09a9':  case '\u09aa':  case '\u09ab':
		case '\u09ac':  case '\u09ad':  case '\u09ae':  case '\u09af':
		case '\u09b0':  case '\u09b1':  case '\u09b2':  case '\u09b3':
		case '\u09b4':  case '\u09b5':  case '\u09b6':  case '\u09b7':
		case '\u09b8':  case '\u09b9':  case '\u09ba':  case '\u09bb':
		case '\u09bc':  case '\u09bd':  case '\u09be':  case '\u09bf':
		case '\u09c0':  case '\u09c1':  case '\u09c2':  case '\u09c3':
		case '\u09c4':  case '\u09c5':  case '\u09c6':  case '\u09c7':
		case '\u09c8':  case '\u09c9':  case '\u09ca':  case '\u09cb':
		case '\u09cc':  case '\u09cd':  case '\u09ce':  case '\u09cf':
		case '\u09d0':  case '\u09d1':  case '\u09d2':  case '\u09d3':
		case '\u09d4':  case '\u09d5':  case '\u09d6':  case '\u09d7':
		case '\u09d8':  case '\u09d9':  case '\u09da':  case '\u09db':
		case '\u09dc':  case '\u09dd':  case '\u09de':  case '\u09df':
		case '\u09e0':  case '\u09e1':  case '\u09e2':  case '\u09e3':
		case '\u09e4':  case '\u09e5':
		{
			matchRange('\u0970','\u09e5');
			break;
		}
		case '\u09f0':  case '\u09f1':  case '\u09f2':  case '\u09f3':
		case '\u09f4':  case '\u09f5':  case '\u09f6':  case '\u09f7':
		case '\u09f8':  case '\u09f9':  case '\u09fa':  case '\u09fb':
		case '\u09fc':  case '\u09fd':  case '\u09fe':  case '\u09ff':
		case '\u0a00':  case '\u0a01':  case '\u0a02':  case '\u0a03':
		case '\u0a04':  case '\u0a05':  case '\u0a06':  case '\u0a07':
		case '\u0a08':  case '\u0a09':  case '\u0a0a':  case '\u0a0b':
		case '\u0a0c':  case '\u0a0d':  case '\u0a0e':  case '\u0a0f':
		case '\u0a10':  case '\u0a11':  case '\u0a12':  case '\u0a13':
		case '\u0a14':  case '\u0a15':  case '\u0a16':  case '\u0a17':
		case '\u0a18':  case '\u0a19':  case '\u0a1a':  case '\u0a1b':
		case '\u0a1c':  case '\u0a1d':  case '\u0a1e':  case '\u0a1f':
		case '\u0a20':  case '\u0a21':  case '\u0a22':  case '\u0a23':
		case '\u0a24':  case '\u0a25':  case '\u0a26':  case '\u0a27':
		case '\u0a28':  case '\u0a29':  case '\u0a2a':  case '\u0a2b':
		case '\u0a2c':  case '\u0a2d':  case '\u0a2e':  case '\u0a2f':
		case '\u0a30':  case '\u0a31':  case '\u0a32':  case '\u0a33':
		case '\u0a34':  case '\u0a35':  case '\u0a36':  case '\u0a37':
		case '\u0a38':  case '\u0a39':  case '\u0a3a':  case '\u0a3b':
		case '\u0a3c':  case '\u0a3d':  case '\u0a3e':  case '\u0a3f':
		case '\u0a40':  case '\u0a41':  case '\u0a42':  case '\u0a43':
		case '\u0a44':  case '\u0a45':  case '\u0a46':  case '\u0a47':
		case '\u0a48':  case '\u0a49':  case '\u0a4a':  case '\u0a4b':
		case '\u0a4c':  case '\u0a4d':  case '\u0a4e':  case '\u0a4f':
		case '\u0a50':  case '\u0a51':  case '\u0a52':  case '\u0a53':
		case '\u0a54':  case '\u0a55':  case '\u0a56':  case '\u0a57':
		case '\u0a58':  case '\u0a59':  case '\u0a5a':  case '\u0a5b':
		case '\u0a5c':  case '\u0a5d':  case '\u0a5e':  case '\u0a5f':
		case '\u0a60':  case '\u0a61':  case '\u0a62':  case '\u0a63':
		case '\u0a64':  case '\u0a65':
		{
			matchRange('\u09f0','\u0a65');
			break;
		}
		case '\u0a70':  case '\u0a71':  case '\u0a72':  case '\u0a73':
		case '\u0a74':  case '\u0a75':  case '\u0a76':  case '\u0a77':
		case '\u0a78':  case '\u0a79':  case '\u0a7a':  case '\u0a7b':
		case '\u0a7c':  case '\u0a7d':  case '\u0a7e':  case '\u0a7f':
		case '\u0a80':  case '\u0a81':  case '\u0a82':  case '\u0a83':
		case '\u0a84':  case '\u0a85':  case '\u0a86':  case '\u0a87':
		case '\u0a88':  case '\u0a89':  case '\u0a8a':  case '\u0a8b':
		case '\u0a8c':  case '\u0a8d':  case '\u0a8e':  case '\u0a8f':
		case '\u0a90':  case '\u0a91':  case '\u0a92':  case '\u0a93':
		case '\u0a94':  case '\u0a95':  case '\u0a96':  case '\u0a97':
		case '\u0a98':  case '\u0a99':  case '\u0a9a':  case '\u0a9b':
		case '\u0a9c':  case '\u0a9d':  case '\u0a9e':  case '\u0a9f':
		case '\u0aa0':  case '\u0aa1':  case '\u0aa2':  case '\u0aa3':
		case '\u0aa4':  case '\u0aa5':  case '\u0aa6':  case '\u0aa7':
		case '\u0aa8':  case '\u0aa9':  case '\u0aaa':  case '\u0aab':
		case '\u0aac':  case '\u0aad':  case '\u0aae':  case '\u0aaf':
		case '\u0ab0':  case '\u0ab1':  case '\u0ab2':  case '\u0ab3':
		case '\u0ab4':  case '\u0ab5':  case '\u0ab6':  case '\u0ab7':
		case '\u0ab8':  case '\u0ab9':  case '\u0aba':  case '\u0abb':
		case '\u0abc':  case '\u0abd':  case '\u0abe':  case '\u0abf':
		case '\u0ac0':  case '\u0ac1':  case '\u0ac2':  case '\u0ac3':
		case '\u0ac4':  case '\u0ac5':  case '\u0ac6':  case '\u0ac7':
		case '\u0ac8':  case '\u0ac9':  case '\u0aca':  case '\u0acb':
		case '\u0acc':  case '\u0acd':  case '\u0ace':  case '\u0acf':
		case '\u0ad0':  case '\u0ad1':  case '\u0ad2':  case '\u0ad3':
		case '\u0ad4':  case '\u0ad5':  case '\u0ad6':  case '\u0ad7':
		case '\u0ad8':  case '\u0ad9':  case '\u0ada':  case '\u0adb':
		case '\u0adc':  case '\u0add':  case '\u0ade':  case '\u0adf':
		case '\u0ae0':  case '\u0ae1':  case '\u0ae2':  case '\u0ae3':
		case '\u0ae4':  case '\u0ae5':
		{
			matchRange('\u0a70','\u0ae5');
			break;
		}
		case '\u0af0':  case '\u0af1':  case '\u0af2':  case '\u0af3':
		case '\u0af4':  case '\u0af5':  case '\u0af6':  case '\u0af7':
		case '\u0af8':  case '\u0af9':  case '\u0afa':  case '\u0afb':
		case '\u0afc':  case '\u0afd':  case '\u0afe':  case '\u0aff':
		case '\u0b00':  case '\u0b01':  case '\u0b02':  case '\u0b03':
		case '\u0b04':  case '\u0b05':  case '\u0b06':  case '\u0b07':
		case '\u0b08':  case '\u0b09':  case '\u0b0a':  case '\u0b0b':
		case '\u0b0c':  case '\u0b0d':  case '\u0b0e':  case '\u0b0f':
		case '\u0b10':  case '\u0b11':  case '\u0b12':  case '\u0b13':
		case '\u0b14':  case '\u0b15':  case '\u0b16':  case '\u0b17':
		case '\u0b18':  case '\u0b19':  case '\u0b1a':  case '\u0b1b':
		case '\u0b1c':  case '\u0b1d':  case '\u0b1e':  case '\u0b1f':
		case '\u0b20':  case '\u0b21':  case '\u0b22':  case '\u0b23':
		case '\u0b24':  case '\u0b25':  case '\u0b26':  case '\u0b27':
		case '\u0b28':  case '\u0b29':  case '\u0b2a':  case '\u0b2b':
		case '\u0b2c':  case '\u0b2d':  case '\u0b2e':  case '\u0b2f':
		case '\u0b30':  case '\u0b31':  case '\u0b32':  case '\u0b33':
		case '\u0b34':  case '\u0b35':  case '\u0b36':  case '\u0b37':
		case '\u0b38':  case '\u0b39':  case '\u0b3a':  case '\u0b3b':
		case '\u0b3c':  case '\u0b3d':  case '\u0b3e':  case '\u0b3f':
		case '\u0b40':  case '\u0b41':  case '\u0b42':  case '\u0b43':
		case '\u0b44':  case '\u0b45':  case '\u0b46':  case '\u0b47':
		case '\u0b48':  case '\u0b49':  case '\u0b4a':  case '\u0b4b':
		case '\u0b4c':  case '\u0b4d':  case '\u0b4e':  case '\u0b4f':
		case '\u0b50':  case '\u0b51':  case '\u0b52':  case '\u0b53':
		case '\u0b54':  case '\u0b55':  case '\u0b56':  case '\u0b57':
		case '\u0b58':  case '\u0b59':  case '\u0b5a':  case '\u0b5b':
		case '\u0b5c':  case '\u0b5d':  case '\u0b5e':  case '\u0b5f':
		case '\u0b60':  case '\u0b61':  case '\u0b62':  case '\u0b63':
		case '\u0b64':  case '\u0b65':
		{
			matchRange('\u0af0','\u0b65');
			break;
		}
		case '\u0b70':  case '\u0b71':  case '\u0b72':  case '\u0b73':
		case '\u0b74':  case '\u0b75':  case '\u0b76':  case '\u0b77':
		case '\u0b78':  case '\u0b79':  case '\u0b7a':  case '\u0b7b':
		case '\u0b7c':  case '\u0b7d':  case '\u0b7e':  case '\u0b7f':
		case '\u0b80':  case '\u0b81':  case '\u0b82':  case '\u0b83':
		case '\u0b84':  case '\u0b85':  case '\u0b86':  case '\u0b87':
		case '\u0b88':  case '\u0b89':  case '\u0b8a':  case '\u0b8b':
		case '\u0b8c':  case '\u0b8d':  case '\u0b8e':  case '\u0b8f':
		case '\u0b90':  case '\u0b91':  case '\u0b92':  case '\u0b93':
		case '\u0b94':  case '\u0b95':  case '\u0b96':  case '\u0b97':
		case '\u0b98':  case '\u0b99':  case '\u0b9a':  case '\u0b9b':
		case '\u0b9c':  case '\u0b9d':  case '\u0b9e':  case '\u0b9f':
		case '\u0ba0':  case '\u0ba1':  case '\u0ba2':  case '\u0ba3':
		case '\u0ba4':  case '\u0ba5':  case '\u0ba6':  case '\u0ba7':
		case '\u0ba8':  case '\u0ba9':  case '\u0baa':  case '\u0bab':
		case '\u0bac':  case '\u0bad':  case '\u0bae':  case '\u0baf':
		case '\u0bb0':  case '\u0bb1':  case '\u0bb2':  case '\u0bb3':
		case '\u0bb4':  case '\u0bb5':  case '\u0bb6':  case '\u0bb7':
		case '\u0bb8':  case '\u0bb9':  case '\u0bba':  case '\u0bbb':
		case '\u0bbc':  case '\u0bbd':  case '\u0bbe':  case '\u0bbf':
		case '\u0bc0':  case '\u0bc1':  case '\u0bc2':  case '\u0bc3':
		case '\u0bc4':  case '\u0bc5':  case '\u0bc6':  case '\u0bc7':
		case '\u0bc8':  case '\u0bc9':  case '\u0bca':  case '\u0bcb':
		case '\u0bcc':  case '\u0bcd':  case '\u0bce':  case '\u0bcf':
		case '\u0bd0':  case '\u0bd1':  case '\u0bd2':  case '\u0bd3':
		case '\u0bd4':  case '\u0bd5':  case '\u0bd6':  case '\u0bd7':
		case '\u0bd8':  case '\u0bd9':  case '\u0bda':  case '\u0bdb':
		case '\u0bdc':  case '\u0bdd':  case '\u0bde':  case '\u0bdf':
		case '\u0be0':  case '\u0be1':  case '\u0be2':  case '\u0be3':
		case '\u0be4':  case '\u0be5':  case '\u0be6':
		{
			matchRange('\u0b70','\u0be6');
			break;
		}
		case '\u0bf0':  case '\u0bf1':  case '\u0bf2':  case '\u0bf3':
		case '\u0bf4':  case '\u0bf5':  case '\u0bf6':  case '\u0bf7':
		case '\u0bf8':  case '\u0bf9':  case '\u0bfa':  case '\u0bfb':
		case '\u0bfc':  case '\u0bfd':  case '\u0bfe':  case '\u0bff':
		case '\u0c00':  case '\u0c01':  case '\u0c02':  case '\u0c03':
		case '\u0c04':  case '\u0c05':  case '\u0c06':  case '\u0c07':
		case '\u0c08':  case '\u0c09':  case '\u0c0a':  case '\u0c0b':
		case '\u0c0c':  case '\u0c0d':  case '\u0c0e':  case '\u0c0f':
		case '\u0c10':  case '\u0c11':  case '\u0c12':  case '\u0c13':
		case '\u0c14':  case '\u0c15':  case '\u0c16':  case '\u0c17':
		case '\u0c18':  case '\u0c19':  case '\u0c1a':  case '\u0c1b':
		case '\u0c1c':  case '\u0c1d':  case '\u0c1e':  case '\u0c1f':
		case '\u0c20':  case '\u0c21':  case '\u0c22':  case '\u0c23':
		case '\u0c24':  case '\u0c25':  case '\u0c26':  case '\u0c27':
		case '\u0c28':  case '\u0c29':  case '\u0c2a':  case '\u0c2b':
		case '\u0c2c':  case '\u0c2d':  case '\u0c2e':  case '\u0c2f':
		case '\u0c30':  case '\u0c31':  case '\u0c32':  case '\u0c33':
		case '\u0c34':  case '\u0c35':  case '\u0c36':  case '\u0c37':
		case '\u0c38':  case '\u0c39':  case '\u0c3a':  case '\u0c3b':
		case '\u0c3c':  case '\u0c3d':  case '\u0c3e':  case '\u0c3f':
		case '\u0c40':  case '\u0c41':  case '\u0c42':  case '\u0c43':
		case '\u0c44':  case '\u0c45':  case '\u0c46':  case '\u0c47':
		case '\u0c48':  case '\u0c49':  case '\u0c4a':  case '\u0c4b':
		case '\u0c4c':  case '\u0c4d':  case '\u0c4e':  case '\u0c4f':
		case '\u0c50':  case '\u0c51':  case '\u0c52':  case '\u0c53':
		case '\u0c54':  case '\u0c55':  case '\u0c56':  case '\u0c57':
		case '\u0c58':  case '\u0c59':  case '\u0c5a':  case '\u0c5b':
		case '\u0c5c':  case '\u0c5d':  case '\u0c5e':  case '\u0c5f':
		case '\u0c60':  case '\u0c61':  case '\u0c62':  case '\u0c63':
		case '\u0c64':  case '\u0c65':
		{
			matchRange('\u0bf0','\u0c65');
			break;
		}
		case '\u0c70':  case '\u0c71':  case '\u0c72':  case '\u0c73':
		case '\u0c74':  case '\u0c75':  case '\u0c76':  case '\u0c77':
		case '\u0c78':  case '\u0c79':  case '\u0c7a':  case '\u0c7b':
		case '\u0c7c':  case '\u0c7d':  case '\u0c7e':  case '\u0c7f':
		case '\u0c80':  case '\u0c81':  case '\u0c82':  case '\u0c83':
		case '\u0c84':  case '\u0c85':  case '\u0c86':  case '\u0c87':
		case '\u0c88':  case '\u0c89':  case '\u0c8a':  case '\u0c8b':
		case '\u0c8c':  case '\u0c8d':  case '\u0c8e':  case '\u0c8f':
		case '\u0c90':  case '\u0c91':  case '\u0c92':  case '\u0c93':
		case '\u0c94':  case '\u0c95':  case '\u0c96':  case '\u0c97':
		case '\u0c98':  case '\u0c99':  case '\u0c9a':  case '\u0c9b':
		case '\u0c9c':  case '\u0c9d':  case '\u0c9e':  case '\u0c9f':
		case '\u0ca0':  case '\u0ca1':  case '\u0ca2':  case '\u0ca3':
		case '\u0ca4':  case '\u0ca5':  case '\u0ca6':  case '\u0ca7':
		case '\u0ca8':  case '\u0ca9':  case '\u0caa':  case '\u0cab':
		case '\u0cac':  case '\u0cad':  case '\u0cae':  case '\u0caf':
		case '\u0cb0':  case '\u0cb1':  case '\u0cb2':  case '\u0cb3':
		case '\u0cb4':  case '\u0cb5':  case '\u0cb6':  case '\u0cb7':
		case '\u0cb8':  case '\u0cb9':  case '\u0cba':  case '\u0cbb':
		case '\u0cbc':  case '\u0cbd':  case '\u0cbe':  case '\u0cbf':
		case '\u0cc0':  case '\u0cc1':  case '\u0cc2':  case '\u0cc3':
		case '\u0cc4':  case '\u0cc5':  case '\u0cc6':  case '\u0cc7':
		case '\u0cc8':  case '\u0cc9':  case '\u0cca':  case '\u0ccb':
		case '\u0ccc':  case '\u0ccd':  case '\u0cce':  case '\u0ccf':
		case '\u0cd0':  case '\u0cd1':  case '\u0cd2':  case '\u0cd3':
		case '\u0cd4':  case '\u0cd5':  case '\u0cd6':  case '\u0cd7':
		case '\u0cd8':  case '\u0cd9':  case '\u0cda':  case '\u0cdb':
		case '\u0cdc':  case '\u0cdd':  case '\u0cde':  case '\u0cdf':
		case '\u0ce0':  case '\u0ce1':  case '\u0ce2':  case '\u0ce3':
		case '\u0ce4':  case '\u0ce5':
		{
			matchRange('\u0c70','\u0ce5');
			break;
		}
		case '\u0cf0':  case '\u0cf1':  case '\u0cf2':  case '\u0cf3':
		case '\u0cf4':  case '\u0cf5':  case '\u0cf6':  case '\u0cf7':
		case '\u0cf8':  case '\u0cf9':  case '\u0cfa':  case '\u0cfb':
		case '\u0cfc':  case '\u0cfd':  case '\u0cfe':  case '\u0cff':
		case '\u0d00':  case '\u0d01':  case '\u0d02':  case '\u0d03':
		case '\u0d04':  case '\u0d05':  case '\u0d06':  case '\u0d07':
		case '\u0d08':  case '\u0d09':  case '\u0d0a':  case '\u0d0b':
		case '\u0d0c':  case '\u0d0d':  case '\u0d0e':  case '\u0d0f':
		case '\u0d10':  case '\u0d11':  case '\u0d12':  case '\u0d13':
		case '\u0d14':  case '\u0d15':  case '\u0d16':  case '\u0d17':
		case '\u0d18':  case '\u0d19':  case '\u0d1a':  case '\u0d1b':
		case '\u0d1c':  case '\u0d1d':  case '\u0d1e':  case '\u0d1f':
		case '\u0d20':  case '\u0d21':  case '\u0d22':  case '\u0d23':
		case '\u0d24':  case '\u0d25':  case '\u0d26':  case '\u0d27':
		case '\u0d28':  case '\u0d29':  case '\u0d2a':  case '\u0d2b':
		case '\u0d2c':  case '\u0d2d':  case '\u0d2e':  case '\u0d2f':
		case '\u0d30':  case '\u0d31':  case '\u0d32':  case '\u0d33':
		case '\u0d34':  case '\u0d35':  case '\u0d36':  case '\u0d37':
		case '\u0d38':  case '\u0d39':  case '\u0d3a':  case '\u0d3b':
		case '\u0d3c':  case '\u0d3d':  case '\u0d3e':  case '\u0d3f':
		case '\u0d40':  case '\u0d41':  case '\u0d42':  case '\u0d43':
		case '\u0d44':  case '\u0d45':  case '\u0d46':  case '\u0d47':
		case '\u0d48':  case '\u0d49':  case '\u0d4a':  case '\u0d4b':
		case '\u0d4c':  case '\u0d4d':  case '\u0d4e':  case '\u0d4f':
		case '\u0d50':  case '\u0d51':  case '\u0d52':  case '\u0d53':
		case '\u0d54':  case '\u0d55':  case '\u0d56':  case '\u0d57':
		case '\u0d58':  case '\u0d59':  case '\u0d5a':  case '\u0d5b':
		case '\u0d5c':  case '\u0d5d':  case '\u0d5e':  case '\u0d5f':
		case '\u0d60':  case '\u0d61':  case '\u0d62':  case '\u0d63':
		case '\u0d64':  case '\u0d65':
		{
			matchRange('\u0cf0','\u0d65');
			break;
		}
		case '\u0e5a':  case '\u0e5b':  case '\u0e5c':  case '\u0e5d':
		case '\u0e5e':  case '\u0e5f':  case '\u0e60':  case '\u0e61':
		case '\u0e62':  case '\u0e63':  case '\u0e64':  case '\u0e65':
		case '\u0e66':  case '\u0e67':  case '\u0e68':  case '\u0e69':
		case '\u0e6a':  case '\u0e6b':  case '\u0e6c':  case '\u0e6d':
		case '\u0e6e':  case '\u0e6f':  case '\u0e70':  case '\u0e71':
		case '\u0e72':  case '\u0e73':  case '\u0e74':  case '\u0e75':
		case '\u0e76':  case '\u0e77':  case '\u0e78':  case '\u0e79':
		case '\u0e7a':  case '\u0e7b':  case '\u0e7c':  case '\u0e7d':
		case '\u0e7e':  case '\u0e7f':  case '\u0e80':  case '\u0e81':
		case '\u0e82':  case '\u0e83':  case '\u0e84':  case '\u0e85':
		case '\u0e86':  case '\u0e87':  case '\u0e88':  case '\u0e89':
		case '\u0e8a':  case '\u0e8b':  case '\u0e8c':  case '\u0e8d':
		case '\u0e8e':  case '\u0e8f':  case '\u0e90':  case '\u0e91':
		case '\u0e92':  case '\u0e93':  case '\u0e94':  case '\u0e95':
		case '\u0e96':  case '\u0e97':  case '\u0e98':  case '\u0e99':
		case '\u0e9a':  case '\u0e9b':  case '\u0e9c':  case '\u0e9d':
		case '\u0e9e':  case '\u0e9f':  case '\u0ea0':  case '\u0ea1':
		case '\u0ea2':  case '\u0ea3':  case '\u0ea4':  case '\u0ea5':
		case '\u0ea6':  case '\u0ea7':  case '\u0ea8':  case '\u0ea9':
		case '\u0eaa':  case '\u0eab':  case '\u0eac':  case '\u0ead':
		case '\u0eae':  case '\u0eaf':  case '\u0eb0':  case '\u0eb1':
		case '\u0eb2':  case '\u0eb3':  case '\u0eb4':  case '\u0eb5':
		case '\u0eb6':  case '\u0eb7':  case '\u0eb8':  case '\u0eb9':
		case '\u0eba':  case '\u0ebb':  case '\u0ebc':  case '\u0ebd':
		case '\u0ebe':  case '\u0ebf':  case '\u0ec0':  case '\u0ec1':
		case '\u0ec2':  case '\u0ec3':  case '\u0ec4':  case '\u0ec5':
		case '\u0ec6':  case '\u0ec7':  case '\u0ec8':  case '\u0ec9':
		case '\u0eca':  case '\u0ecb':  case '\u0ecc':  case '\u0ecd':
		case '\u0ece':  case '\u0ecf':
		{
			matchRange('\u0e5a','\u0ecf');
			break;
		}
		case '\u2000':  case '\u2001':  case '\u2002':  case '\u2003':
		case '\u2004':  case '\u2005':  case '\u2006':  case '\u2007':
		case '\u2008':  case '\u2009':  case '\u200a':  case '\u200b':
		case '\u200c':  case '\u200d':  case '\u200e':  case '\u200f':
		case '\u2010':  case '\u2011':  case '\u2012':  case '\u2013':
		case '\u2014':  case '\u2015':  case '\u2016':  case '\u2017':
		case '\u2018':  case '\u2019':  case '\u201a':  case '\u201b':
		case '\u201c':  case '\u201d':  case '\u201e':  case '\u201f':
		case '\u2020':  case '\u2021':  case '\u2022':  case '\u2023':
		case '\u2024':  case '\u2025':  case '\u2026':  case '\u2027':
		case '\u2028':  case '\u2029':  case '\u202a':  case '\u202b':
		case '\u202c':  case '\u202d':  case '\u202e':  case '\u202f':
		case '\u2030':  case '\u2031':  case '\u2032':  case '\u2033':
		case '\u2034':  case '\u2035':  case '\u2036':  case '\u2037':
		case '\u2038':  case '\u2039':  case '\u203a':  case '\u203b':
		case '\u203c':  case '\u203d':  case '\u203e':  case '\u203f':
		case '\u2040':  case '\u2041':  case '\u2042':  case '\u2043':
		case '\u2044':  case '\u2045':  case '\u2046':  case '\u2047':
		case '\u2048':  case '\u2049':  case '\u204a':  case '\u204b':
		case '\u204c':  case '\u204d':  case '\u204e':  case '\u204f':
		case '\u2050':  case '\u2051':  case '\u2052':  case '\u2053':
		case '\u2054':  case '\u2055':  case '\u2056':  case '\u2057':
		case '\u2058':  case '\u2059':  case '\u205a':  case '\u205b':
		case '\u205c':  case '\u205d':  case '\u205e':  case '\u205f':
		case '\u2060':  case '\u2061':  case '\u2062':  case '\u2063':
		case '\u2064':  case '\u2065':  case '\u2066':  case '\u2067':
		case '\u2068':  case '\u2069':  case '\u206a':  case '\u206b':
		case '\u206c':  case '\u206d':  case '\u206e':  case '\u206f':
		{
			matchRange('\u2000','\u206f');
			break;
		}
		default:
			if (((LA(1) >= '\u0100' && LA(1) <= '\u065f'))) {
				matchRange('\u0100','\u065f');
			}
			else if (((LA(1) >= '\u066a' && LA(1) <= '\u06ef'))) {
				matchRange('\u066a','\u06ef');
			}
			else if (((LA(1) >= '\u06fa' && LA(1) <= '\u0965'))) {
				matchRange('\u06fa','\u0965');
			}
			else if (((LA(1) >= '\u0d70' && LA(1) <= '\u0e4f'))) {
				matchRange('\u0d70','\u0e4f');
			}
			else if (((LA(1) >= '\u0eda' && LA(1) <= '\u103f'))) {
				matchRange('\u0eda','\u103f');
			}
			else if (((LA(1) >= '\u104a' && LA(1) <= '\u1fff'))) {
				matchRange('\u104a','\u1fff');
			}
			else if (((LA(1) >= '\u3040' && LA(1) <= '\u318f'))) {
				matchRange('\u3040','\u318f');
			}
			else if (((LA(1) >= '\u3300' && LA(1) <= '\u337f'))) {
				matchRange('\u3300','\u337f');
			}
			else if (((LA(1) >= '\u3400' && LA(1) <= '\u3d2d'))) {
				matchRange('\u3400','\u3d2d');
			}
			else if (((LA(1) >= '\u4e00' && LA(1) <= '\u9fff'))) {
				matchRange('\u4e00','\u9fff');
			}
			else if (((LA(1) >= '\uf900' && LA(1) <= '\ufaff'))) {
				matchRange('\uf900','\ufaff');
			}
		else {
			throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
		}
		}
		}
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	protected final void mDIGIT(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = DIGIT;
		int _saveIndex;
		
		{
		switch ( LA(1)) {
		case '0':  case '1':  case '2':  case '3':
		case '4':  case '5':  case '6':  case '7':
		case '8':  case '9':
		{
			matchRange('\u0030','\u0039');
			break;
		}
		case '\u0660':  case '\u0661':  case '\u0662':  case '\u0663':
		case '\u0664':  case '\u0665':  case '\u0666':  case '\u0667':
		case '\u0668':  case '\u0669':
		{
			matchRange('\u0660','\u0669');
			break;
		}
		case '\u06f0':  case '\u06f1':  case '\u06f2':  case '\u06f3':
		case '\u06f4':  case '\u06f5':  case '\u06f6':  case '\u06f7':
		case '\u06f8':  case '\u06f9':
		{
			matchRange('\u06f0','\u06f9');
			break;
		}
		case '\u0966':  case '\u0967':  case '\u0968':  case '\u0969':
		case '\u096a':  case '\u096b':  case '\u096c':  case '\u096d':
		case '\u096e':  case '\u096f':
		{
			matchRange('\u0966','\u096f');
			break;
		}
		case '\u09e6':  case '\u09e7':  case '\u09e8':  case '\u09e9':
		case '\u09ea':  case '\u09eb':  case '\u09ec':  case '\u09ed':
		case '\u09ee':  case '\u09ef':
		{
			matchRange('\u09e6','\u09ef');
			break;
		}
		case '\u0a66':  case '\u0a67':  case '\u0a68':  case '\u0a69':
		case '\u0a6a':  case '\u0a6b':  case '\u0a6c':  case '\u0a6d':
		case '\u0a6e':  case '\u0a6f':
		{
			matchRange('\u0a66','\u0a6f');
			break;
		}
		case '\u0ae6':  case '\u0ae7':  case '\u0ae8':  case '\u0ae9':
		case '\u0aea':  case '\u0aeb':  case '\u0aec':  case '\u0aed':
		case '\u0aee':  case '\u0aef':
		{
			matchRange('\u0ae6','\u0aef');
			break;
		}
		case '\u0b66':  case '\u0b67':  case '\u0b68':  case '\u0b69':
		case '\u0b6a':  case '\u0b6b':  case '\u0b6c':  case '\u0b6d':
		case '\u0b6e':  case '\u0b6f':
		{
			matchRange('\u0b66','\u0b6f');
			break;
		}
		case '\u0be7':  case '\u0be8':  case '\u0be9':  case '\u0bea':
		case '\u0beb':  case '\u0bec':  case '\u0bed':  case '\u0bee':
		case '\u0bef':
		{
			matchRange('\u0be7','\u0bef');
			break;
		}
		case '\u0c66':  case '\u0c67':  case '\u0c68':  case '\u0c69':
		case '\u0c6a':  case '\u0c6b':  case '\u0c6c':  case '\u0c6d':
		case '\u0c6e':  case '\u0c6f':
		{
			matchRange('\u0c66','\u0c6f');
			break;
		}
		case '\u0ce6':  case '\u0ce7':  case '\u0ce8':  case '\u0ce9':
		case '\u0cea':  case '\u0ceb':  case '\u0cec':  case '\u0ced':
		case '\u0cee':  case '\u0cef':
		{
			matchRange('\u0ce6','\u0cef');
			break;
		}
		case '\u0d66':  case '\u0d67':  case '\u0d68':  case '\u0d69':
		case '\u0d6a':  case '\u0d6b':  case '\u0d6c':  case '\u0d6d':
		case '\u0d6e':  case '\u0d6f':
		{
			matchRange('\u0d66','\u0d6f');
			break;
		}
		case '\u0e50':  case '\u0e51':  case '\u0e52':  case '\u0e53':
		case '\u0e54':  case '\u0e55':  case '\u0e56':  case '\u0e57':
		case '\u0e58':  case '\u0e59':
		{
			matchRange('\u0e50','\u0e59');
			break;
		}
		case '\u0ed0':  case '\u0ed1':  case '\u0ed2':  case '\u0ed3':
		case '\u0ed4':  case '\u0ed5':  case '\u0ed6':  case '\u0ed7':
		case '\u0ed8':  case '\u0ed9':
		{
			matchRange('\u0ed0','\u0ed9');
			break;
		}
		case '\u1040':  case '\u1041':  case '\u1042':  case '\u1043':
		case '\u1044':  case '\u1045':  case '\u1046':  case '\u1047':
		case '\u1048':  case '\u1049':
		{
			matchRange('\u1040','\u1049');
			break;
		}
		default:
		{
			throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
		}
		}
		}
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	protected final void mALL_UNICODE(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = ALL_UNICODE;
		int _saveIndex;
		
		{
		matchRange('\u0061','\ufffd');
		}
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	protected final void mNameFirstCharacter(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = NameFirstCharacter;
		int _saveIndex;
		
		{
		if ((_tokenSet_2.member(LA(1)))) {
			mLETTER(false);
		}
		else if ((LA(1)=='_')) {
			match('_');
		}
		else {
			throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
		}
		
		}
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	protected final void mNameCharacter(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = NameCharacter;
		int _saveIndex;
		
		{
		switch ( LA(1)) {
		case '_':
		{
			match('_');
			break;
		}
		case '$':
		{
			match('$');
			break;
		}
		default:
			if ((_tokenSet_2.member(LA(1)))) {
				mLETTER(false);
			}
			else if ((_tokenSet_3.member(LA(1)))) {
				mDIGIT(false);
			}
		else {
			throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
		}
		}
		}
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	protected final void mRegionNameCharacter(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = RegionNameCharacter;
		int _saveIndex;
		
		{
		switch ( LA(1)) {
		case '_':
		{
			match('_');
			break;
		}
		case '+':
		{
			match('+');
			break;
		}
		case '-':
		{
			match('-');
			break;
		}
		case ':':
		{
			match(':');
			break;
		}
		case '#':
		{
			match('#');
			break;
		}
		case '@':
		{
			match('@');
			break;
		}
		case '$':
		{
			match('$');
			break;
		}
		default:
			if (((LA(1) >= 'a' && LA(1) <= '\ufffd')) && (true)) {
				mALL_UNICODE(false);
			}
			else if ((_tokenSet_3.member(LA(1))) && (true)) {
				mDIGIT(false);
			}
		else {
			throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
		}
		}
		}
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mQuotedIdentifier(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = QuotedIdentifier;
		int _saveIndex;
		
		_saveIndex=text.length();
		match('"');
		text.setLength(_saveIndex);
		mNameFirstCharacter(false);
		{
		_loop38:
		do {
			if ((_tokenSet_4.member(LA(1)))) {
				mNameCharacter(false);
			}
			else {
				break _loop38;
			}
			
		} while (true);
		}
		_saveIndex=text.length();
		match('"');
		text.setLength(_saveIndex);
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mIdentifier(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = Identifier;
		int _saveIndex;
		
		mNameFirstCharacter(false);
		{
		_loop41:
		do {
			if ((_tokenSet_4.member(LA(1)))) {
				mNameCharacter(false);
			}
			else {
				break _loop41;
			}
			
		} while (true);
		}
		_ttype = testLiteralsTable(_ttype);
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mRegionPath(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = RegionPath;
		int _saveIndex;
		
		{
		if ((LA(1)=='/') && (_tokenSet_5.member(LA(2)))) {
			{
			{
			int _cnt48=0;
			_loop48:
			do {
				if ((LA(1)=='/')) {
					mTOK_SLASH(false);
					{
					int _cnt47=0;
					_loop47:
					do {
						if ((_tokenSet_5.member(LA(1)))) {
							mRegionNameCharacter(false);
						}
						else {
							if ( _cnt47>=1 ) { break _loop47; } else {throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());}
						}
						
						_cnt47++;
					} while (true);
					}
				}
				else {
					if ( _cnt48>=1 ) { break _loop48; } else {throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());}
				}
				
				_cnt48++;
			} while (true);
			}
			}
		}
		else if ((LA(1)=='/') && (LA(2)=='\'')) {
			{
			{
			int _cnt51=0;
			_loop51:
			do {
				if ((LA(1)=='/')) {
					mTOK_SLASH(false);
					mStringLiteral(false);
				}
				else {
					if ( _cnt51>=1 ) { break _loop51; } else {throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());}
				}
				
				_cnt51++;
			} while (true);
			}
			}
		}
		else {
			throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
		}
		
		}
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mStringLiteral(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = StringLiteral;
		int _saveIndex;
		
		_saveIndex=text.length();
		mQUOTE(false);
		text.setLength(_saveIndex);
		{
		_loop87:
		do {
			if ((LA(1)=='\'') && (LA(2)=='\'')) {
				mQUOTE(false);
				_saveIndex=text.length();
				mQUOTE(false);
				text.setLength(_saveIndex);
			}
			else if ((LA(1)=='\n')) {
				match('\n');
				if ( inputState.guessing==0 ) {
					newline();
				}
			}
			else if ((_tokenSet_6.member(LA(1)))) {
				{
				match(_tokenSet_6);
				}
			}
			else {
				break _loop87;
			}
			
		} while (true);
		}
		_saveIndex=text.length();
		mQUOTE(false);
		text.setLength(_saveIndex);
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mNUM_INT(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = NUM_INT;
		int _saveIndex;
		Token f2=null;
		Token f3=null;
		Token f4=null;
		boolean isDecimal=false; Token t=null;
		
		{
		switch ( LA(1)) {
		case '0':
		{
			match('0');
			if ( inputState.guessing==0 ) {
				isDecimal = true;
			}
			{
			if ((LA(1)=='x')) {
				match('x');
				{
				int _cnt56=0;
				_loop56:
				do {
					if ((_tokenSet_7.member(LA(1))) && (true)) {
						mHEX_DIGIT(false);
					}
					else {
						if ( _cnt56>=1 ) { break _loop56; } else {throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());}
					}
					
					_cnt56++;
				} while (true);
				}
			}
			else {
				boolean synPredMatched61 = false;
				if ((((LA(1) >= '0' && LA(1) <= '9')) && (true))) {
					int _m61 = mark();
					synPredMatched61 = true;
					inputState.guessing++;
					try {
						{
						{
						int _cnt59=0;
						_loop59:
						do {
							if (((LA(1) >= '0' && LA(1) <= '9'))) {
								matchRange('0','9');
							}
							else {
								if ( _cnt59>=1 ) { break _loop59; } else {throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());}
							}
							
							_cnt59++;
						} while (true);
						}
						{
						switch ( LA(1)) {
						case '.':
						{
							match('.');
							break;
						}
						case 'e':
						{
							mEXPONENT(false);
							break;
						}
						case 'd':  case 'f':
						{
							mFLOAT_SUFFIX(false);
							break;
						}
						default:
						{
							throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
						}
						}
						}
						}
					}
					catch (RecognitionException pe) {
						synPredMatched61 = false;
					}
					rewind(_m61);
					inputState.guessing--;
				}
				if ( synPredMatched61 ) {
					{
					int _cnt63=0;
					_loop63:
					do {
						if (((LA(1) >= '0' && LA(1) <= '9'))) {
							matchRange('0','9');
						}
						else {
							if ( _cnt63>=1 ) { break _loop63; } else {throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());}
						}
						
						_cnt63++;
					} while (true);
					}
				}
				else if (((LA(1) >= '0' && LA(1) <= '7')) && (true)) {
					{
					int _cnt65=0;
					_loop65:
					do {
						if (((LA(1) >= '0' && LA(1) <= '7'))) {
							matchRange('0','7');
						}
						else {
							if ( _cnt65>=1 ) { break _loop65; } else {throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());}
						}
						
						_cnt65++;
					} while (true);
					}
				}
				else {
				}
				}
				}
				break;
			}
			case '1':  case '2':  case '3':  case '4':
			case '5':  case '6':  case '7':  case '8':
			case '9':
			{
				{
				matchRange('1','9');
				}
				{
				_loop68:
				do {
					if (((LA(1) >= '0' && LA(1) <= '9'))) {
						matchRange('0','9');
					}
					else {
						break _loop68;
					}
					
				} while (true);
				}
				if ( inputState.guessing==0 ) {
					isDecimal=true;
				}
				break;
			}
			default:
			{
				throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
			}
			}
			}
			{
			if ((LA(1)=='l')) {
				match('l');
				if ( inputState.guessing==0 ) {
					_ttype = NUM_LONG;
				}
			}
			else if (((_tokenSet_8.member(LA(1))))&&(isDecimal)) {
				{
				switch ( LA(1)) {
				case '.':
				{
					match('.');
					{
					_loop72:
					do {
						if (((LA(1) >= '0' && LA(1) <= '9'))) {
							matchRange('0','9');
						}
						else {
							break _loop72;
						}
						
					} while (true);
					}
					{
					if ((LA(1)=='e')) {
						mEXPONENT(false);
					}
					else {
					}
					
					}
					{
					if ((LA(1)=='d'||LA(1)=='f')) {
						mFLOAT_SUFFIX(true);
						f2=_returnToken;
						if ( inputState.guessing==0 ) {
							t=f2;
						}
					}
					else {
					}
					
					}
					break;
				}
				case 'e':
				{
					mEXPONENT(false);
					{
					if ((LA(1)=='d'||LA(1)=='f')) {
						mFLOAT_SUFFIX(true);
						f3=_returnToken;
						if ( inputState.guessing==0 ) {
							t=f3;
						}
					}
					else {
					}
					
					}
					break;
				}
				case 'd':  case 'f':
				{
					mFLOAT_SUFFIX(true);
					f4=_returnToken;
					if ( inputState.guessing==0 ) {
						t=f4;
					}
					break;
				}
				default:
				{
					throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
				}
				}
				}
				if ( inputState.guessing==0 ) {
					
								if (t != null && t.getText().toUpperCase() .indexOf('F') >= 0) {
					_ttype = NUM_FLOAT;
								}
					else {
						           	_ttype = NUM_DOUBLE; // assume double
								}
								
				}
			}
			else {
			}
			
			}
			if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
				_token = makeToken(_ttype);
				_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
			}
			_returnToken = _token;
		}
		
	protected final void mHEX_DIGIT(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = HEX_DIGIT;
		int _saveIndex;
		
		{
		switch ( LA(1)) {
		case '0':  case '1':  case '2':  case '3':
		case '4':  case '5':  case '6':  case '7':
		case '8':  case '9':
		{
			matchRange('0','9');
			break;
		}
		case 'a':  case 'b':  case 'c':  case 'd':
		case 'e':  case 'f':
		{
			matchRange('a','f');
			break;
		}
		default:
		{
			throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
		}
		}
		}
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	protected final void mEXPONENT(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = EXPONENT;
		int _saveIndex;
		
		match('e');
		{
		switch ( LA(1)) {
		case '+':
		{
			match('+');
			break;
		}
		case '-':
		{
			match('-');
			break;
		}
		case '0':  case '1':  case '2':  case '3':
		case '4':  case '5':  case '6':  case '7':
		case '8':  case '9':
		{
			break;
		}
		default:
		{
			throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
		}
		}
		}
		{
		int _cnt79=0;
		_loop79:
		do {
			if (((LA(1) >= '0' && LA(1) <= '9'))) {
				matchRange('0','9');
			}
			else {
				if ( _cnt79>=1 ) { break _loop79; } else {throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());}
			}
			
			_cnt79++;
		} while (true);
		}
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	protected final void mFLOAT_SUFFIX(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = FLOAT_SUFFIX;
		int _saveIndex;
		
		switch ( LA(1)) {
		case 'f':
		{
			match('f');
			break;
		}
		case 'd':
		{
			match('d');
			break;
		}
		default:
		{
			throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
		}
		}
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	protected final void mQUOTE(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = QUOTE;
		int _saveIndex;
		
		match('\'');
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mWS(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = WS;
		int _saveIndex;
		
		{
		int _cnt91=0;
		_loop91:
		do {
			switch ( LA(1)) {
			case ' ':
			{
				match(' ');
				break;
			}
			case '\t':
			{
				match('\t');
				break;
			}
			case '\u000c':
			{
				match('\f');
				break;
			}
			case '\n':  case '\r':
			{
				{
				if ((LA(1)=='\r') && (LA(2)=='\n')) {
					match("\r\n");
				}
				else if ((LA(1)=='\r') && (true)) {
					match('\r');
				}
				else if ((LA(1)=='\n')) {
					match('\n');
				}
				else {
					throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());
				}
				
				}
				if ( inputState.guessing==0 ) {
					newline();
				}
				break;
			}
			default:
			{
				if ( _cnt91>=1 ) { break _loop91; } else {throw new NoViableAltForCharException((char)LA(1), getFilename(), getLine(), getColumn());}
			}
			}
			_cnt91++;
		} while (true);
		}
		if ( inputState.guessing==0 ) {
			_ttype = Token.SKIP;
		}
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mSL_COMMENT(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = SL_COMMENT;
		int _saveIndex;
		
		match("--");
		{
		_loop95:
		do {
			if ((_tokenSet_9.member(LA(1)))) {
				{
				match(_tokenSet_9);
				}
			}
			else {
				break _loop95;
			}
			
		} while (true);
		}
		{
		switch ( LA(1)) {
		case '\n':
		{
			match('\n');
			break;
		}
		case '\r':
		{
			match('\r');
			{
			if ((LA(1)=='\n')) {
				match('\n');
			}
			else {
			}
			
			}
			break;
		}
		default:
			{
			}
		}
		}
		if ( inputState.guessing==0 ) {
			_ttype = Token.SKIP; newline();
		}
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	public final void mML_COMMENT(boolean _createToken) throws RecognitionException, CharStreamException, TokenStreamException {
		int _ttype; Token _token=null; int _begin=text.length();
		_ttype = ML_COMMENT;
		int _saveIndex;
		
		match("/*");
		{
		_loop101:
		do {
			if (((LA(1)=='*') && ((LA(2) >= '\u0000' && LA(2) <= '\ufffe')))&&( LA(2)!='/' )) {
				match('*');
			}
			else if ((LA(1)=='\r') && (LA(2)=='\n')) {
				match('\r');
				match('\n');
				if ( inputState.guessing==0 ) {
					newline();
				}
			}
			else if ((LA(1)=='\r') && ((LA(2) >= '\u0000' && LA(2) <= '\ufffe'))) {
				match('\r');
				if ( inputState.guessing==0 ) {
					newline();
				}
			}
			else if ((LA(1)=='\n')) {
				match('\n');
				if ( inputState.guessing==0 ) {
					newline();
				}
			}
			else if ((_tokenSet_10.member(LA(1)))) {
				{
				match(_tokenSet_10);
				}
			}
			else {
				break _loop101;
			}
			
		} while (true);
		}
		match("*/");
		if ( inputState.guessing==0 ) {
			_ttype = Token.SKIP;
		}
		if ( _createToken && _token==null && _ttype!=Token.SKIP ) {
			_token = makeToken(_ttype);
			_token.setText(new String(text.getBuffer(), _begin, text.length()-_begin));
		}
		_returnToken = _token;
	}
	
	
	private static final long[] mk_tokenSet_0() {
		long[] data = new long[2048];
		data[0]=576223910626852864L;
		data[1]=-6442450943L;
		for (int i = 2; i<=1022; i++) { data[i]=-1L; }
		data[1023]=4611686018427387903L;
		return data;
	}
	public static final BitSet _tokenSet_0 = new BitSet(mk_tokenSet_0());
	private static final long[] mk_tokenSet_1() {
		long[] data = new long[3988];
		data[1]=576460745860972544L;
		data[2]=-8589934592L;
		data[3]=-36028797027352577L;
		for (int i = 4; i<=24; i++) { data[i]=-1L; }
		data[25]=-4393751543809L;
		data[26]=-1L;
		data[27]=-287948901175001089L;
		for (int i = 28; i<=36; i++) { data[i]=-1L; }
		data[37]=-281200098803713L;
		data[38]=-1L;
		data[39]=-281200098803713L;
		data[40]=-1L;
		data[41]=-281200098803713L;
		data[42]=-1L;
		data[43]=-281200098803713L;
		data[44]=-1L;
		data[45]=-281200098803713L;
		data[46]=-1L;
		data[47]=-280925220896769L;
		data[48]=-1L;
		data[49]=-281200098803713L;
		data[50]=-1L;
		data[51]=-281200098803713L;
		data[52]=-1L;
		data[53]=-281200098803713L;
		for (int i = 54; i<=56; i++) { data[i]=-1L; }
		data[57]=-67043329L;
		data[58]=-1L;
		data[59]=-67043329L;
		for (int i = 60; i<=64; i++) { data[i]=-1L; }
		data[65]=-1024L;
		for (int i = 66; i<=128; i++) { data[i]=-1L; }
		data[129]=281474976710655L;
		for (int i = 193; i<=197; i++) { data[i]=-1L; }
		data[198]=65535L;
		for (int i = 204; i<=205; i++) { data[i]=-1L; }
		for (int i = 208; i<=243; i++) { data[i]=-1L; }
		data[244]=70368744177663L;
		for (int i = 312; i<=639; i++) { data[i]=-1L; }
		for (int i = 996; i<=1003; i++) { data[i]=-1L; }
		return data;
	}
	public static final BitSet _tokenSet_1 = new BitSet(mk_tokenSet_1());
	private static final long[] mk_tokenSet_2() {
		long[] data = new long[3988];
		data[1]=576460743713488896L;
		data[2]=-8589934592L;
		data[3]=-36028797027352577L;
		for (int i = 4; i<=24; i++) { data[i]=-1L; }
		data[25]=-4393751543809L;
		data[26]=-1L;
		data[27]=-287948901175001089L;
		for (int i = 28; i<=36; i++) { data[i]=-1L; }
		data[37]=-281200098803713L;
		data[38]=-1L;
		data[39]=-281200098803713L;
		data[40]=-1L;
		data[41]=-281200098803713L;
		data[42]=-1L;
		data[43]=-281200098803713L;
		data[44]=-1L;
		data[45]=-281200098803713L;
		data[46]=-1L;
		data[47]=-280925220896769L;
		data[48]=-1L;
		data[49]=-281200098803713L;
		data[50]=-1L;
		data[51]=-281200098803713L;
		data[52]=-1L;
		data[53]=-281200098803713L;
		for (int i = 54; i<=56; i++) { data[i]=-1L; }
		data[57]=-67043329L;
		data[58]=-1L;
		data[59]=-67043329L;
		for (int i = 60; i<=64; i++) { data[i]=-1L; }
		data[65]=-1024L;
		for (int i = 66; i<=128; i++) { data[i]=-1L; }
		data[129]=281474976710655L;
		for (int i = 193; i<=197; i++) { data[i]=-1L; }
		data[198]=65535L;
		for (int i = 204; i<=205; i++) { data[i]=-1L; }
		for (int i = 208; i<=243; i++) { data[i]=-1L; }
		data[244]=70368744177663L;
		for (int i = 312; i<=639; i++) { data[i]=-1L; }
		for (int i = 996; i<=1003; i++) { data[i]=-1L; }
		return data;
	}
	public static final BitSet _tokenSet_2 = new BitSet(mk_tokenSet_2());
	private static final long[] mk_tokenSet_3() {
		long[] data = new long[1025];
		data[0]=287948901175001088L;
		data[25]=4393751543808L;
		data[27]=287948901175001088L;
		data[37]=281200098803712L;
		data[39]=281200098803712L;
		data[41]=281200098803712L;
		data[43]=281200098803712L;
		data[45]=281200098803712L;
		data[47]=280925220896768L;
		data[49]=281200098803712L;
		data[51]=281200098803712L;
		data[53]=281200098803712L;
		data[57]=67043328L;
		data[59]=67043328L;
		data[65]=1023L;
		return data;
	}
	public static final BitSet _tokenSet_3 = new BitSet(mk_tokenSet_3());
	private static final long[] mk_tokenSet_4() {
		long[] data = new long[3988];
		data[0]=287948969894477824L;
		data[1]=576460745860972544L;
		data[2]=-8589934592L;
		data[3]=-36028797027352577L;
		for (int i = 4; i<=128; i++) { data[i]=-1L; }
		data[129]=281474976710655L;
		for (int i = 193; i<=197; i++) { data[i]=-1L; }
		data[198]=65535L;
		for (int i = 204; i<=205; i++) { data[i]=-1L; }
		for (int i = 208; i<=243; i++) { data[i]=-1L; }
		data[244]=70368744177663L;
		for (int i = 312; i<=639; i++) { data[i]=-1L; }
		for (int i = 996; i<=1003; i++) { data[i]=-1L; }
		return data;
	}
	public static final BitSet _tokenSet_4 = new BitSet(mk_tokenSet_4());
	private static final long[] mk_tokenSet_5() {
		long[] data = new long[2048];
		data[0]=576223360871038976L;
		data[1]=-6442450943L;
		for (int i = 2; i<=1022; i++) { data[i]=-1L; }
		data[1023]=4611686018427387903L;
		return data;
	}
	public static final BitSet _tokenSet_5 = new BitSet(mk_tokenSet_5());
	private static final long[] mk_tokenSet_6() {
		long[] data = new long[2048];
		data[0]=-549755814913L;
		for (int i = 1; i<=1022; i++) { data[i]=-1L; }
		data[1023]=9223372036854775807L;
		return data;
	}
	public static final BitSet _tokenSet_6 = new BitSet(mk_tokenSet_6());
	private static final long[] mk_tokenSet_7() {
		long[] data = new long[1025];
		data[0]=287948901175001088L;
		data[1]=541165879296L;
		return data;
	}
	public static final BitSet _tokenSet_7 = new BitSet(mk_tokenSet_7());
	private static final long[] mk_tokenSet_8() {
		long[] data = new long[1025];
		data[0]=70368744177664L;
		data[1]=481036337152L;
		return data;
	}
	public static final BitSet _tokenSet_8 = new BitSet(mk_tokenSet_8());
	private static final long[] mk_tokenSet_9() {
		long[] data = new long[2048];
		data[0]=-9217L;
		for (int i = 1; i<=1022; i++) { data[i]=-1L; }
		data[1023]=9223372036854775807L;
		return data;
	}
	public static final BitSet _tokenSet_9 = new BitSet(mk_tokenSet_9());
	private static final long[] mk_tokenSet_10() {
		long[] data = new long[2048];
		data[0]=-4398046520321L;
		for (int i = 1; i<=1022; i++) { data[i]=-1L; }
		data[1023]=9223372036854775807L;
		return data;
	}
	public static final BitSet _tokenSet_10 = new BitSet(mk_tokenSet_10());
	
	}
