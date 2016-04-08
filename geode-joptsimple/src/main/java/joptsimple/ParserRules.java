/*
 The MIT License

 Copyright (c) 2004-2011 Paul R. Holser, Jr.

 Permission is hereby granted, free of charge, to any person obtaining
 a copy of this software and associated documentation files (the
 "Software"), to deal in the Software without restriction, including
 without limitation the rights to use, copy, modify, merge, publish,
 distribute, sublicense, and/or sell copies of the Software, and to
 permit persons to whom the Software is furnished to do so, subject to
 the following conditions:

 The above copyright notice and this permission notice shall be
 included in all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

package joptsimple;

import java.util.Collection;

import static java.lang.Character.*;

/**
 * Can tell whether or not options are well-formed.
 *
 * @author <a href="mailto:pholser@alumni.rice.edu">Paul Holser</a>
 */
final class ParserRules {
    static final char HYPHEN_CHAR = '-';
    static final String HYPHEN = String.valueOf( HYPHEN_CHAR );
    static final String DOUBLE_HYPHEN = "--";
    static final String OPTION_TERMINATOR = DOUBLE_HYPHEN;
    static final String RESERVED_FOR_EXTENSIONS = "W";

    private ParserRules() {
        throw new UnsupportedOperationException();
    }

    static boolean isShortOptionToken( String argument ) {
        return argument.startsWith( HYPHEN )
            && !HYPHEN.equals( argument )
            && !isLongOptionToken( argument );
    }

    static boolean isLongOptionToken( String argument ) {
        return argument.startsWith( DOUBLE_HYPHEN ) && !isOptionTerminator( argument );
    }

    static boolean isOptionTerminator( String argument ) {
        return OPTION_TERMINATOR.equals( argument );
    }

    static void ensureLegalOption( String option ) {
        if ( option.startsWith( HYPHEN ) )
            throw new IllegalOptionSpecificationException( String.valueOf( option ) );

        for ( int i = 0; i < option.length(); ++i )
            ensureLegalOptionCharacter( option.charAt( i ) );
    }

    static void ensureLegalOptions( Collection<String> options ) {
        for ( String each : options )
            ensureLegalOption( each );
    }

    private static void ensureLegalOptionCharacter( char option ) {
        if ( !( isLetterOrDigit( option ) || isAllowedPunctuation( option ) ) )
            throw new IllegalOptionSpecificationException( String.valueOf( option ) );
    }

    private static boolean isAllowedPunctuation( char option ) {
        String allowedPunctuation = "?." + HYPHEN_CHAR;
        return allowedPunctuation.indexOf( option ) != -1;
    }
}
