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

/**
 * Specification of an option that accepts an optional argument.
 *
 * @param <V> represents the type of the arguments this option accepts
 * @author <a href="mailto:pholser@alumni.rice.edu">Paul Holser</a>
 */
class OptionalArgumentOptionSpec<V> extends ArgumentAcceptingOptionSpec<V> {
    OptionalArgumentOptionSpec( String option ) {
        super( option, false );
    }

    OptionalArgumentOptionSpec( Collection<String> options, String description ) {
        super( options, false, description );
    }

    @Override
    protected void detectOptionArgument( OptionParser parser, ArgumentList arguments, OptionSet detectedOptions ) {
        if ( arguments.hasMore() ) {
            String nextArgument = arguments.peek();

            if ( !parser.looksLikeAnOption( nextArgument ) )
                handleOptionArgument( parser, detectedOptions, arguments );
            else if ( isArgumentOfNumberType() && canConvertArgument( nextArgument ) )
                addArguments( detectedOptions, arguments.next() );
            else
                detectedOptions.add( this );
        }
        else
            detectedOptions.add( this );
    }

    private void handleOptionArgument( OptionParser parser, OptionSet detectedOptions, ArgumentList arguments ) {
        if ( parser.posixlyCorrect() ) {
            detectedOptions.add( this );
            parser.noMoreOptions();
        }
        else
            addArguments( detectedOptions, arguments.next() );
    }
}
