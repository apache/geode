package com.gemstone.gemfire.codeAnalysis.decode.cp;
import java.io.*;

public class CpString extends Cp {
    int string_index;  // har har - points to a Utf8 holding the string's guts
    CpString( DataInputStream source ) throws IOException {
        string_index = source.readUnsignedShort();
    }
}