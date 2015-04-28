set -e
sed -e 's/(char)//' OQLLexer.java >x.java
mv x.java OQLLexer.java

# TODO: this will leave two errors in the resulting file.  So deal
# with it.
sed -e '%s/[ 	]*int _saveIndex;//' OQLLexer.java >x.java
mv x.java OQLLexer.java

sed -e 's/_token==null &&/' OQLLexer.java >x.java
mv x.java OQLLexer.java

# Really, there's only one occurrence and it's the  first one...
sed -e 's/Token _token = null;//' OQLLexer.java >x.java
mv x.java OQLLexer.java

sed -e 's/.*theRetToken.*//' OQLLexer.java >x.java
mv x.java OQLLexer.java

# --------------------------------

sed -e 's/(AST)//g' OQLParser.java >x.java
mv x.java OQLParser.java

# Only one occurrence...
sed -e 's/methodInvocation_AST!=null &&//' OQLParser.java >x.java
mv x.java OQLParser.java

# Ummm...I think this name could change...
# (variable never read)
sed -e 's/AST tmp32_AST = null;//' OQLParser.java >x.java
mv x.java OQLParser.java
sed -e 's/tmp32_AST = //' OQLParser.java >x.java
mv x.java OQLParser.java

sed -e 's/type_AST!=null &&//' OQLParser.java >x.java
mv x.java OQLParser.java
