/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file is an adaptation of Presto's presto-parser/src/main/antlr4/com/facebook/presto/sql/parser/SqlBase.g4 grammar.
 */

grammar SqlBase;

tokens {
    DELIMITER
}

singleStatement
    : statement EOF
    ;

singleExpression
    : expression EOF
    ;

statement
    : query                                                            #statementDefault
    | IMPORT FROM CSV FILE STRING INTO qualifiedName
        ('(' columnDefinition (',' columnDefinition)* ')')?            #importCsv
    ;

query
    : queryTerm
    ;

queryTerm
    : queryPrimary                                                             #queryTermDefault
    ;

queryPrimary
    : querySpecification                   #queryPrimaryDefault
    | diffQuerySpecification               #diffQuery
    | TABLE qualifiedName                  #table
    | '(' query  ')'                       #subquery
    ;

querySpecification
    : SELECT setQuantifier? selectItem (',' selectItem)*
      (FROM relation (',' relation)*)?
      (WHERE where=booleanExpression)?
      (ORDER BY sortItem (',' sortItem)*)?
      (LIMIT limit=(INTEGER_VALUE | ALL))?
      exportClause?
    | SELECT setQuantifier? selectItem (',' selectItem)*
      exportClause?
      (FROM relation (',' relation)*)?
      (WHERE where=booleanExpression)?
      (ORDER BY sortItem (',' sortItem)*)?
      (LIMIT limit=(INTEGER_VALUE | ALL))?
    ;

diffQuerySpecification
    : SELECT setQuantifier? selectItem (',' selectItem)*
      FROM (ANTI?) DIFF queryTerm qualifiedName? ',' queryTerm qualifiedName?
      ON (columnAliases | ASTERISK)
      (WITH (minRatioExpression? minSupportExpression? | minSupportExpression? minRatioExpression?))?
      (COMPARE BY ratioMetricExpression)?
      (MAX COMBO maxCombo=INTEGER_VALUE)?
      (WHERE where=booleanExpression)?
      (ORDER BY sortItem (',' sortItem)*)?
      (LIMIT limit=(INTEGER_VALUE | ALL))?
      exportClause?
    | SELECT setQuantifier? selectItem (',' selectItem)*
      exportClause?
      FROM (ANTI?) DIFF queryTerm qualifiedName? ',' queryTerm qualifiedName?
      ON (columnAliases | ASTERISK)
      (WITH (minRatioExpression? minSupportExpression? | minSupportExpression? minRatioExpression?))?
      (COMPARE BY ratioMetricExpression)?
      (MAX COMBO maxCombo=INTEGER_VALUE)?
      (WHERE where=booleanExpression)?
      (ORDER BY sortItem (',' sortItem)*)?
      (LIMIT limit=(INTEGER_VALUE | ALL))?
    | SELECT setQuantifier? selectItem (',' selectItem)*
      FROM (ANTI?) DIFF '(' splitQuery ')'
      ON (columnAliases | ASTERISK)
      (WITH (minRatioExpression? minSupportExpression? | minSupportExpression? minRatioExpression?))?
      (COMPARE BY ratioMetricExpression)?
      (MAX COMBO maxCombo=INTEGER_VALUE)?
      (WHERE where=booleanExpression)?
      (ORDER BY sortItem (',' sortItem)*)?
      (LIMIT limit=(INTEGER_VALUE | ALL))?
      exportClause?
    | SELECT setQuantifier? selectItem (',' selectItem)*
      exportClause?
      FROM (ANTI?) DIFF '(' splitQuery ')'
      ON (columnAliases | ASTERISK)
      (WITH (minRatioExpression? minSupportExpression? | minSupportExpression? minRatioExpression?))?
      (COMPARE BY ratioMetricExpression)?
      (MAX COMBO maxCombo=INTEGER_VALUE)?
      (WHERE where=booleanExpression)?
      (ORDER BY sortItem (',' sortItem)*)?
      (LIMIT limit=(INTEGER_VALUE | ALL))?
    ;

splitQuery
    :  SPLIT relation WHERE where=booleanExpression
    |  SPLIT queryTerm WHERE where=booleanExpression
    ;

columnDefinition
    : identifier type (COMMENT string)?
    ;

sortItem
    : expression ordering=(ASC | DESC)? (NULLS nullOrdering=(FIRST | LAST))?
    ;

minRatioExpression
    : MIN RATIO minRatio=DECIMAL_VALUE
    ;

minSupportExpression
    : MIN_SUPPORT minSupport=DECIMAL_VALUE
    ;

ratioMetricExpression
    : identifier '(' aggregateExpression ')'
    ;

aggregateExpression
    : aggregate '(' ASTERISK ')'
    // | aggregate '(' (setQuantifier? expression (',' expression)*)? ')'
    ;

aggregate
    : COUNT
    | MIN
    | MAX
    | SUM
    ;

setQuantifier
    : DISTINCT
    | ALL
    ;

selectItem
    : expression (AS? identifier)?  #selectSingle
    | qualifiedName '.' ASTERISK    #selectAll
    | ASTERISK                      #selectAll
    ;

exportClause
    : (
        INTO OUTFILE filename=STRING
        (
          fieldsFormat=(FIELDS | COLUMNS)
          delimiterClause /*escapeClause?*/
        )?
        (
          LINES delimiterClause
        )?
      )
    ;


delimiterClause
    : TERMINATED BY delimiter=STRING
    // | OPTIONALLY? ENCLOSED BY enclosion=STRING
    ;

// TODO: add support for escaping
escapeClause
    : ESCAPED BY escaping=STRING
    ;

relation
    : aliasedRelation                             #relationDefault
    | left=relation
      ( CROSS JOIN right=aliasedRelation
      | joinType JOIN rightRelation=relation joinCriteria
      | NATURAL joinType JOIN right=aliasedRelation
      )                                           #joinRelation

    ;

joinType
    : INNER?
    | LEFT OUTER?
    | RIGHT OUTER?
    | FULL OUTER?
    ;

joinCriteria
    : ON booleanExpression
    | USING '(' identifier (',' identifier)* ')'
    ;

aliasedRelation
    : relationPrimary (AS? identifier ('(' columnAliases ')')?)?
    ;

columnAliases
    : identifier (',' identifier)*
    ;

relationPrimary
    : qualifiedName                                                   #tableName
    | '(' query ')'                                                   #subqueryRelation
    | '(' relation ')'                                                #parenthesizedRelation
    ;

expression
    : booleanExpression
    ;

booleanExpression
    : predicated                                                   #booleanDefault
    | NOT booleanExpression                                        #logicalNot
    | left=booleanExpression operator=AND right=booleanExpression  #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression   #logicalBinary
    ;

// workaround for:
//  https://github.com/antlr/antlr4/issues/780
//  https://github.com/antlr/antlr4/issues/781
predicated
    : valueExpression predicate[$valueExpression.ctx]?
    ;

predicate[ParserRuleContext value]
    : comparisonOperator right=valueExpression                            #comparison
    | comparisonOperator comparisonQuantifier '(' query ')'               #quantifiedComparison
    | NOT? BETWEEN lower=valueExpression AND upper=valueExpression        #between
    | NOT? IN '(' expression (',' expression)* ')'                        #inList
    | NOT? IN '(' query ')'                                               #inSubquery
    | NOT? LIKE pattern=valueExpression (ESCAPE escape=valueExpression)?  #like
    | IS NOT? NULL                                                        #nullPredicate
    | IS NOT? DISTINCT FROM right=valueExpression                         #distinctFrom
    ;

valueExpression
    : primaryExpression                                                                 #valueExpressionDefault
    | operator=(MINUS | PLUS) valueExpression                                           #arithmeticUnary
    | left=valueExpression operator=(ASTERISK | SLASH | PERCENT) right=valueExpression  #arithmeticBinary
    | left=valueExpression operator=(PLUS | MINUS) right=valueExpression                #arithmeticBinary
    | left=valueExpression CONCAT right=valueExpression                                 #concatenation
    ;

primaryExpression
    : NULL                                                                                #nullLiteral
    | identifier string                                                                   #typeConstructor
    | DOUBLE_PRECISION string                                                             #typeConstructor
    | number                                                                              #numericLiteral
    | booleanValue                                                                        #booleanLiteral
    | string                                                                              #stringLiteral
    | BINARY_LITERAL                                                                      #binaryLiteral
    | qualifiedName '(' ASTERISK ')' filter?                                              #functionCall
    | qualifiedName '(' (setQuantifier? expression (',' expression)*)? ')' filter?        #functionCall
    | '(' query ')'                                                                       #subqueryExpression
    // This is an extension to ANSI SQL, which considers EXISTS to be a <boolean expression>
    | EXISTS '(' query ')'                                                                #exists
    | identifier                                                                          #columnReference
    | base=primaryExpression '.' fieldName=identifier                                     #dereference
    | '(' expression ')'                                                                  #parenthesizedExpression
    ;

string
    : STRING                                #basicStringLiteral
    | UNICODE_STRING (UESCAPE STRING)?      #unicodeStringLiteral
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

comparisonQuantifier
    : ALL | SOME | ANY
    ;

booleanValue
    : TRUE | FALSE
    ;

type
    : type ARRAY
    | ARRAY '<' type '>'
    | MAP '<' type ',' type '>'
    | ROW '(' identifier type (',' identifier type)* ')'
    | baseType ('(' typeParameter (',' typeParameter)* ')')?
    ;

typeParameter
    : INTEGER_VALUE | type
    ;

baseType
    : TIME_WITH_TIME_ZONE
    | TIMESTAMP_WITH_TIME_ZONE
    | DOUBLE_PRECISION
    | identifier
    ;

whenClause
    : WHEN condition=expression THEN result=expression
    ;

filter
    : FILTER '(' WHERE booleanExpression ')'
    ;

qualifiedName
    : identifier ('.' identifier)*
    ;

identifier
    : IDENTIFIER             #unquotedIdentifier
    | nonReserved            #unquotedIdentifier
    | BACKQUOTED_IDENTIFIER  #backQuotedIdentifier
    | DIGIT_IDENTIFIER       #digitIdentifier
    ;

number
    : DECIMAL_VALUE  #decimalLiteral
    | INTEGER_VALUE  #integerLiteral
    ;

nonReserved
    // IMPORTANT: this rule must only contain tokens. Nested rules are not supported. See SqlParser.exitNonReserved
    : ADD | ALL | ANALYZE | ANY | ARRAY | ASC | AT
    | BERNOULLI
    | CALL | CASCADE | CATALOGS | COALESCE | COLUMN | COLUMNS | COMMENT | COMMIT | COMMITTED | CURRENT
    | DATA | DATE | DAY | DESC | DISTRIBUTED
    | EXCLUDING | EXPLAIN
    | FILTER | FIRST | FOLLOWING | FORMAT | FUNCTIONS
    | GRANT | GRANTS | GRAPHVIZ
    | HOUR
    | IF | INCLUDING | INPUT | INTEGER | INTERVAL | ISOLATION
    | LAST | LATERAL | LEVEL | LIMIT | LOGICAL
    | MAP | MINUTE | MONTH
    | NFC | NFD | NFKC | NFKD | NO | NULLIF | NULLS
    | ONLY | OPTION | ORDINALITY | OUTPUT | OVER
    | PARTITION | PARTITIONS | POSITION | PRECEDING | PRIVILEGES | PROPERTIES | PUBLIC
    | RANGE | READ | RENAME | REPEATABLE | REPLACE | RESET | RESTRICT | REVOKE | ROLLBACK | ROW | ROWS
    | SCHEMA | SCHEMAS | SECOND | SESSION | SET | SETS
    | SHOW | SMALLINT | SOME | START | STATS | SUBSTRING | SYSTEM
    | TABLES | TABLESAMPLE | TEXT | TIME | TIMESTAMP | TINYINT | TO | TRY_CAST | TYPE
    | UNBOUNDED | UNCOMMITTED | USE
    | VALIDATE | VERBOSE | VIEW
    | WORK | WRITE
    | YEAR
    | ZONE
    ;

ADD: 'ADD';
ALL: 'ALL';
ALTER: 'ALTER';
ANALYZE: 'ANALYZE';
AND: 'AND';
ANTI: 'ANTI';
ANY: 'ANY';
ARRAY: 'ARRAY';
AS: 'AS';
ASC: 'ASC';
AT: 'AT';
BERNOULLI: 'BERNOULLI';
BETWEEN: 'BETWEEN';
BY: 'BY';
CALL: 'CALL';
CASCADE: 'CASCADE';
CASE: 'CASE';
CAST: 'CAST';
CATALOGS: 'CATALOGS';
COALESCE: 'COALESCE';
COLUMN: 'COLUMN';
COLUMNS: 'COLUMNS';
COMBO: 'COMBO';
COMMENT: 'COMMENT';
COMMIT: 'COMMIT';
COMMITTED: 'COMMITTED';
COMPARE: 'COMPARE';
CONSTRAINT: 'CONSTRAINT';
COUNT: 'COUNT';
CREATE: 'CREATE';
CSV: 'CSV';
CROSS: 'CROSS';
CUBE: 'CUBE';
CURRENT: 'CURRENT';
DATA: 'DATA';
DATE: 'DATE';
DAY: 'DAY';
DEALLOCATE: 'DEALLOCATE';
DELETE: 'DELETE';
DESC: 'DESC';
DESCRIBE: 'DESCRIBE';
DIFF: 'DIFF';
DISTINCT: 'DISTINCT';
DISTRIBUTED: 'DISTRIBUTED';
DROP: 'DROP';
ELSE: 'ELSE';
ENCLOSED: 'ENCLOSED';
END: 'END';
ESCAPE: 'ESCAPE';
ESCAPED: 'ESCAPED';
EXCEPT: 'EXCEPT';
EXCLUDING: 'EXCLUDING';
EXECUTE: 'EXECUTE';
EXISTS: 'EXISTS';
EXPLAIN: 'EXPLAIN';
EXTRACT: 'EXTRACT';
FALSE: 'FALSE';
FIELDS: 'FIELDS';
FILE: 'FILE';
FILTER: 'FILTER';
FIRST: 'FIRST';
FOLLOWING: 'FOLLOWING';
FOR: 'FOR';
FORMAT: 'FORMAT';
FROM: 'FROM';
FULL: 'FULL';
FUNCTIONS: 'FUNCTIONS';
GRANT: 'GRANT';
GRANTS: 'GRANTS';
GRAPHVIZ: 'GRAPHVIZ';
GROUP: 'GROUP';
GROUPING: 'GROUPING';
HAVING: 'HAVING';
HOUR: 'HOUR';
IF: 'IF';
IMPORT: 'IMPORT';
IN: 'IN';
INCLUDING: 'INCLUDING';
INNER: 'INNER';
INPUT: 'INPUT';
INSERT: 'INSERT';
INTEGER: 'INTEGER';
INTERSECT: 'INTERSECT';
INTERVAL: 'INTERVAL';
INTO: 'INTO';
IS: 'IS';
ISOLATION: 'ISOLATION';
JOIN: 'JOIN';
LAST: 'LAST';
LATERAL: 'LATERAL';
LEFT: 'LEFT';
LEVEL: 'LEVEL';
LIKE: 'LIKE';
LIMIT: 'LIMIT';
LINES: 'LINES';
LOGICAL: 'LOGICAL';
MAP: 'MAP';
MAX: 'MAX';
MIN: 'MIN';
MIN_SUPPORT: 'MIN SUPPORT';
MINUTE: 'MINUTE';
MONTH: 'MONTH';
NATURAL: 'NATURAL';
NFC : 'NFC';
NFD : 'NFD';
NFKC : 'NFKC';
NFKD : 'NFKD';
NO: 'NO';
NOT: 'NOT';
NULL: 'NULL';
NULLIF: 'NULLIF';
NULLS: 'NULLS';
ON: 'ON';
ONLY: 'ONLY';
OPTION: 'OPTION';
OPTIONALLY: 'OPTIONALLY';
OR: 'OR';
ORDER: 'ORDER';
ORDINALITY: 'ORDINALITY';
OUTER: 'OUTER';
OUTFILE: 'OUTFILE';
OUTPUT: 'OUTPUT';
OVER: 'OVER';
PARTITION: 'PARTITION';
PARTITIONS: 'PARTITIONS';
POSITION: 'POSITION';
PRECEDING: 'PRECEDING';
PREPARE: 'PREPARE';
PRIVILEGES: 'PRIVILEGES';
PROPERTIES: 'PROPERTIES';
PUBLIC: 'PUBLIC';
RANGE: 'RANGE';
RATIO: 'RATIO';
READ: 'READ';
RECURSIVE: 'RECURSIVE';
RENAME: 'RENAME';
REPEATABLE: 'REPEATABLE';
REPLACE: 'REPLACE';
RESET: 'RESET';
RESTRICT: 'RESTRICT';
REVOKE: 'REVOKE';
RIGHT: 'RIGHT';
ROLLBACK: 'ROLLBACK';
ROLLUP: 'ROLLUP';
ROW: 'ROW';
ROWS: 'ROWS';
SCHEMA: 'SCHEMA';
SCHEMAS: 'SCHEMAS';
SECOND: 'SECOND';
SELECT: 'SELECT';
SESSION: 'SESSION';
SET: 'SET';
SETS: 'SETS';
SHOW: 'SHOW';
SMALLINT: 'SMALLINT';
SOME: 'SOME';
SPLIT: 'SPLIT';
START: 'START';
STARTING: 'STARTING';
STATS: 'STATS';
SUBSTRING: 'SUBSTRING';
SUM: 'SUM';
SYSTEM: 'SYSTEM';
TABLE: 'TABLE';
TABLES: 'TABLES';
TABLESAMPLE: 'TABLESAMPLE';
TERMINATED: 'TERMINATED';
TEXT: 'TEXT';
THEN: 'THEN';
TIME: 'TIME';
TIMESTAMP: 'TIMESTAMP';
TINYINT: 'TINYINT';
TO: 'TO';
TRUE: 'TRUE';
TRY_CAST: 'TRY_CAST';
TYPE: 'TYPE';
UESCAPE: 'UESCAPE';
UNBOUNDED: 'UNBOUNDED';
UNCOMMITTED: 'UNCOMMITTED';
UNION: 'UNION';
UNNEST: 'UNNEST';
USE: 'USE';
USING: 'USING';
VALIDATE: 'VALIDATE';
VALUES: 'VALUES';
VERBOSE: 'VERBOSE';
VIEW: 'VIEW';
WHEN: 'WHEN';
WHERE: 'WHERE';
WITH: 'WITH';
WORK: 'WORK';
WRITE: 'WRITE';
YEAR: 'YEAR';
ZONE: 'ZONE';

EQ  : '=';
NEQ : '<>' | '!=';
LT  : '<';
LTE : '<=';
GT  : '>';
GTE : '>=';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';
CONCAT: '||';

STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
    | '"' ( ~'"' | '""' )* '"'
    ;

UNICODE_STRING
    : 'U&\'' ( ~'\'' | '\'\'' )* '\''
    ;

// Note: we allow any character inside the binary literal and validate
// its a correct literal when the AST is being constructed. This
// allows us to provide more meaningful error messages to the user
BINARY_LITERAL
    :  'X\'' (~'\'')* '\''
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    | DIGIT+ ('.' DIGIT*)? EXPONENT
    | '.' DIGIT+ EXPONENT
    ;

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_' | '@' | ':')*
    ;

DIGIT_IDENTIFIER
    : DIGIT (LETTER | DIGIT | '_' | '@' | ':')+
    ;

QUOTED_IDENTIFIER
    : '"' ( ~'"' | '""' )* '"'
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

TIME_WITH_TIME_ZONE
    : 'TIME' WS 'WITH' WS 'TIME' WS 'ZONE'
    ;

TIMESTAMP_WITH_TIME_ZONE
    : 'TIMESTAMP' WS 'WITH' WS 'TIME' WS 'ZONE'
    ;

DOUBLE_PRECISION
    : 'DOUBLE' WS 'PRECISION'
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;

SIMPLE_COMMENT
    : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;

