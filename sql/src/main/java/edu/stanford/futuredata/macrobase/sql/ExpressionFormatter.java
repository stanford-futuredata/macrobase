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
 */
package edu.stanford.futuredata.macrobase.sql;

import static com.google.common.base.Preconditions.checkArgument;
import static edu.stanford.futuredata.macrobase.sql.SqlFormatter.formatSql;
import static java.lang.String.format;

import com.google.common.base.Joiner;
import edu.stanford.futuredata.macrobase.sql.tree.AllColumns;
import edu.stanford.futuredata.macrobase.sql.tree.ArithmeticBinaryExpression;
import edu.stanford.futuredata.macrobase.sql.tree.ArithmeticUnaryExpression;
import edu.stanford.futuredata.macrobase.sql.tree.AstVisitor;
import edu.stanford.futuredata.macrobase.sql.tree.BinaryLiteral;
import edu.stanford.futuredata.macrobase.sql.tree.BooleanLiteral;
import edu.stanford.futuredata.macrobase.sql.tree.CharLiteral;
import edu.stanford.futuredata.macrobase.sql.tree.ComparisonExpression;
import edu.stanford.futuredata.macrobase.sql.tree.DecimalLiteral;
import edu.stanford.futuredata.macrobase.sql.tree.DoubleLiteral;
import edu.stanford.futuredata.macrobase.sql.tree.ExistsPredicate;
import edu.stanford.futuredata.macrobase.sql.tree.Expression;
import edu.stanford.futuredata.macrobase.sql.tree.FieldReference;
import edu.stanford.futuredata.macrobase.sql.tree.FunctionCall;
import edu.stanford.futuredata.macrobase.sql.tree.GenericLiteral;
import edu.stanford.futuredata.macrobase.sql.tree.Identifier;
import edu.stanford.futuredata.macrobase.sql.tree.InListExpression;
import edu.stanford.futuredata.macrobase.sql.tree.IntLiteral;
import edu.stanford.futuredata.macrobase.sql.tree.IsNotNullPredicate;
import edu.stanford.futuredata.macrobase.sql.tree.IsNullPredicate;
import edu.stanford.futuredata.macrobase.sql.tree.LikePredicate;
import edu.stanford.futuredata.macrobase.sql.tree.LogicalBinaryExpression;
import edu.stanford.futuredata.macrobase.sql.tree.Node;
import edu.stanford.futuredata.macrobase.sql.tree.NotExpression;
import edu.stanford.futuredata.macrobase.sql.tree.NullLiteral;
import edu.stanford.futuredata.macrobase.sql.tree.OrderBy;
import edu.stanford.futuredata.macrobase.sql.tree.QualifiedName;
import edu.stanford.futuredata.macrobase.sql.tree.QuantifiedComparisonExpression;
import edu.stanford.futuredata.macrobase.sql.tree.RatioMetricExpression;
import edu.stanford.futuredata.macrobase.sql.tree.SortItem;
import edu.stanford.futuredata.macrobase.sql.tree.StringLiteral;
import edu.stanford.futuredata.macrobase.sql.tree.SubqueryExpression;
import edu.stanford.futuredata.macrobase.sql.tree.WhenClause;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.function.Function;

public final class ExpressionFormatter {

    private ExpressionFormatter() {
    }

    public static String formatExpression(Expression expression,
        Optional<List<Expression>> parameters) {
        return new Formatter(parameters).process(expression, null);
    }

    public static class Formatter
        extends AstVisitor<String, Void> {

        private final Optional<List<Expression>> parameters;

        public Formatter(Optional<List<Expression>> parameters) {
            this.parameters = parameters;
        }

        @Override
        protected String visitNode(Node node, Void context) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected String visitExpression(Expression node, Void context) {
            throw new UnsupportedOperationException(
                format("not yet implemented: %s.visit%s", getClass().getName(),
                    node.getClass().getSimpleName()));
        }

        @Override
        protected String visitBooleanLiteral(BooleanLiteral node, Void context) {
            return String.valueOf(node.getValue());
        }

        @Override
        protected String visitStringLiteral(StringLiteral node, Void context) {
            return formatStringLiteral(node.getValue());
        }

        @Override
        protected String visitCharLiteral(CharLiteral node, Void context) {
            return "CHAR " + formatStringLiteral(node.getValue());
        }

        @Override
        protected String visitBinaryLiteral(BinaryLiteral node, Void context) {
            return "X'" + node.toHexString() + "'";
        }

        @Override
        protected String visitLongLiteral(IntLiteral node, Void context) {
            return Long.toString(node.getValue());
        }

        @Override
        protected String visitDoubleLiteral(DoubleLiteral node, Void context) {
            return Double.toString(node.getValue());
        }

        @Override
        protected String visitDecimalLiteral(DecimalLiteral node, Void context) {
            return "DECIMAL '" + node.getValue() + "'";
        }

        @Override
        protected String visitGenericLiteral(GenericLiteral node, Void context) {
            return node.getType() + " " + formatStringLiteral(node.getValue());
        }

        @Override
        protected String visitNullLiteral(NullLiteral node, Void context) {
            return "null";
        }

        @Override
        protected String visitSubqueryExpression(SubqueryExpression node, Void context) {
            return "(" + formatSql(node.getQuery(), parameters) + ")";
        }

        @Override
        protected String visitExists(ExistsPredicate node, Void context) {
            return "(EXISTS " + formatSql(node.getSubquery(), parameters) + ")";
        }

        @Override
        protected String visitIdentifier(Identifier node, Void context) {
            if (!node.isDelimited()) {
                return node.getValue();
            } else {
                return '"' + node.getValue().replace("\"", "\"\"") + '"';
            }
        }

        private static String formatQualifiedName(QualifiedName name) {
            List<String> parts = new ArrayList<>();
            for (String part : name.getParts()) {
                parts.add(formatIdentifier(part));
            }
            return Joiner.on('.').join(parts);
        }

        @Override
        public String visitFieldReference(FieldReference node, Void context) {
            // add colon so this won't parse
            return ":input(" + node.getFieldIndex() + ")";
        }

        @Override
        protected String visitFunctionCall(FunctionCall node, Void context) {
            StringBuilder builder = new StringBuilder();

            String arguments = joinExpressions(node.getArguments());
            if (node.getArguments().isEmpty() && "count"
                .equalsIgnoreCase(node.getName().getSuffix())) {
                arguments = "*";
            }
            if (node.isDistinct()) {
                arguments = "DISTINCT " + arguments;
            }

            builder.append(formatQualifiedName(node.getName()))
                .append('(').append(arguments).append(')');

            if (node.getFilter().isPresent()) {
                builder.append(" FILTER ").append(visitFilter(node.getFilter().get(), context));
            }

            return builder.toString();
        }

        @Override
        protected String visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context) {
            return formatBinaryExpression(node.getType().toString(), node.getLeft(),
                node.getRight());
        }

        @Override
        protected String visitNotExpression(NotExpression node, Void context) {
            return "(NOT " + process(node.getValue(), context) + ")";
        }

        @Override
        protected String visitComparisonExpression(ComparisonExpression node, Void context) {
            return formatBinaryExpression(node.getType().getValue(), node.getLeft(),
                node.getRight());
        }

        @Override
        protected String visitIsNullPredicate(IsNullPredicate node, Void context) {
            return "(" + process(node.getValue(), context) + " IS NULL)";
        }

        @Override
        protected String visitIsNotNullPredicate(IsNotNullPredicate node, Void context) {
            return "(" + process(node.getValue(), context) + " IS NOT NULL)";
        }

        @Override
        protected String visitArithmeticUnary(ArithmeticUnaryExpression node, Void context) {
            String value = process(node.getValue(), context);

            switch (node.getSign()) {
                case MINUS:
                    // this is to avoid turning a sequence of "-" into a comment (i.e., "-- comment")
                    String separator = value.startsWith("-") ? " " : "";
                    return "-" + separator + value;
                case PLUS:
                    return "+" + value;
                default:
                    throw new UnsupportedOperationException("Unsupported sign: " + node.getSign());
            }
        }

        @Override
        protected String visitArithmeticBinary(ArithmeticBinaryExpression node, Void context) {
            return formatBinaryExpression(node.getType().getValue(), node.getLeft(),
                node.getRight());
        }

        @Override
        protected String visitLikePredicate(LikePredicate node, Void context) {
            StringBuilder builder = new StringBuilder();

            builder.append('(')
                .append(process(node.getValue(), context))
                .append(" LIKE ")
                .append(process(node.getPattern(), context));

            if (node.getEscape() != null) {
                builder.append(" ESCAPE ")
                    .append(process(node.getEscape(), context));
            }

            builder.append(')');

            return builder.toString();
        }

        @Override
        protected String visitAllColumns(AllColumns node, Void context) {
            if (node.getPrefix().isPresent()) {
                return node.getPrefix().get() + ".*";
            }

            return "*";
        }

        @Override
        protected String visitWhenClause(WhenClause node, Void context) {
            return "WHEN " + process(node.getOperand(), context) + " THEN " + process(
                node.getResult(),
                context);
        }

        @Override
        protected String visitInListExpression(InListExpression node, Void context) {
            return "(" + joinExpressions(node.getValues()) + ")";
        }

        private String visitFilter(Expression node, Void context) {
            return "(WHERE " + process(node, context) + ')';
        }

        @Override
        protected String visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node,
            Void context) {
            return new StringBuilder()
                .append("(")
                .append(process(node.getValue(), context))
                .append(' ')
                .append(node.getComparisonType().getValue())
                .append(' ')
                .append(node.getQuantifier().toString())
                .append(' ')
                .append(process(node.getSubquery(), context))
                .append(")")
                .toString();
        }

        public String visitRatioMetricExpression(RatioMetricExpression node, Void context) {
            return node.getFuncName() + "(" + node.getAggExpr() + "(*))";
        }

        private String formatBinaryExpression(String operator, Expression left, Expression right) {
            return '(' + process(left, null) + ' ' + operator + ' ' + process(right, null) + ')';
        }

        private String joinExpressions(List<Expression> expressions) {
            return Joiner.on(", ")
                .join(expressions.stream().map((e) -> process(e, null)).iterator());
        }

        private static String formatIdentifier(String s) {
            // TODO: handle escaping properly
            return '"' + s + '"';
        }
    }

    static String formatStringLiteral(String s) {
        s = s.replace("'", "''");
        if (isAsciiPrintable(s)) {
            return s;
        }

        StringBuilder builder = new StringBuilder();
        builder.append("U&'");
        PrimitiveIterator.OfInt iterator = s.codePoints().iterator();
        while (iterator.hasNext()) {
            int codePoint = iterator.nextInt();
            checkArgument(codePoint >= 0, "Invalid UTF-8 encoding in characters: %s", s);
            if (isAsciiPrintable(codePoint)) {
                char ch = (char) codePoint;
                if (ch == '\\') {
                    builder.append(ch);
                }
                builder.append(ch);
            } else if (codePoint <= 0xFFFF) {
                builder.append('\\');
                builder.append(String.format("%04X", codePoint));
            } else {
                builder.append("\\+");
                builder.append(String.format("%06X", codePoint));
            }
        }
        builder.append("'");
        return builder.toString();
    }

    static String formatOrderBy(OrderBy orderBy, Optional<List<Expression>> parameters) {
        return "ORDER BY " + formatSortItems(orderBy.getSortItems(), parameters);
    }

    private static String formatSortItems(List<SortItem> sortItems,
        Optional<List<Expression>> parameters) {
        return Joiner.on(", ").join((Iterable<?>) sortItems.stream()
            .map(sortItemFormatterFunction(parameters))
            .iterator());
    }

    private static boolean isAsciiPrintable(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (!isAsciiPrintable(s.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isAsciiPrintable(int codePoint) {
        return codePoint < 0x7F && codePoint >= 0x20;
    }

    private static Function<SortItem, String> sortItemFormatterFunction(
        Optional<List<Expression>> parameters) {
        return input -> {
            StringBuilder builder = new StringBuilder();

            builder.append(formatExpression(input.getSortKey(), parameters));

            switch (input.getOrdering()) {
                case ASCENDING:
                    builder.append(" ASC");
                    break;
                case DESCENDING:
                    builder.append(" DESC");
                    break;
                default:
                    throw new UnsupportedOperationException(
                        "unknown ordering: " + input.getOrdering());
            }

            switch (input.getNullOrdering()) {
                case FIRST:
                    builder.append(" NULLS FIRST");
                    break;
                case LAST:
                    builder.append(" NULLS LAST");
                    break;
                case UNDEFINED:
                    // no op
                    break;
                default:
                    throw new UnsupportedOperationException(
                        "unknown null ordering: " + input.getNullOrdering());
            }

            return builder.toString();
        };
    }
}
