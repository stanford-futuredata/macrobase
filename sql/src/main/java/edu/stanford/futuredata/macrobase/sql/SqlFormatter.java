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
import static com.google.common.collect.Iterables.getOnlyElement;
import static edu.stanford.futuredata.macrobase.sql.ExpressionFormatter.formatExpression;
import static edu.stanford.futuredata.macrobase.sql.ExpressionFormatter.formatOrderBy;
import static java.util.stream.Collectors.joining;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import edu.stanford.futuredata.macrobase.sql.tree.AliasedRelation;
import edu.stanford.futuredata.macrobase.sql.tree.AllColumns;
import edu.stanford.futuredata.macrobase.sql.tree.AstVisitor;
import edu.stanford.futuredata.macrobase.sql.tree.Expression;
import edu.stanford.futuredata.macrobase.sql.tree.Identifier;
import edu.stanford.futuredata.macrobase.sql.tree.Join;
import edu.stanford.futuredata.macrobase.sql.tree.JoinCriteria;
import edu.stanford.futuredata.macrobase.sql.tree.JoinOn;
import edu.stanford.futuredata.macrobase.sql.tree.JoinUsing;
import edu.stanford.futuredata.macrobase.sql.tree.NaturalJoin;
import edu.stanford.futuredata.macrobase.sql.tree.Node;
import edu.stanford.futuredata.macrobase.sql.tree.OrderBy;
import edu.stanford.futuredata.macrobase.sql.tree.QualifiedName;
import edu.stanford.futuredata.macrobase.sql.tree.Query;
import edu.stanford.futuredata.macrobase.sql.tree.QuerySpecification;
import edu.stanford.futuredata.macrobase.sql.tree.Relation;
import edu.stanford.futuredata.macrobase.sql.tree.Select;
import edu.stanford.futuredata.macrobase.sql.tree.SelectItem;
import edu.stanford.futuredata.macrobase.sql.tree.SingleColumn;
import edu.stanford.futuredata.macrobase.sql.tree.Table;
import edu.stanford.futuredata.macrobase.sql.tree.TableSubquery;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

final class SqlFormatter {

    private static final String INDENT = "   ";
    private static final Pattern NAME_PATTERN = Pattern.compile("[a-z_][a-z0-9_]*");

    private SqlFormatter() {
    }

    static String formatSql(Node root, Optional<List<Expression>> parameters) {
        StringBuilder builder = new StringBuilder();
        new Formatter(builder, parameters).process(root, 0);
        return builder.toString();
    }

    private static class Formatter
        extends AstVisitor<Void, Integer> {

        private final StringBuilder builder;
        private final Optional<List<Expression>> parameters;

        Formatter(StringBuilder builder, Optional<List<Expression>> parameters) {
            this.builder = builder;
            this.parameters = parameters;
        }

        @Override
        protected Void visitNode(Node node, Integer indent) {
            throw new UnsupportedOperationException("not yet implemented: " + node);
        }

        @Override
        protected Void visitExpression(Expression node, Integer indent) {
            checkArgument(indent == 0, "visitExpression should only be called at root");
            builder.append(formatExpression(node, parameters));
            return null;
        }

        @Override
        protected Void visitQuery(Query node, Integer indent) {
            processRelation(node.getQueryBody(), indent);

            return null;
        }

        @Override
        protected Void visitQuerySpecification(QuerySpecification node, Integer indent) {
            process(node.getSelect(), indent);

            if (node.getFrom().isPresent()) {
                append(indent, "FROM");
                builder.append('\n');
                append(indent, "  ");
                process(node.getFrom().get(), indent);
            }

            builder.append('\n');

            if (node.getWhere().isPresent()) {
                append(indent, "WHERE " + formatExpression(node.getWhere().get(), parameters))
                    .append('\n');
            }

            if (node.getOrderBy().isPresent()) {
                process(node.getOrderBy().get(), indent);
            }

            if (node.getLimit().isPresent()) {
                append(indent, "LIMIT " + node.getLimit().get())
                    .append('\n');
            }
            return null;
        }

        @Override
        protected Void visitOrderBy(OrderBy node, Integer indent) {
            append(indent, formatOrderBy(node, parameters))
                .append('\n');
            return null;
        }

        @Override
        protected Void visitSelect(Select node, Integer indent) {
            append(indent, "SELECT");
            if (node.isDistinct()) {
                builder.append(" DISTINCT");
            }

            if (node.getSelectItems().size() > 1) {
                boolean first = true;
                for (SelectItem item : node.getSelectItems()) {
                    builder.append("\n")
                        .append(indentString(indent))
                        .append(first ? "  " : ", ");

                    process(item, indent);
                    first = false;
                }
            } else {
                builder.append(' ');
                process(getOnlyElement(node.getSelectItems()), indent);
            }

            builder.append('\n');

            return null;
        }

        @Override
        protected Void visitSingleColumn(SingleColumn node, Integer indent) {
            builder.append(formatExpression(node.getExpression(), parameters));
            if (node.getAlias().isPresent()) {
                builder.append(' ')
                    .append(formatExpression(node.getAlias().get(), parameters));
            }

            return null;
        }

        @Override
        protected Void visitAllColumns(AllColumns node, Integer context) {
            builder.append(node.toString());

            return null;
        }

        @Override
        protected Void visitTable(Table node, Integer indent) {
            builder.append(formatName(node.getName()));

            return null;
        }

        @Override
        protected Void visitJoin(Join node, Integer indent) {
            JoinCriteria criteria = node.getCriteria().orElse(null);
            String type = node.getType().toString();
            if (criteria instanceof NaturalJoin) {
                type = "NATURAL " + type;
            }

            if (node.getType() != Join.Type.IMPLICIT) {
                builder.append('(');
            }
            process(node.getLeft(), indent);

            builder.append('\n');
            if (node.getType() == Join.Type.IMPLICIT) {
                append(indent, ", ");
            } else {
                append(indent, type).append(" JOIN ");
            }

            process(node.getRight(), indent);

            if (node.getType() != Join.Type.CROSS && node.getType() != Join.Type.IMPLICIT) {
                if (criteria instanceof JoinUsing) {
                    JoinUsing using = (JoinUsing) criteria;
                    builder.append(" USING (")
                        .append(Joiner.on(", ").join(using.getColumns()))
                        .append(")");
                } else if (criteria instanceof JoinOn) {
                    JoinOn on = (JoinOn) criteria;
                    builder.append(" ON ")
                        .append(formatExpression(on.getExpression(), parameters));
                } else if (!(criteria instanceof NaturalJoin)) {
                    throw new UnsupportedOperationException("unknown join criteria: " + criteria);
                }
            }

            if (node.getType() != Join.Type.IMPLICIT) {
                builder.append(")");
            }

            return null;
        }

        @Override
        protected Void visitAliasedRelation(AliasedRelation node, Integer indent) {
            process(node.getRelation(), indent);

            builder.append(' ')
                .append(formatExpression(node.getAlias(), parameters));
            appendAliasColumns(builder, node.getColumnNames());

            return null;
        }

        @Override
        protected Void visitTableSubquery(TableSubquery node, Integer indent) {
            builder.append('(')
                .append('\n');

            process(node.getQuery(), indent + 1);

            append(indent, ") ");

            return null;
        }

        private static String formatName(String name) {
            if (NAME_PATTERN.matcher(name).matches()) {
                return name;
            }
            return "\"" + name.replace("\"", "\"\"") + "\"";
        }

        private static String formatName(QualifiedName name) {
            return name.getOriginalParts().stream()
                .map(Formatter::formatName)
                .collect(joining("."));
        }

        private void processRelation(Relation relation, Integer indent) {
            // TODO: handle this properly
            if (relation instanceof Table) {
                builder.append("TABLE ")
                    .append(((Table) relation).getName())
                    .append('\n');
            } else {
                process(relation, indent);
            }
        }

        private StringBuilder append(int indent, String value) {
            return builder.append(indentString(indent))
                .append(value);
        }

        private static String indentString(int indent) {
            return Strings.repeat(INDENT, indent);
        }
    }

    private static void appendAliasColumns(StringBuilder builder, List<Identifier> columns) {
        if ((columns != null) && (!columns.isEmpty())) {
            String formattedColumns = columns.stream()
                .map(name -> formatExpression(name, Optional.empty()))
                .collect(Collectors.joining(", "));

            builder.append(" (")
                .append(formattedColumns)
                .append(')');
        }
    }
}
