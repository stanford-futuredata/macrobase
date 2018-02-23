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
package edu.stanford.futuredata.macrobase.sql.tree;

import javax.annotation.Nullable;

/**
 * Portions of this copied from Facebook's presto-parser (https://github.com/prestodb/presto/tree/master/presto-parser);
 * any new AST type defined in the tree subpackage should be added to this file as follows:
 * <code>
 *     protected R visitNewAstType(NewAstType node, C context) {
 *          return visitNode(node, context);
 *     }
 * </code>
 **/
public abstract class AstVisitor<R, C> {

    public R process(Node node) {
        return process(node, null);
    }

    public R process(Node node, @Nullable C context) {
        return node.accept(this, context);
    }

    protected R visitNode(Node node, C context) {
        return null;
    }

    protected R visitExpression(Expression node, C context) {
        return visitNode(node, context);
    }

    protected R visitArithmeticBinary(ArithmeticBinaryExpression node, C context) {
        return visitExpression(node, context);
    }

    protected R visitComparisonExpression(ComparisonExpression node, C context) {
        return visitExpression(node, context);
    }

    protected R visitLiteral(Literal node, C context) {
        return visitExpression(node, context);
    }

    protected R visitDoubleLiteral(DoubleLiteral node, C context) {
        return visitLiteral(node, context);
    }

    protected R visitDecimalLiteral(DecimalLiteral node, C context) {
        return visitLiteral(node, context);
    }

    protected R visitStatement(Statement node, C context) {
        return visitNode(node, context);
    }

    protected R visitQuery(Query node, C context) {
        return visitStatement(node, context);
    }

    protected R visitGenericLiteral(GenericLiteral node, C context) {
        return visitLiteral(node, context);
    }

    protected R visitSelect(Select node, C context) {
        return visitNode(node, context);
    }

    protected R visitRelation(Relation node, C context) {
        return visitNode(node, context);
    }

    protected R visitQueryBody(QueryBody node, C context) {
        return visitRelation(node, context);
    }

    protected R visitOrderBy(OrderBy node, C context) {
        return visitNode(node, context);
    }

    protected R visitQuerySpecification(QuerySpecification node, C context) {
        return visitQueryBody(node, context);
    }

    public R visitDiffQuerySpecification(DiffQuerySpecification node, C context) {
        return visitQueryBody(node, context);
    }

    protected R visitWhenClause(WhenClause node, C context) {
        return visitExpression(node, context);
    }

    protected R visitFunctionCall(FunctionCall node, C context) {
        return visitExpression(node, context);
    }

    protected R visitStringLiteral(StringLiteral node, C context) {
        return visitLiteral(node, context);
    }

    protected R visitCharLiteral(CharLiteral node, C context) {
        return visitLiteral(node, context);
    }

    protected R visitBinaryLiteral(BinaryLiteral node, C context) {
        return visitLiteral(node, context);
    }

    protected R visitBooleanLiteral(BooleanLiteral node, C context) {
        return visitLiteral(node, context);
    }

    protected R visitInListExpression(InListExpression node, C context) {
        return visitExpression(node, context);
    }

    protected R visitIdentifier(Identifier node, C context) {
        return visitExpression(node, context);
    }

    protected R visitNullLiteral(NullLiteral node, C context) {
        return visitLiteral(node, context);
    }

    protected R visitArithmeticUnary(ArithmeticUnaryExpression node, C context) {
        return visitExpression(node, context);
    }

    protected R visitNotExpression(NotExpression node, C context) {
        return visitExpression(node, context);
    }

    protected R visitSelectItem(SelectItem node, C context) {
        return visitNode(node, context);
    }

    protected R visitSingleColumn(SingleColumn node, C context) {
        return visitSelectItem(node, context);
    }

    protected R visitAllColumns(AllColumns node, C context) {
        return visitSelectItem(node, context);
    }

    protected R visitLikePredicate(LikePredicate node, C context) {
        return visitExpression(node, context);
    }

    protected R visitIsNotNullPredicate(IsNotNullPredicate node, C context) {
        return visitExpression(node, context);
    }

    protected R visitIsNullPredicate(IsNullPredicate node, C context) {
        return visitExpression(node, context);
    }

    protected R visitLongLiteral(IntLiteral node, C context) {
        return visitLiteral(node, context);
    }

    protected R visitLogicalBinaryExpression(LogicalBinaryExpression node, C context) {
        return visitExpression(node, context);
    }

    protected R visitSubqueryExpression(SubqueryExpression node, C context) {
        return visitExpression(node, context);
    }

    protected R visitSortItem(SortItem node, C context) {
        return visitNode(node, context);
    }

    protected R visitTable(Table node, C context) {
        return visitQueryBody(node, context);
    }

    protected R visitTableSubquery(TableSubquery node, C context) {
        return visitQueryBody(node, context);
    }

    protected R visitAliasedRelation(AliasedRelation node, C context) {
        return visitRelation(node, context);
    }

    protected R visitJoin(Join node, C context) {
        return visitRelation(node, context);
    }

    protected R visitExists(ExistsPredicate node, C context) {
        return visitExpression(node, context);
    }

    protected R visitFieldReference(FieldReference node, C context) {
        return visitExpression(node, context);
    }

    protected R visitColumnDefinition(ColumnDefinition node, C context) {
        return visitNode(node, context);
    }

    protected R visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node,
        C context) {
        return visitExpression(node, context);
    }

    public R visitRatioMetricExpression(RatioMetricExpression node, C context) {
        return visitExpression(node, context);
    }

    public R visitImportCsv(ImportCsv node, C context) {
        return visitStatement(node, context);
    }
}
