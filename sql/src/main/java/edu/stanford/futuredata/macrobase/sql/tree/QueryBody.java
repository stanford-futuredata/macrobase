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

import com.google.common.collect.Lists;
import java.util.Optional;

public abstract class QueryBody extends Relation {

    // subclasses can use this to return nothing for getSelect()
    protected static final Select SELECT_ALL = new Select(false,
        Lists.newArrayList(new SingleColumn(new StringLiteral("*"))));

    protected QueryBody(Optional<NodeLocation> location) {
        super(location);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitQueryBody(this, context);
    }

    public abstract Select getSelect();

    public abstract Optional<Expression> getWhere();

    public abstract Optional<OrderBy> getOrderBy();

    public abstract Optional<String> getLimit();

    public abstract Optional<ExportClause> getExportExpr();
}
