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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class Query extends Statement {

    private final QueryBody queryBody;

    public Query(QueryBody queryBody) {
        this(Optional.empty(), queryBody);
    }

    public Query(NodeLocation location, QueryBody queryBody) {
        this(Optional.of(location), queryBody);
    }

    private Query(Optional<NodeLocation> location, QueryBody queryBody) {
        super(location);
        requireNonNull(queryBody, "queryBody is null");

        this.queryBody = queryBody;
    }

    public QueryBody getQueryBody() {
        return queryBody;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitQuery(this, context);
    }

    @Override
    public List<Node> getChildren() {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.add(queryBody);
        return nodes.build();
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .add("queryBody", queryBody)
            .omitNullValues()
            .toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Query o = (Query) obj;
        return Objects.equals(queryBody, o.queryBody);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryBody);
    }
}
