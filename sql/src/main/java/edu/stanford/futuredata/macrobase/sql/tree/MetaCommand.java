package edu.stanford.futuredata.macrobase.sql.tree;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class MetaCommand extends Statement {

    public enum Command {
        TUPLE,
        TRUNCATE
    }

    public enum CommandValue {
        ON,
        OFF,
        AUTO
    }

    private final Command command;
    private final CommandValue value;

    public MetaCommand(Command command, CommandValue value) {
        this(Optional.empty(), command, value);
    }

    public MetaCommand(NodeLocation location, Command command, CommandValue value) {
        this(Optional.of(location), command, value);
    }

    private MetaCommand(Optional<NodeLocation> location, Command command, CommandValue value) {
        super(location);
        this.command = requireNonNull(command, "command is null");
        this.value = requireNonNull(value, "value is null");
    }

    public Command getCommand() {
        return command;
    }

    public CommandValue getValue() {
        return value;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitMetaCommand(this, context);
    }

    @Override
    public List<Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MetaCommand o = (MetaCommand) obj;
        return Objects.equals(this.command, o.command) &&
            Objects.equals(this.value, o.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(command, value);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .add("command", command)
            .add("value", value)
            .toString();
    }
}
