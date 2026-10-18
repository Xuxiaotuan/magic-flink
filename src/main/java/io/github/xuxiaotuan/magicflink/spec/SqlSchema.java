package io.github.xuxiaotuan.magicflink.spec;

import org.apache.flink.table.types.DataType;
import java.util.List;
import java.util.Objects;

/** Physical schema from the platform registry, with no unproven PRIMARY KEY optimizer hints. */
public record SqlSchema(List<Column> columns) {
    public SqlSchema {
        columns = List.copyOf(columns);
        if (columns.isEmpty() || columns.stream().map(Column::name).distinct().count() != columns.size())
            throw new IllegalArgumentException("schema requires nonempty, unique columns");
    }
    public record Column(String name, DataType type) {
        public Column {
            if (name == null || name.isBlank()) throw new IllegalArgumentException("column name required");
            Objects.requireNonNull(type, "type");
        }
    }
}
