package org.example;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Spliterator;
import java.util.function.Consumer;

public class ResultSetSpliterator implements Spliterator<ResultSet> {
    private final ResultSet resultSet;

    public ResultSetSpliterator(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    @Override
    public boolean tryAdvance(Consumer<? super ResultSet> action) {
        try {
            if (resultSet.next()) {
                action.accept(resultSet);
                return true;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    @Override
    public Spliterator<ResultSet> trySplit() {
        return null; // Not supported for simplicity
    }

    @Override
    public long estimateSize() {
        return Long.MAX_VALUE; // Unknown size
    }

    @Override
    public int characteristics() {
        return IMMUTABLE | NONNULL | ORDERED;
    }
}
