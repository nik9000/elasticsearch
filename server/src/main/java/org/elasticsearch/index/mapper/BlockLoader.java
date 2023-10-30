/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Releasable;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Interface for loading data in a block shape. Instances of this class
 * must be immutable and thread safe.
 */
public interface BlockLoader {
    /**
     * How blocks should be loaded.
     */
    enum Method {
        CONSTANT,
        DOC_VALUES; // TODO add STORED_FIELDS
    }

    /**
     * How blocks should be loaded.
     */
    Method method();

    /**
     * The {@link BlockLoader.Builder} for data of this type. Called when
     * loading from a multi-segment or unsorted block.
     */
    Builder builder(BlockFactory factory, int expectedCount);

    /**
     * Build a {@link LeafReaderContext leaf} level reader for doc values.
     * This is only supported if {@link #method} returns {@link Method#DOC_VALUES}.
     */
    BlockDocValuesReader docValuesReader(LeafReaderContext context) throws IOException;

    /**
     * Build a constant block for this field with the specified size. This
     * is only supported if {@link #method} returns {@link Method#CONSTANT}.
     */
    Block constant(BlockFactory factory, int size);

    /**
     * Does this loader support loading bytes via calling {@link #ordinals}.
     */
    default boolean supportsOrdinals() {
        return false;
    }

    /**
     * Load ordinals for the provided context.
     */
    default SortedSetDocValues ordinals(LeafReaderContext context) throws IOException {
        throw new IllegalStateException("ordinals not supported");
    }

    /**
     * Load blocks with only null.
     */
    static BlockLoader constantNulls() {
        return new BlockLoader() {
            @Override
            public Method method() {
                return Method.CONSTANT;
            }

            @Override
            public Builder builder(BlockFactory factory, int expectedCount) {
                return factory.nulls(expectedCount);
            }

            @Override
            public BlockDocValuesReader docValuesReader(LeafReaderContext context) throws IOException {
                throw new UnsupportedEncodingException();
            }

            @Override
            public Block constant(BlockFactory factory, int size) {
                return factory.constantNulls(size);
            }

            @Override
            public String toString() {
                return "ConstantNull";
            }
        };
    }

    /**
     * Load blocks with only {@code value}.
     */
    static BlockLoader constantBytes(BytesRef value) {
        return new BlockLoader() {
            @Override
            public Method method() {
                return Method.CONSTANT;
            }

            @Override
            public Builder builder(BlockFactory factory, int expectedCount) {
                return factory.bytesRefs(expectedCount);
            }

            @Override
            public BlockDocValuesReader docValuesReader(LeafReaderContext context) throws IOException {
                throw new UnsupportedEncodingException();
            }

            @Override
            public Block constant(BlockFactory factory, int size) {
                return factory.constantBytes(value, size);
            }

            @Override
            public String toString() {
                return "ConstantBytes[" + value + "]";
            }
        };
    }

    /**
     * A list of documents to load.
     */
    interface Docs {
        int count();

        int get(int i);
    }

    /**
     * Builds block "builders" for loading data into blocks for the compute engine.
     * It's important for performance that this only have one implementation in
     * production code. That implementation sits in the "compute" project. The is
     * also a test implementation, but there may be no more other implementations.
     */
    interface BlockFactory {
        /**
         * Build a builder to load booleans as loaded from doc values. Doc values
         * load booleans deduplicated and in sorted order.
         */
        BooleanBuilder booleansFromDocValues(int expectedCount);

        /**
         * Build a builder to load booleans without any loading constraints.
         */
        BooleanBuilder booleans(int expectedCount);

        /**
         * Build a builder to load {@link BytesRef}s as loaded from doc values.
         * Doc values load {@linkplain BytesRef}s deduplicated and in sorted order.
         */
        BytesRefBuilder bytesRefsFromDocValues(int expectedCount);

        /**
         * Build a builder to load {@link BytesRef}s without any loading constraints.
         */
        BytesRefBuilder bytesRefs(int expectedCount);

        /**
         * Build a builder to load doubles as loaded from doc values.
         * Doc values load doubles deduplicated and in sorted order.
         */
        DoubleBuilder doublesFromDocValues(int expectedCount);

        /**
         * Build a builder to load doubles without any loading constraints.
         */
        DoubleBuilder doubles(int expectedCount);

        /**
         * Build a builder to load ints as loaded from doc values.
         * Doc values load ints deduplicated and in sorted order.
         */
        IntBuilder intsFromDocValues(int expectedCount);

        /**
         * Build a builder to load ints without any loading constraints.
         */
        IntBuilder ints(int expectedCount);

        /**
         * Build a builder to load longs as loaded from doc values.
         * Doc values load longs deduplicated and in sorted order.
         */
        LongBuilder longsFromDocValues(int expectedCount);

        /**
         * Build a builder to load longs without any loading constraints.
         */
        LongBuilder longs(int expectedCount);

        /**
         * Build a builder to load only {@code null}s.
         */
        Builder nulls(int expectedCount);

        /**
         * Build a block that contains only {@code null}.
         */
        Block constantNulls(int size);

        /**
         * Build a block that contains {@code value} repeated
         * {@code size} times.
         */
        Block constantBytes(BytesRef value, int size);

        /**
         * Build a reader for reading keyword ordinals.
         */
        SingletonOrdinalsBuilder singletonOrdinalsBuilder(SortedDocValues ordinals, int count);

        // TODO support non-singleton ords
    }

    /**
     * Marker interface for block results. The compute engine has a fleshed
     * out implementation.
     */
    interface Block {}

    /**
     * A builder for typed values. For each document you may either call
     * {@link #appendNull}, {@code append<Type>}, or
     * {@link #beginPositionEntry} followed by two or more {@code append<Type>}
     * calls, and then {@link #endPositionEntry}.
     */
    interface Builder extends Releasable {
        /**
         * Build the actual block.
         */
        Block build();

        /**
         * Insert a null value.
         */
        Builder appendNull();

        /**
         * Start a multivalued field.
         */
        Builder beginPositionEntry();

        /**
         * End a multivalued field.
         */
        Builder endPositionEntry();
    }

    interface BooleanBuilder extends Builder {
        /**
         * Appends a boolean to the current entry.
         */
        BooleanBuilder appendBoolean(boolean value);
    }

    interface BytesRefBuilder extends Builder {
        /**
         * Appends a BytesRef to the current entry.
         */
        BytesRefBuilder appendBytesRef(BytesRef value);
    }

    interface DoubleBuilder extends Builder {
        /**
         * Appends a double to the current entry.
         */
        DoubleBuilder appendDouble(double value);
    }

    interface IntBuilder extends Builder {
        /**
         * Appends an int to the current entry.
         */
        IntBuilder appendInt(int value);
    }

    interface LongBuilder extends Builder {
        /**
         * Appends a long to the current entry.
         */
        LongBuilder appendLong(long value);
    }

    interface SingletonOrdinalsBuilder extends Builder {
        /**
         * Appends an ordinal to the builder.
         */
        SingletonOrdinalsBuilder appendOrd(int value);
    }
}
