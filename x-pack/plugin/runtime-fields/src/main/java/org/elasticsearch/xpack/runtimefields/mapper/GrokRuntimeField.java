/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.grok.FloatConsumer;
import org.elasticsearch.grok.Grok;
import org.elasticsearch.grok.GrokCaptureConfig;
import org.elasticsearch.grok.GrokCaptureExtracter;
import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.RuntimeField;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

import static java.util.stream.Collectors.toList;

public final class GrokRuntimeField implements RuntimeField {
    private static final Logger logger = LogManager.getLogger(GrokRuntimeField.class);

    public static final RuntimeField.Parser PARSER = (name, node, context) -> new GrokRuntimeField(
        name,
        getPatterns(name, node),
        getSourceField(name, node)
    );

    private final String name;
    private final List<String> patterns;
    private final String sourceField;
    private final List<MappedFieldType> fields;

    private GrokRuntimeField(String name, List<String> patterns, String sourceField) {
        this.name = name;
        this.patterns = patterns;
        this.sourceField = sourceField;
        String combinedPatterns = Grok.combinePatterns(patterns, null);
        // TODO is it safe to do this with the warnings? Who'd see the warnings?
        // Joni warnings are only emitted on an attempt to match, and the warning emitted for every call to match which is too verbose
        // so here we emit a warning (if there is one) to the logfile at warn level on construction / processor creation.
        new Grok(Grok.BUILTIN_PATTERNS, combinedPatterns, MatcherWatchdog.noop(), logger::warn).match("___nomatch___");
        // TODO noop is almost certainly not an ok watchdog
        Grok grok = new Grok(Grok.BUILTIN_PATTERNS, combinedPatterns, MatcherWatchdog.noop(), logger::debug);
        this.fields = grok.captureConfig().stream().map(config -> createSubField(name, grok, sourceField, config)).collect(toList());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject(name()).field("type", "grok").field("patterns", patterns).field("field", sourceField).endObject();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public List<MappedFieldType> fields() {
        return fields;
    }

    private static List<String> getPatterns(String name, Map<String, Object> node) {
        Object patternsObject = node.remove("patterns");
        if (patternsObject == null) {
            throw new MapperParsingException("[patterns] is required for field [" + name + "]");
        }
        if (patternsObject instanceof List) {
            List<?> p = (List<?>) patternsObject;
            return p.stream().map(pattern -> {
                if (pattern instanceof String) {
                    return (String) pattern;
                }
                throw new MapperParsingException("[patterns] must be a String or list of string for field [" + name + "]");
            }).collect(toList());
        }
        if (patternsObject instanceof String) {
            return List.of((String) patternsObject);
        } else {
            throw new MapperParsingException("[patterns] must be a String or list of string for field [" + name + "]");
        }
    }

    private static String getSourceField(String name, Map<String, Object> node) {
        Object fieldObject = node.remove("field");
        if (fieldObject == null) {
            throw new MapperParsingException("[field] is required for field [" + name + "]");
        }
        if (fieldObject instanceof String) {
            return (String) fieldObject;
        }
        throw new MapperParsingException("[field] must be a string for field [" + name + "]");
    }

    /**
     * Build a sub-field for one of the named extractions.
     * <p>
     * This all centers around {@link GrokCaptureConfig#nativeExtracter} which
     * we use to build an appropriate {@link AbstractScriptFieldType} subclass
     * based on the type configured in the pattern. So a pattern like
     * {@code %{double}} will build a {@code DoubleScriptFieldType}.
     * <p>
     * Most of the rest of the method is about adapting the guts of
     * {@linkplain AbstractScriptFieldType} which wants a script into running
     * grok and making sure grok syncs its results to the methods like
     * {@link DoubleFieldScript#emit}.
     */
    private static MappedFieldType createSubField(String mainFieldName, Grok grok, String sourceField, GrokCaptureConfig config) {
        String field = mainFieldName + "." + config.name();
        // Dummy script for equality checks in queries. Really just delegates to Grok's equals and hashCode. Which it doesn't impl.
        Script dummyScript = new Script(ScriptType.INLINE, "grok_hack", "grok_hack", Map.of(), Map.of("grok", grok));
        CheckedBiConsumer<XContentBuilder, Boolean, IOException> toXContent = (builder, includeDefaults) -> {
            throw new UnsupportedOperationException();
        };

        Function<SearchLookup, IndexFieldData<?>> indexFieldData = lookup -> {
            MappedFieldType ft = lookup.doc().fieldType(sourceField);
            if (ft == null) {
                throw new IllegalArgumentException("Can't find source [" + sourceField + "] for grok field [" + mainFieldName + "] ");
            }
            return lookup.doc().getForField(ft);
        };
        TriConsumer<SortedBinaryDocValues, GrokCaptureExtracter, LeafSearchLookup> run = (values, extracter, leafSearchLookup) -> {
            try {
                if (false == values.advanceExact(leafSearchLookup.source().docId())) {
                    return;
                }
                for (int i = 0; i < values.docValueCount(); i++) {
                    BytesRef ref = values.nextValue();
                    // Deep copy to fix a bug where grok doesn't respect the offset.....
                    ref = BytesRef.deepCopyOf(ref);
                    // TODO we won't match anything if the bytes ain't utf-8. ok?
                    grok.match(ref.bytes, ref.offset, ref.length, extracter);
                    // TODO what should we do if we don't match?
                }
            } catch (IOException e) {
                throw new RuntimeException(e); // TODO runtime fields should be allowed to throw IOException
            }
        };

        return config.nativeExtracter(new GrokCaptureConfig.NativeExtracterMap<MappedFieldType>() {
            @Override
            public MappedFieldType forString(Function<Consumer<String>, GrokCaptureExtracter> buildExtracter) {
                StringFieldScript.Factory factory = (fieldName, params, lookup) -> {
                    IndexFieldData<?> ifd = indexFieldData.apply(lookup);
                    return ctx -> {
                        SortedBinaryDocValues values = ifd.load(ctx).getBytesValues();
                        return new StringFieldScript(fieldName, params, lookup, ctx) {
                            GrokCaptureExtracter extracter = buildExtracter.apply(this::emit);

                            @Override
                            public void execute() {
                                run.apply(values, extracter, leafSearchLookup);
                            }
                        };
                    };
                };
                return new KeywordScriptFieldType(field, factory, dummyScript, Map.of(), toXContent);
            }

            @Override
            public MappedFieldType forLong(Function<LongConsumer, GrokCaptureExtracter> buildExtracter) {
                LongFieldScript.Factory factory = (fieldName, params, lookup) -> {
                    IndexFieldData<?> ifd = indexFieldData.apply(lookup);
                    return ctx -> {
                        SortedBinaryDocValues values = ifd.load(ctx).getBytesValues();
                        return new LongFieldScript(fieldName, params, lookup, ctx) {
                            GrokCaptureExtracter extracter = buildExtracter.apply(this::emit);

                            @Override
                            public void execute() {
                                run.apply(values, extracter, leafSearchLookup);
                            }
                        };
                    };
                };
                return new LongScriptFieldType(field, factory, dummyScript, Map.of(), toXContent);
            }

            @Override
            public MappedFieldType forDouble(Function<DoubleConsumer, GrokCaptureExtracter> buildExtracter) {
                DoubleFieldScript.Factory factory = (fieldName, params, lookup) -> {
                    IndexFieldData<?> ifd = indexFieldData.apply(lookup);
                    return ctx -> {
                        SortedBinaryDocValues values = ifd.load(ctx).getBytesValues();
                        return new DoubleFieldScript(fieldName, params, lookup, ctx) {
                            GrokCaptureExtracter extracter = buildExtracter.apply(this::emit);

                            @Override
                            public void execute() {
                                run.apply(values, extracter, leafSearchLookup);
                            }
                        };
                    };
                };
                return new DoubleScriptFieldType(field, factory, dummyScript, Map.of(), toXContent);
            }

            @Override
            public MappedFieldType forBoolean(Function<Consumer<Boolean>, GrokCaptureExtracter> buildExtracter) {
                BooleanFieldScript.Factory factory = (fieldName, params, lookup) -> {
                    IndexFieldData<?> ifd = indexFieldData.apply(lookup);
                    return ctx -> {
                        SortedBinaryDocValues values = ifd.load(ctx).getBytesValues();
                        return new BooleanFieldScript(fieldName, params, lookup, ctx) {
                            GrokCaptureExtracter extracter = buildExtracter.apply(this::emit);

                            @Override
                            public void execute() {
                                run.apply(values, extracter, leafSearchLookup);
                            }
                        };
                    };
                };
                return new BooleanScriptFieldType(field, factory, dummyScript, Map.of(), toXContent);
            }

            /*
             * Runtime fields don't support int or float but grok does. So
             * we "upgrade" the ints to longs and floats to doubles, both
             * of which runtime fields do support.
             */
            @Override
            public MappedFieldType forInt(Function<IntConsumer, GrokCaptureExtracter> buildExtracter) {
                return forLong(longConsumer -> buildExtracter.apply(i -> longConsumer.accept(i)));
            }

            @Override
            public MappedFieldType forFloat(Function<FloatConsumer, GrokCaptureExtracter> buildExtracter) {
                return forDouble(doubleConsumer -> buildExtracter.apply(f -> doubleConsumer.accept(f)));
            }
        });
    }
}
