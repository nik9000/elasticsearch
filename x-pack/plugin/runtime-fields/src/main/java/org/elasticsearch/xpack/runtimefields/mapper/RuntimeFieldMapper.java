/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.util.LocaleUtils;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.grok.FloatConsumer;
import org.elasticsearch.grok.Grok;
import org.elasticsearch.grok.GrokCaptureConfig;
import org.elasticsearch.grok.GrokCaptureConfig.NativeExtracterMap;
import org.elasticsearch.grok.GrokCaptureExtracter;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DocValueFetcher;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.mapper.ParametrizedFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

public final class RuntimeFieldMapper extends ParametrizedFieldMapper {

    public static final String CONTENT_TYPE = "runtime";

    public static final TypeParser PARSER = new TypeParser((name, parserContext) -> new Builder(name, new ScriptCompiler() {
        @Override
        public <FactoryType> FactoryType compile(Script script, ScriptContext<FactoryType> context) {
            return parserContext.scriptService().compile(script, context);
        }
    }));

    private static final Logger logger = LogManager.getLogger(RuntimeFieldMapper.class);

    private final DataSource dataSource;
    private final ScriptCompiler scriptCompiler;

    protected RuntimeFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        DataSource dataSource,
        ScriptCompiler scriptCompiler
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.scriptCompiler = scriptCompiler;
        this.dataSource = dataSource;
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new RuntimeFieldMapper.Builder(simpleName(), scriptCompiler).init(this);
    }

    @Override
    protected void parseCreateField(ParseContext context) {
        // there is no lucene field
    }

    @Override
    public ValueFetcher valueFetcher(MapperService mapperService, SearchLookup lookup, String format) {
        // NOCOMMIT grok should somehow opt-out of this - we want to fetch the subfields but there isn't really a "root"
        return new DocValueFetcher(fieldType().docValueFormat(format, null), lookup.doc().getForField(fieldType()));
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public Iterator<Mapper> iterator() {
        return dataSource.subFields().iterator();
    }

    public static class Builder extends ParametrizedFieldMapper.Builder {

        static final Map<String, BiFunction<Builder, BuilderContext, AbstractScriptMappedFieldType<?>>> FIELD_TYPE_RESOLVER = Map.of(
            BooleanFieldMapper.CONTENT_TYPE,
            (builder, context) -> {
                builder.formatAndLocaleNotSupported();
                BooleanFieldScript.Factory factory = builder.scriptCompiler.compile(builder.script.getValue(), BooleanFieldScript.CONTEXT);
                return new BooleanScriptMappedFieldType(
                    builder.buildFullName(context),
                    builder.script.getValue(),
                    factory,
                    builder.meta.getValue()
                );
            },
            DateFieldMapper.CONTENT_TYPE,
            (builder, context) -> {
                DateFieldScript.Factory factory = builder.scriptCompiler.compile(builder.script.getValue(), DateFieldScript.CONTEXT);
                String format = builder.format.getValue();
                if (format == null) {
                    format = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.pattern();
                }
                Locale locale = builder.locale.getValue();
                if (locale == null) {
                    locale = Locale.ROOT;
                }
                DateFormatter dateTimeFormatter = DateFormatter.forPattern(format).withLocale(locale);
                return new DateScriptMappedFieldType(
                    builder.buildFullName(context),
                    builder.script.getValue(),
                    factory,
                    dateTimeFormatter,
                    builder.meta.getValue()
                );
            },
            NumberType.DOUBLE.typeName(),
            (builder, context) -> {
                builder.formatAndLocaleNotSupported();
                DoubleFieldScript.Factory factory = builder.scriptCompiler.compile(builder.script.getValue(), DoubleFieldScript.CONTEXT);
                return new DoubleScriptMappedFieldType(
                    builder.buildFullName(context),
                    builder.script.getValue(),
                    factory,
                    builder.meta.getValue()
                );
            },
            IpFieldMapper.CONTENT_TYPE,
            (builder, context) -> {
                builder.formatAndLocaleNotSupported();
                IpFieldScript.Factory factory = builder.scriptCompiler.compile(builder.script.getValue(), IpFieldScript.CONTEXT);
                return new IpScriptMappedFieldType(
                    builder.buildFullName(context),
                    builder.script.getValue(),
                    factory,
                    builder.meta.getValue()
                );
            },
            KeywordFieldMapper.CONTENT_TYPE,
            (builder, context) -> {
                builder.formatAndLocaleNotSupported();
                StringFieldScript.Factory factory = builder.scriptCompiler.compile(builder.script.getValue(), StringFieldScript.CONTEXT);
                return new KeywordScriptMappedFieldType(
                    builder.buildFullName(context),
                    builder.script.getValue(),
                    factory,
                    builder.meta.getValue()
                );
            },
            NumberType.LONG.typeName(),
            (builder, context) -> {
                builder.formatAndLocaleNotSupported();
                LongFieldScript.Factory factory = builder.scriptCompiler.compile(builder.script.getValue(), LongFieldScript.CONTEXT);
                return new LongScriptMappedFieldType(
                    builder.buildFullName(context),
                    builder.script.getValue(),
                    factory,
                    builder.meta.getValue()
                );
            }
        );

        private static RuntimeFieldMapper toType(FieldMapper in) {
            return (RuntimeFieldMapper) in;
        }

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();
        private final Parameter<String> runtimeType = Parameter.stringParam(
            "runtime_type",
            true,
            mapper -> toType(mapper).dataSource.runtimeType(),
            null
        ).acceptsNull();
        private final Parameter<Script> script = new Parameter<>(
            "script",
            true,
            () -> null,
            Builder::parseScript,
            mapper -> toType(mapper).dataSource.script()
        ).acceptsNull();
        private final Parameter<GrokConfig> grok = new Parameter<>(
            "grok",
            true,
            () -> null,
            GrokConfig::parse,
            mapper -> toType(mapper).dataSource.grok()
        ).acceptsNull();
        private final Parameter<String> format = Parameter.stringParam("format", true, mapper -> toType(mapper).dataSource.format(), null)
            .setSerializer((b, n, v) -> {
                if (v != null && false == v.equals(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.pattern())) {
                    b.field(n, v);
                }
            }, Object::toString)
            .acceptsNull();
        private final Parameter<Locale> locale = new Parameter<>(
            "locale",
            true,
            () -> null,
            (n, c, o) -> o == null ? null : LocaleUtils.parse(o.toString()),
            mapper -> toType(mapper).dataSource.formatLocale()
        ).setSerializer((b, n, v) -> {
            if (v != null && false == v.equals(Locale.ROOT)) {
                b.field(n, v.toString());
            }
        }, Object::toString).acceptsNull();

        private final ScriptCompiler scriptCompiler;

        protected Builder(String name, ScriptCompiler scriptCompiler) {
            super(name);
            this.scriptCompiler = scriptCompiler;
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(meta, runtimeType, script, grok, format, locale);
        }

        @Override
        public RuntimeFieldMapper build(BuilderContext context) {
            if (grok.getValue() != null) {
                if (script.getValue() != null) {
                    throw new IllegalArgumentException("only [grok] or [script] can be specified");
                }
                return buildGrok(context);
            }
            if (script != null) {
                return buildScript(context);
            }
            throw new IllegalArgumentException("one of [grok] or [script] must be specified");
        }

        private RuntimeFieldMapper buildGrok(BuilderContext context) {
            if (format.getValue() != null) {
                throw new IllegalArgumentException(
                    "format can not be specified for [" + CONTENT_TYPE + "] field [" + name + "] with [grok]"
                );
            }
            if (locale.getValue() != null) {
                throw new IllegalArgumentException(
                    "locale can not be specified for [" + CONTENT_TYPE + "] field [" + name + "] with [grok]"
                );
            }
            GrokConfig config = this.grok.getValue();
            // NOCOMMIT watchdog
            // Force any warning to be emitted when compiling. This behavior shamelessly stolen from ingest processor.
            new Grok(Grok.BUILTIN_PATTERNS, config.pattern, logger::warn).match("__no_match__");
            Grok grok = new Grok(Grok.BUILTIN_PATTERNS, config.pattern, logger::debug);

            String fullName = buildFullName(context);
            List<Mapper> subFields = new ArrayList<>(grok.captureConfig().size());
            for (GrokCaptureConfig captureConfig : grok.captureConfig()) {
                // NOCOMMIT we shouldn't need a Script here.
                Script script = new Script(ScriptType.INLINE, "grok_hack", "name", singletonMap("pattern", config.pattern));
                // NOCOMMIT pick the right type based on the grok metadata
                String leafName = fullName + "." + captureConfig.name(); // NOCMMIT this seems hacky
                MappedFieldType type = captureConfig.nativeExtracter(new NativeExtracterMap<MappedFieldType>() {
                    @Override
                    public MappedFieldType forString(Function<Consumer<String>, GrokCaptureExtracter> buildExtracter) {
                        return new KeywordScriptMappedFieldType(
                            leafName,
                            script,
                            StringGrokValues.factory(config.field, grok, buildExtracter),
                            emptyMap()
                        );
                    }

                    @Override
                    public MappedFieldType forInt(Function<IntConsumer, GrokCaptureExtracter> buildExtracter) {
                        return new LongScriptMappedFieldType(
                            leafName,
                            script,
                            LongGrokValues.factory(config.field, grok, emit -> buildExtracter.apply(i -> emit.accept(i))),
                            emptyMap()
                        );
                    }

                    @Override
                    public MappedFieldType forLong(Function<LongConsumer, GrokCaptureExtracter> buildExtracter) {
                        return new LongScriptMappedFieldType(
                            leafName,
                            script,
                            LongGrokValues.factory(config.field, grok, buildExtracter),
                            emptyMap()
                        );
                    }

                    @Override
                    public MappedFieldType forFloat(Function<FloatConsumer, GrokCaptureExtracter> buildExtracter) {
                        return new DoubleScriptMappedFieldType(
                            leafName,
                            script,
                            DoubleGrokValues.factory(config.field, grok, emit -> buildExtracter.apply(f -> emit.accept(f))),
                            emptyMap()
                        );
                    }

                    @Override
                    public MappedFieldType forDouble(Function<DoubleConsumer, GrokCaptureExtracter> buildExtracter) {
                        return new DoubleScriptMappedFieldType(
                            leafName,
                            script,
                            DoubleGrokValues.factory(config.field, grok, buildExtracter),
                            emptyMap()
                        );
                    }

                    @Override
                    public MappedFieldType forBoolean(Function<Consumer<Boolean>, GrokCaptureExtracter> buildExtracter) {
                        return new BooleanScriptMappedFieldType(
                            leafName,
                            script,
                            BooleanGrokValues.factory(config.field, grok, buildExtracter),
                            emptyMap()
                        );
                    }
                });
                subFields.add(new RuntimeFieldMapper(captureConfig.name(), type, MultiFields.empty(), CopyTo.empty(), new DataSource() {
                    @Override
                    public Script script() {
                        return null;
                    }

                    @Override
                    public String runtimeType() {
                        return null;
                    }

                    @Override
                    public String format() {
                        return null;
                    }

                    @Override
                    public Locale formatLocale() {
                        return null;
                    }

                    @Override
                    public GrokConfig grok() {
                        return null;
                    }

                    @Override
                    public List<Mapper> subFields() {
                        return emptyList();
                    }
                }, scriptCompiler));
            }
            return new RuntimeFieldMapper(
                name,
                new GrokRootMappedFieldType(fullName, meta.getValue()),
                MultiFields.empty(),
                CopyTo.empty(),
                new DataSource() {
                    @Override
                    public Script script() {
                        return null;
                    }

                    @Override
                    public String runtimeType() {
                        return null;
                    }

                    @Override
                    public String format() {
                        return null;
                    }

                    @Override
                    public Locale formatLocale() {
                        return null;
                    }

                    @Override
                    public GrokConfig grok() {
                        return config;
                    }

                    @Override
                    public List<Mapper> subFields() {
                        return subFields;
                    }
                },
                scriptCompiler
            );
        }

        private RuntimeFieldMapper buildScript(BuilderContext context) {
            BiFunction<Builder, BuilderContext, AbstractScriptMappedFieldType<?>> fieldTypeResolver = Builder.FIELD_TYPE_RESOLVER.get(
                runtimeType.getValue()
            );
            if (fieldTypeResolver == null) {
                throw new IllegalArgumentException(
                    "runtime_type [" + runtimeType.getValue() + "] not supported for " + CONTENT_TYPE + " field [" + name + "]"
                );
            }
            MultiFields multiFields = multiFieldsBuilder.build(this, context);
            if (multiFields.iterator().hasNext()) {
                throw new IllegalArgumentException(CONTENT_TYPE + " field [" + name + "] does not support [fields]");
            }
            CopyTo copyTo = this.copyTo.build();
            if (copyTo.copyToFields().isEmpty() == false) {
                throw new IllegalArgumentException(CONTENT_TYPE + " field [" + name + "] does not support [copy_to]");
            }
            String runtimeType = this.runtimeType.getValue();
            if (runtimeType == null) {
                throw new IllegalArgumentException("runtime_type must be specified for " + CONTENT_TYPE + " field [" + name + "]");
            }
            Script script = this.script.getValue();
            AbstractScriptMappedFieldType<?> fieldType = fieldTypeResolver.apply(this, context);
            return new RuntimeFieldMapper(name, fieldType, MultiFields.empty(), CopyTo.empty(), new DataSource() {
                @Override
                public Script script() {
                    return script;
                }

                @Override
                public String runtimeType() {
                    return runtimeType;
                }

                @Override
                public String format() {
                    return fieldType.format();
                }

                @Override
                public Locale formatLocale() {
                    return fieldType.formatLocale();
                }

                @Override
                public GrokConfig grok() {
                    return null;
                }

                @Override
                public List<Mapper> subFields() {
                    return emptyList();
                }
            }, scriptCompiler);
        }

        static Script parseScript(String name, Mapper.TypeParser.ParserContext parserContext, Object scriptObject) {
            Script script = Script.parse(scriptObject);
            if (script.getType() == ScriptType.STORED) {
                throw new IllegalArgumentException("stored scripts are not supported for " + CONTENT_TYPE + " field [" + name + "]");
            }
            return script;
        }

        private void formatAndLocaleNotSupported() {
            if (format.getValue() != null) {
                throw new IllegalArgumentException(
                    "format can not be specified for ["
                        + CONTENT_TYPE
                        + "] field ["
                        + name
                        + "] of "
                        + runtimeType.name
                        + " ["
                        + runtimeType.getValue()
                        + "]"
                );
            }
            if (locale.getValue() != null) {
                throw new IllegalArgumentException(
                    "locale can not be specified for ["
                        + CONTENT_TYPE
                        + "] field ["
                        + name
                        + "] of "
                        + runtimeType.name
                        + " ["
                        + runtimeType.getValue()
                        + "]"
                );
            }
        }
    }

    @FunctionalInterface
    private interface ScriptCompiler {
        <FactoryType> FactoryType compile(Script script, ScriptContext<FactoryType> context);
    }

    private static class GrokConfig implements ToXContent {
        static GrokConfig parse(String name, Mapper.TypeParser.ParserContext parserContext, Object object) {
            if (false == object instanceof Map) {
                throw new IllegalArgumentException("[grok] must be an object");
            }
            Map<?, ?> m = (Map<?, ?>) object;
            Object field = m.get("field");
            if (field == null) {
                throw new IllegalArgumentException("[grok] requires a [field]");
            }
            Object pattern = m.get("pattern");
            if (pattern == null) {
                throw new IllegalArgumentException("[grok] requires a [pattern]");
            }
            return new GrokConfig(field.toString(), pattern.toString());
        }

        private final String field;
        private final String pattern;

        GrokConfig(String field, String pattern) {
            this.field = field;
            this.pattern = pattern;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field("field", field).field("pattern", pattern).endObject();
        }
    }

    private interface DataSource {
        Script script();

        String runtimeType();

        String format();

        Locale formatLocale();

        GrokConfig grok();

        List<Mapper> subFields();
    }
}
