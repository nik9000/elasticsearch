// ANTLR GENERATED CODE: DO NOT EDIT
package org.elasticsearch.xpack.esql.parser;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue"})
public class EsqlBaseParser extends ParserConfig {
  static { RuntimeMetaData.checkVersion("4.13.1", RuntimeMetaData.VERSION); }

  protected static final DFA[] _decisionToDFA;
  protected static final PredictionContextCache _sharedContextCache =
    new PredictionContextCache();
  public static final int
    LINE_COMMENT=1, MULTILINE_COMMENT=2, WS=3, DEV_CHANGE_POINT=4, ENRICH=5, 
    EXPLAIN=6, DISSECT=7, EVAL=8, GROK=9, LIMIT=10, ROW=11, SORT=12, STATS=13, 
    WHERE=14, DEV_INLINESTATS=15, FROM=16, DEV_METRICS=17, DEV_FORK=18, JOIN_LOOKUP=19, 
    DEV_JOIN_FULL=20, DEV_JOIN_LEFT=21, DEV_JOIN_RIGHT=22, DEV_LOOKUP=23, 
    MV_EXPAND=24, DROP=25, KEEP=26, DEV_INSIST=27, DEV_RRF=28, RENAME=29, 
    SHOW=30, UNKNOWN_CMD=31, CHANGE_POINT_LINE_COMMENT=32, CHANGE_POINT_MULTILINE_COMMENT=33, 
    CHANGE_POINT_WS=34, ON=35, WITH=36, ENRICH_POLICY_NAME=37, ENRICH_LINE_COMMENT=38, 
    ENRICH_MULTILINE_COMMENT=39, ENRICH_WS=40, ENRICH_FIELD_LINE_COMMENT=41, 
    ENRICH_FIELD_MULTILINE_COMMENT=42, ENRICH_FIELD_WS=43, SETTING=44, SETTING_LINE_COMMENT=45, 
    SETTTING_MULTILINE_COMMENT=46, SETTING_WS=47, EXPLAIN_WS=48, EXPLAIN_LINE_COMMENT=49, 
    EXPLAIN_MULTILINE_COMMENT=50, PIPE=51, QUOTED_STRING=52, INTEGER_LITERAL=53, 
    DECIMAL_LITERAL=54, BY=55, AND=56, ASC=57, ASSIGN=58, CAST_OP=59, COLON=60, 
    COMMA=61, DESC=62, DOT=63, FALSE=64, FIRST=65, IN=66, IS=67, LAST=68, 
    LIKE=69, NOT=70, NULL=71, NULLS=72, OR=73, PARAM=74, RLIKE=75, TRUE=76, 
    EQ=77, CIEQ=78, NEQ=79, LT=80, LTE=81, GT=82, GTE=83, PLUS=84, MINUS=85, 
    ASTERISK=86, SLASH=87, PERCENT=88, LEFT_BRACES=89, RIGHT_BRACES=90, DOUBLE_PARAMS=91, 
    NAMED_OR_POSITIONAL_PARAM=92, NAMED_OR_POSITIONAL_DOUBLE_PARAMS=93, OPENING_BRACKET=94, 
    CLOSING_BRACKET=95, LP=96, RP=97, UNQUOTED_IDENTIFIER=98, QUOTED_IDENTIFIER=99, 
    EXPR_LINE_COMMENT=100, EXPR_MULTILINE_COMMENT=101, EXPR_WS=102, METADATA=103, 
    UNQUOTED_SOURCE=104, FROM_LINE_COMMENT=105, FROM_MULTILINE_COMMENT=106, 
    FROM_WS=107, FORK_WS=108, FORK_LINE_COMMENT=109, FORK_MULTILINE_COMMENT=110, 
    JOIN=111, USING=112, JOIN_LINE_COMMENT=113, JOIN_MULTILINE_COMMENT=114, 
    JOIN_WS=115, LOOKUP_LINE_COMMENT=116, LOOKUP_MULTILINE_COMMENT=117, LOOKUP_WS=118, 
    LOOKUP_FIELD_LINE_COMMENT=119, LOOKUP_FIELD_MULTILINE_COMMENT=120, LOOKUP_FIELD_WS=121, 
    MVEXPAND_LINE_COMMENT=122, MVEXPAND_MULTILINE_COMMENT=123, MVEXPAND_WS=124, 
    ID_PATTERN=125, PROJECT_LINE_COMMENT=126, PROJECT_MULTILINE_COMMENT=127, 
    PROJECT_WS=128, AS=129, RENAME_LINE_COMMENT=130, RENAME_MULTILINE_COMMENT=131, 
    RENAME_WS=132, INFO=133, SHOW_LINE_COMMENT=134, SHOW_MULTILINE_COMMENT=135, 
    SHOW_WS=136;
  public static final int
    RULE_singleStatement = 0, RULE_query = 1, RULE_sourceCommand = 2, RULE_processingCommand = 3, 
    RULE_whereCommand = 4, RULE_dataType = 5, RULE_rowCommand = 6, RULE_fields = 7, 
    RULE_field = 8, RULE_fromCommand = 9, RULE_metricsCommand = 10, RULE_indexPatternAndMetadataFields = 11, 
    RULE_indexPattern = 12, RULE_clusterString = 13, RULE_selectorString = 14, 
    RULE_indexString = 15, RULE_metadata = 16, RULE_evalCommand = 17, RULE_statsCommand = 18, 
    RULE_aggFields = 19, RULE_aggField = 20, RULE_qualifiedName = 21, RULE_qualifiedNamePattern = 22, 
    RULE_qualifiedNamePatterns = 23, RULE_identifier = 24, RULE_identifierPattern = 25, 
    RULE_parameter = 26, RULE_doubleParameter = 27, RULE_identifierOrParameter = 28, 
    RULE_limitCommand = 29, RULE_sortCommand = 30, RULE_orderExpression = 31, 
    RULE_keepCommand = 32, RULE_dropCommand = 33, RULE_renameCommand = 34, 
    RULE_renameClause = 35, RULE_dissectCommand = 36, RULE_grokCommand = 37, 
    RULE_mvExpandCommand = 38, RULE_commandOptions = 39, RULE_commandOption = 40, 
    RULE_explainCommand = 41, RULE_subqueryExpression = 42, RULE_showCommand = 43, 
    RULE_enrichCommand = 44, RULE_enrichWithClause = 45, RULE_lookupCommand = 46, 
    RULE_inlinestatsCommand = 47, RULE_changePointCommand = 48, RULE_insistCommand = 49, 
    RULE_forkCommand = 50, RULE_forkSubQueries = 51, RULE_forkSubQuery = 52, 
    RULE_forkSubQueryCommand = 53, RULE_forkSubQueryProcessingCommand = 54, 
    RULE_rrfCommand = 55, RULE_booleanExpression = 56, RULE_regexBooleanExpression = 57, 
    RULE_matchBooleanExpression = 58, RULE_valueExpression = 59, RULE_operatorExpression = 60, 
    RULE_primaryExpression = 61, RULE_functionExpression = 62, RULE_functionName = 63, 
    RULE_mapExpression = 64, RULE_entryExpression = 65, RULE_constant = 66, 
    RULE_booleanValue = 67, RULE_numericValue = 68, RULE_decimalValue = 69, 
    RULE_integerValue = 70, RULE_string = 71, RULE_comparisonOperator = 72, 
    RULE_joinCommand = 73, RULE_joinTarget = 74, RULE_joinCondition = 75, 
    RULE_joinPredicate = 76;
  private static String[] makeRuleNames() {
    return new String[] {
      "singleStatement", "query", "sourceCommand", "processingCommand", "whereCommand", 
      "dataType", "rowCommand", "fields", "field", "fromCommand", "metricsCommand", 
      "indexPatternAndMetadataFields", "indexPattern", "clusterString", "selectorString", 
      "indexString", "metadata", "evalCommand", "statsCommand", "aggFields", 
      "aggField", "qualifiedName", "qualifiedNamePattern", "qualifiedNamePatterns", 
      "identifier", "identifierPattern", "parameter", "doubleParameter", "identifierOrParameter", 
      "limitCommand", "sortCommand", "orderExpression", "keepCommand", "dropCommand", 
      "renameCommand", "renameClause", "dissectCommand", "grokCommand", "mvExpandCommand", 
      "commandOptions", "commandOption", "explainCommand", "subqueryExpression", 
      "showCommand", "enrichCommand", "enrichWithClause", "lookupCommand", 
      "inlinestatsCommand", "changePointCommand", "insistCommand", "forkCommand", 
      "forkSubQueries", "forkSubQuery", "forkSubQueryCommand", "forkSubQueryProcessingCommand", 
      "rrfCommand", "booleanExpression", "regexBooleanExpression", "matchBooleanExpression", 
      "valueExpression", "operatorExpression", "primaryExpression", "functionExpression", 
      "functionName", "mapExpression", "entryExpression", "constant", "booleanValue", 
      "numericValue", "decimalValue", "integerValue", "string", "comparisonOperator", 
      "joinCommand", "joinTarget", "joinCondition", "joinPredicate"
    };
  }
  public static final String[] ruleNames = makeRuleNames();

  private static String[] makeLiteralNames() {
    return new String[] {
      null, null, null, null, null, "'enrich'", "'explain'", "'dissect'", "'eval'", 
      "'grok'", "'limit'", "'row'", "'sort'", "'stats'", "'where'", null, "'from'", 
      null, null, "'lookup'", null, null, null, null, "'mv_expand'", "'drop'", 
      "'keep'", null, null, "'rename'", "'show'", null, null, null, null, "'on'", 
      "'with'", null, null, null, null, null, null, null, null, null, null, 
      null, null, null, null, "'|'", null, null, null, "'by'", "'and'", "'asc'", 
      "'='", "'::'", "':'", "','", "'desc'", "'.'", "'false'", "'first'", "'in'", 
      "'is'", "'last'", "'like'", "'not'", "'null'", "'nulls'", "'or'", "'?'", 
      "'rlike'", "'true'", "'=='", "'=~'", "'!='", "'<'", "'<='", "'>'", "'>='", 
      "'+'", "'-'", "'*'", "'/'", "'%'", "'{'", "'}'", "'??'", null, null, 
      null, "']'", null, "')'", null, null, null, null, null, "'metadata'", 
      null, null, null, null, null, null, null, "'join'", "'USING'", null, 
      null, null, null, null, null, null, null, null, null, null, null, null, 
      null, null, null, "'as'", null, null, null, "'info'"
    };
  }
  private static final String[] _LITERAL_NAMES = makeLiteralNames();
  private static String[] makeSymbolicNames() {
    return new String[] {
      null, "LINE_COMMENT", "MULTILINE_COMMENT", "WS", "DEV_CHANGE_POINT", 
      "ENRICH", "EXPLAIN", "DISSECT", "EVAL", "GROK", "LIMIT", "ROW", "SORT", 
      "STATS", "WHERE", "DEV_INLINESTATS", "FROM", "DEV_METRICS", "DEV_FORK", 
      "JOIN_LOOKUP", "DEV_JOIN_FULL", "DEV_JOIN_LEFT", "DEV_JOIN_RIGHT", "DEV_LOOKUP", 
      "MV_EXPAND", "DROP", "KEEP", "DEV_INSIST", "DEV_RRF", "RENAME", "SHOW", 
      "UNKNOWN_CMD", "CHANGE_POINT_LINE_COMMENT", "CHANGE_POINT_MULTILINE_COMMENT", 
      "CHANGE_POINT_WS", "ON", "WITH", "ENRICH_POLICY_NAME", "ENRICH_LINE_COMMENT", 
      "ENRICH_MULTILINE_COMMENT", "ENRICH_WS", "ENRICH_FIELD_LINE_COMMENT", 
      "ENRICH_FIELD_MULTILINE_COMMENT", "ENRICH_FIELD_WS", "SETTING", "SETTING_LINE_COMMENT", 
      "SETTTING_MULTILINE_COMMENT", "SETTING_WS", "EXPLAIN_WS", "EXPLAIN_LINE_COMMENT", 
      "EXPLAIN_MULTILINE_COMMENT", "PIPE", "QUOTED_STRING", "INTEGER_LITERAL", 
      "DECIMAL_LITERAL", "BY", "AND", "ASC", "ASSIGN", "CAST_OP", "COLON", 
      "COMMA", "DESC", "DOT", "FALSE", "FIRST", "IN", "IS", "LAST", "LIKE", 
      "NOT", "NULL", "NULLS", "OR", "PARAM", "RLIKE", "TRUE", "EQ", "CIEQ", 
      "NEQ", "LT", "LTE", "GT", "GTE", "PLUS", "MINUS", "ASTERISK", "SLASH", 
      "PERCENT", "LEFT_BRACES", "RIGHT_BRACES", "DOUBLE_PARAMS", "NAMED_OR_POSITIONAL_PARAM", 
      "NAMED_OR_POSITIONAL_DOUBLE_PARAMS", "OPENING_BRACKET", "CLOSING_BRACKET", 
      "LP", "RP", "UNQUOTED_IDENTIFIER", "QUOTED_IDENTIFIER", "EXPR_LINE_COMMENT", 
      "EXPR_MULTILINE_COMMENT", "EXPR_WS", "METADATA", "UNQUOTED_SOURCE", "FROM_LINE_COMMENT", 
      "FROM_MULTILINE_COMMENT", "FROM_WS", "FORK_WS", "FORK_LINE_COMMENT", 
      "FORK_MULTILINE_COMMENT", "JOIN", "USING", "JOIN_LINE_COMMENT", "JOIN_MULTILINE_COMMENT", 
      "JOIN_WS", "LOOKUP_LINE_COMMENT", "LOOKUP_MULTILINE_COMMENT", "LOOKUP_WS", 
      "LOOKUP_FIELD_LINE_COMMENT", "LOOKUP_FIELD_MULTILINE_COMMENT", "LOOKUP_FIELD_WS", 
      "MVEXPAND_LINE_COMMENT", "MVEXPAND_MULTILINE_COMMENT", "MVEXPAND_WS", 
      "ID_PATTERN", "PROJECT_LINE_COMMENT", "PROJECT_MULTILINE_COMMENT", "PROJECT_WS", 
      "AS", "RENAME_LINE_COMMENT", "RENAME_MULTILINE_COMMENT", "RENAME_WS", 
      "INFO", "SHOW_LINE_COMMENT", "SHOW_MULTILINE_COMMENT", "SHOW_WS"
    };
  }
  private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
  public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

  /**
   * @deprecated Use {@link #VOCABULARY} instead.
   */
  @Deprecated
  public static final String[] tokenNames;
  static {
    tokenNames = new String[_SYMBOLIC_NAMES.length];
    for (int i = 0; i < tokenNames.length; i++) {
      tokenNames[i] = VOCABULARY.getLiteralName(i);
      if (tokenNames[i] == null) {
        tokenNames[i] = VOCABULARY.getSymbolicName(i);
      }

      if (tokenNames[i] == null) {
        tokenNames[i] = "<INVALID>";
      }
    }
  }

  @Override
  @Deprecated
  public String[] getTokenNames() {
    return tokenNames;
  }

  @Override

  public Vocabulary getVocabulary() {
    return VOCABULARY;
  }

  @Override
  public String getGrammarFileName() { return "EsqlBaseParser.g4"; }

  @Override
  public String[] getRuleNames() { return ruleNames; }

  @Override
  public String getSerializedATN() { return _serializedATN; }

  @Override
  public ATN getATN() { return _ATN; }

  @SuppressWarnings("this-escape")
  public EsqlBaseParser(TokenStream input) {
    super(input);
    _interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
  }

  @SuppressWarnings("CheckReturnValue")
  public static class SingleStatementContext extends ParserRuleContext {
    public QueryContext query() {
      return getRuleContext(QueryContext.class,0);
    }
    public TerminalNode EOF() { return getToken(EsqlBaseParser.EOF, 0); }
    @SuppressWarnings("this-escape")
    public SingleStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_singleStatement; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterSingleStatement(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitSingleStatement(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitSingleStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SingleStatementContext singleStatement() throws RecognitionException {
    SingleStatementContext _localctx = new SingleStatementContext(_ctx, getState());
    enterRule(_localctx, 0, RULE_singleStatement);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(154);
      query(0);
      setState(155);
      match(EOF);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class QueryContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public QueryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_query; }
   
    @SuppressWarnings("this-escape")
    public QueryContext() { }
    public void copyFrom(QueryContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class CompositeQueryContext extends QueryContext {
    public QueryContext query() {
      return getRuleContext(QueryContext.class,0);
    }
    public TerminalNode PIPE() { return getToken(EsqlBaseParser.PIPE, 0); }
    public ProcessingCommandContext processingCommand() {
      return getRuleContext(ProcessingCommandContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public CompositeQueryContext(QueryContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterCompositeQuery(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitCompositeQuery(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitCompositeQuery(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class SingleCommandQueryContext extends QueryContext {
    public SourceCommandContext sourceCommand() {
      return getRuleContext(SourceCommandContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public SingleCommandQueryContext(QueryContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterSingleCommandQuery(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitSingleCommandQuery(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitSingleCommandQuery(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QueryContext query() throws RecognitionException {
    return query(0);
  }

  private QueryContext query(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    QueryContext _localctx = new QueryContext(_ctx, _parentState);
    QueryContext _prevctx = _localctx;
    int _startState = 2;
    enterRecursionRule(_localctx, 2, RULE_query, _p);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      {
      _localctx = new SingleCommandQueryContext(_localctx);
      _ctx = _localctx;
      _prevctx = _localctx;

      setState(158);
      sourceCommand();
      }
      _ctx.stop = _input.LT(-1);
      setState(165);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,0,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          {
          _localctx = new CompositeQueryContext(new QueryContext(_parentctx, _parentState));
          pushNewRecursionContext(_localctx, _startState, RULE_query);
          setState(160);
          if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(161);
          match(PIPE);
          setState(162);
          processingCommand();
          }
          } 
        }
        setState(167);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,0,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class SourceCommandContext extends ParserRuleContext {
    public ExplainCommandContext explainCommand() {
      return getRuleContext(ExplainCommandContext.class,0);
    }
    public FromCommandContext fromCommand() {
      return getRuleContext(FromCommandContext.class,0);
    }
    public RowCommandContext rowCommand() {
      return getRuleContext(RowCommandContext.class,0);
    }
    public ShowCommandContext showCommand() {
      return getRuleContext(ShowCommandContext.class,0);
    }
    public MetricsCommandContext metricsCommand() {
      return getRuleContext(MetricsCommandContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public SourceCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_sourceCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterSourceCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitSourceCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitSourceCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SourceCommandContext sourceCommand() throws RecognitionException {
    SourceCommandContext _localctx = new SourceCommandContext(_ctx, getState());
    enterRule(_localctx, 4, RULE_sourceCommand);
    try {
      setState(174);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,1,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(168);
        explainCommand();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(169);
        fromCommand();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(170);
        rowCommand();
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(171);
        showCommand();
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(172);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(173);
        metricsCommand();
        }
        break;
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ProcessingCommandContext extends ParserRuleContext {
    public EvalCommandContext evalCommand() {
      return getRuleContext(EvalCommandContext.class,0);
    }
    public WhereCommandContext whereCommand() {
      return getRuleContext(WhereCommandContext.class,0);
    }
    public KeepCommandContext keepCommand() {
      return getRuleContext(KeepCommandContext.class,0);
    }
    public LimitCommandContext limitCommand() {
      return getRuleContext(LimitCommandContext.class,0);
    }
    public StatsCommandContext statsCommand() {
      return getRuleContext(StatsCommandContext.class,0);
    }
    public SortCommandContext sortCommand() {
      return getRuleContext(SortCommandContext.class,0);
    }
    public DropCommandContext dropCommand() {
      return getRuleContext(DropCommandContext.class,0);
    }
    public RenameCommandContext renameCommand() {
      return getRuleContext(RenameCommandContext.class,0);
    }
    public DissectCommandContext dissectCommand() {
      return getRuleContext(DissectCommandContext.class,0);
    }
    public GrokCommandContext grokCommand() {
      return getRuleContext(GrokCommandContext.class,0);
    }
    public EnrichCommandContext enrichCommand() {
      return getRuleContext(EnrichCommandContext.class,0);
    }
    public MvExpandCommandContext mvExpandCommand() {
      return getRuleContext(MvExpandCommandContext.class,0);
    }
    public JoinCommandContext joinCommand() {
      return getRuleContext(JoinCommandContext.class,0);
    }
    public InlinestatsCommandContext inlinestatsCommand() {
      return getRuleContext(InlinestatsCommandContext.class,0);
    }
    public LookupCommandContext lookupCommand() {
      return getRuleContext(LookupCommandContext.class,0);
    }
    public ChangePointCommandContext changePointCommand() {
      return getRuleContext(ChangePointCommandContext.class,0);
    }
    public InsistCommandContext insistCommand() {
      return getRuleContext(InsistCommandContext.class,0);
    }
    public ForkCommandContext forkCommand() {
      return getRuleContext(ForkCommandContext.class,0);
    }
    public RrfCommandContext rrfCommand() {
      return getRuleContext(RrfCommandContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public ProcessingCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_processingCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterProcessingCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitProcessingCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitProcessingCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ProcessingCommandContext processingCommand() throws RecognitionException {
    ProcessingCommandContext _localctx = new ProcessingCommandContext(_ctx, getState());
    enterRule(_localctx, 6, RULE_processingCommand);
    try {
      setState(201);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(176);
        evalCommand();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(177);
        whereCommand();
        }
        break;
      case 3:
        enterOuterAlt(_localctx, 3);
        {
        setState(178);
        keepCommand();
        }
        break;
      case 4:
        enterOuterAlt(_localctx, 4);
        {
        setState(179);
        limitCommand();
        }
        break;
      case 5:
        enterOuterAlt(_localctx, 5);
        {
        setState(180);
        statsCommand();
        }
        break;
      case 6:
        enterOuterAlt(_localctx, 6);
        {
        setState(181);
        sortCommand();
        }
        break;
      case 7:
        enterOuterAlt(_localctx, 7);
        {
        setState(182);
        dropCommand();
        }
        break;
      case 8:
        enterOuterAlt(_localctx, 8);
        {
        setState(183);
        renameCommand();
        }
        break;
      case 9:
        enterOuterAlt(_localctx, 9);
        {
        setState(184);
        dissectCommand();
        }
        break;
      case 10:
        enterOuterAlt(_localctx, 10);
        {
        setState(185);
        grokCommand();
        }
        break;
      case 11:
        enterOuterAlt(_localctx, 11);
        {
        setState(186);
        enrichCommand();
        }
        break;
      case 12:
        enterOuterAlt(_localctx, 12);
        {
        setState(187);
        mvExpandCommand();
        }
        break;
      case 13:
        enterOuterAlt(_localctx, 13);
        {
        setState(188);
        joinCommand();
        }
        break;
      case 14:
        enterOuterAlt(_localctx, 14);
        {
        setState(189);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(190);
        inlinestatsCommand();
        }
        break;
      case 15:
        enterOuterAlt(_localctx, 15);
        {
        setState(191);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(192);
        lookupCommand();
        }
        break;
      case 16:
        enterOuterAlt(_localctx, 16);
        {
        setState(193);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(194);
        changePointCommand();
        }
        break;
      case 17:
        enterOuterAlt(_localctx, 17);
        {
        setState(195);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(196);
        insistCommand();
        }
        break;
      case 18:
        enterOuterAlt(_localctx, 18);
        {
        setState(197);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(198);
        forkCommand();
        }
        break;
      case 19:
        enterOuterAlt(_localctx, 19);
        {
        setState(199);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(200);
        rrfCommand();
        }
        break;
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class WhereCommandContext extends ParserRuleContext {
    public TerminalNode WHERE() { return getToken(EsqlBaseParser.WHERE, 0); }
    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public WhereCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_whereCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterWhereCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitWhereCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitWhereCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final WhereCommandContext whereCommand() throws RecognitionException {
    WhereCommandContext _localctx = new WhereCommandContext(_ctx, getState());
    enterRule(_localctx, 8, RULE_whereCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(203);
      match(WHERE);
      setState(204);
      booleanExpression(0);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class DataTypeContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public DataTypeContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_dataType; }
   
    @SuppressWarnings("this-escape")
    public DataTypeContext() { }
    public void copyFrom(DataTypeContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ToDataTypeContext extends DataTypeContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public ToDataTypeContext(DataTypeContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterToDataType(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitToDataType(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitToDataType(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DataTypeContext dataType() throws RecognitionException {
    DataTypeContext _localctx = new DataTypeContext(_ctx, getState());
    enterRule(_localctx, 10, RULE_dataType);
    try {
      _localctx = new ToDataTypeContext(_localctx);
      enterOuterAlt(_localctx, 1);
      {
      setState(206);
      identifier();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class RowCommandContext extends ParserRuleContext {
    public TerminalNode ROW() { return getToken(EsqlBaseParser.ROW, 0); }
    public FieldsContext fields() {
      return getRuleContext(FieldsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public RowCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_rowCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterRowCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitRowCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitRowCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final RowCommandContext rowCommand() throws RecognitionException {
    RowCommandContext _localctx = new RowCommandContext(_ctx, getState());
    enterRule(_localctx, 12, RULE_rowCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(208);
      match(ROW);
      setState(209);
      fields();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class FieldsContext extends ParserRuleContext {
    public List<FieldContext> field() {
      return getRuleContexts(FieldContext.class);
    }
    public FieldContext field(int i) {
      return getRuleContext(FieldContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public FieldsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_fields; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterFields(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitFields(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitFields(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FieldsContext fields() throws RecognitionException {
    FieldsContext _localctx = new FieldsContext(_ctx, getState());
    enterRule(_localctx, 14, RULE_fields);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(211);
      field();
      setState(216);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,3,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(212);
          match(COMMA);
          setState(213);
          field();
          }
          } 
        }
        setState(218);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,3,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class FieldContext extends ParserRuleContext {
    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class,0);
    }
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    @SuppressWarnings("this-escape")
    public FieldContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_field; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterField(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitField(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitField(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FieldContext field() throws RecognitionException {
    FieldContext _localctx = new FieldContext(_ctx, getState());
    enterRule(_localctx, 16, RULE_field);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(222);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
      case 1:
        {
        setState(219);
        qualifiedName();
        setState(220);
        match(ASSIGN);
        }
        break;
      }
      setState(224);
      booleanExpression(0);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class FromCommandContext extends ParserRuleContext {
    public TerminalNode FROM() { return getToken(EsqlBaseParser.FROM, 0); }
    public IndexPatternAndMetadataFieldsContext indexPatternAndMetadataFields() {
      return getRuleContext(IndexPatternAndMetadataFieldsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public FromCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_fromCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterFromCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitFromCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitFromCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FromCommandContext fromCommand() throws RecognitionException {
    FromCommandContext _localctx = new FromCommandContext(_ctx, getState());
    enterRule(_localctx, 18, RULE_fromCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(226);
      match(FROM);
      setState(227);
      indexPatternAndMetadataFields();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class MetricsCommandContext extends ParserRuleContext {
    public TerminalNode DEV_METRICS() { return getToken(EsqlBaseParser.DEV_METRICS, 0); }
    public IndexPatternAndMetadataFieldsContext indexPatternAndMetadataFields() {
      return getRuleContext(IndexPatternAndMetadataFieldsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public MetricsCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_metricsCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterMetricsCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitMetricsCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitMetricsCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MetricsCommandContext metricsCommand() throws RecognitionException {
    MetricsCommandContext _localctx = new MetricsCommandContext(_ctx, getState());
    enterRule(_localctx, 20, RULE_metricsCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(229);
      match(DEV_METRICS);
      setState(230);
      indexPatternAndMetadataFields();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class IndexPatternAndMetadataFieldsContext extends ParserRuleContext {
    public List<IndexPatternContext> indexPattern() {
      return getRuleContexts(IndexPatternContext.class);
    }
    public IndexPatternContext indexPattern(int i) {
      return getRuleContext(IndexPatternContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    public MetadataContext metadata() {
      return getRuleContext(MetadataContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public IndexPatternAndMetadataFieldsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_indexPatternAndMetadataFields; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIndexPatternAndMetadataFields(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIndexPatternAndMetadataFields(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIndexPatternAndMetadataFields(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IndexPatternAndMetadataFieldsContext indexPatternAndMetadataFields() throws RecognitionException {
    IndexPatternAndMetadataFieldsContext _localctx = new IndexPatternAndMetadataFieldsContext(_ctx, getState());
    enterRule(_localctx, 22, RULE_indexPatternAndMetadataFields);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(232);
      indexPattern();
      setState(237);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,5,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(233);
          match(COMMA);
          setState(234);
          indexPattern();
          }
          } 
        }
        setState(239);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,5,_ctx);
      }
      setState(241);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
      case 1:
        {
        setState(240);
        metadata();
        }
        break;
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class IndexPatternContext extends ParserRuleContext {
    public IndexStringContext indexString() {
      return getRuleContext(IndexStringContext.class,0);
    }
    public ClusterStringContext clusterString() {
      return getRuleContext(ClusterStringContext.class,0);
    }
    public TerminalNode COLON() { return getToken(EsqlBaseParser.COLON, 0); }
    public TerminalNode CAST_OP() { return getToken(EsqlBaseParser.CAST_OP, 0); }
    public SelectorStringContext selectorString() {
      return getRuleContext(SelectorStringContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public IndexPatternContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_indexPattern; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIndexPattern(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIndexPattern(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIndexPattern(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IndexPatternContext indexPattern() throws RecognitionException {
    IndexPatternContext _localctx = new IndexPatternContext(_ctx, getState());
    enterRule(_localctx, 24, RULE_indexPattern);
    try {
      setState(255);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,9,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(246);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,7,_ctx) ) {
        case 1:
          {
          setState(243);
          clusterString();
          setState(244);
          match(COLON);
          }
          break;
        }
        setState(248);
        indexString();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(249);
        if (!(this.isDevVersion())) throw new FailedPredicateException(this, "this.isDevVersion()");
        setState(250);
        indexString();
        setState(253);
        _errHandler.sync(this);
        switch ( getInterpreter().adaptivePredict(_input,8,_ctx) ) {
        case 1:
          {
          setState(251);
          match(CAST_OP);
          setState(252);
          selectorString();
          }
          break;
        }
        }
        break;
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ClusterStringContext extends ParserRuleContext {
    public TerminalNode UNQUOTED_SOURCE() { return getToken(EsqlBaseParser.UNQUOTED_SOURCE, 0); }
    public TerminalNode QUOTED_STRING() { return getToken(EsqlBaseParser.QUOTED_STRING, 0); }
    @SuppressWarnings("this-escape")
    public ClusterStringContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_clusterString; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterClusterString(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitClusterString(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitClusterString(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ClusterStringContext clusterString() throws RecognitionException {
    ClusterStringContext _localctx = new ClusterStringContext(_ctx, getState());
    enterRule(_localctx, 26, RULE_clusterString);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(257);
      _la = _input.LA(1);
      if ( !(_la==QUOTED_STRING || _la==UNQUOTED_SOURCE) ) {
      _errHandler.recoverInline(this);
      }
      else {
        if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
        _errHandler.reportMatch(this);
        consume();
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class SelectorStringContext extends ParserRuleContext {
    public TerminalNode UNQUOTED_SOURCE() { return getToken(EsqlBaseParser.UNQUOTED_SOURCE, 0); }
    public TerminalNode QUOTED_STRING() { return getToken(EsqlBaseParser.QUOTED_STRING, 0); }
    @SuppressWarnings("this-escape")
    public SelectorStringContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_selectorString; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterSelectorString(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitSelectorString(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitSelectorString(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SelectorStringContext selectorString() throws RecognitionException {
    SelectorStringContext _localctx = new SelectorStringContext(_ctx, getState());
    enterRule(_localctx, 28, RULE_selectorString);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(259);
      _la = _input.LA(1);
      if ( !(_la==QUOTED_STRING || _la==UNQUOTED_SOURCE) ) {
      _errHandler.recoverInline(this);
      }
      else {
        if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
        _errHandler.reportMatch(this);
        consume();
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class IndexStringContext extends ParserRuleContext {
    public TerminalNode UNQUOTED_SOURCE() { return getToken(EsqlBaseParser.UNQUOTED_SOURCE, 0); }
    public TerminalNode QUOTED_STRING() { return getToken(EsqlBaseParser.QUOTED_STRING, 0); }
    @SuppressWarnings("this-escape")
    public IndexStringContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_indexString; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIndexString(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIndexString(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIndexString(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IndexStringContext indexString() throws RecognitionException {
    IndexStringContext _localctx = new IndexStringContext(_ctx, getState());
    enterRule(_localctx, 30, RULE_indexString);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(261);
      _la = _input.LA(1);
      if ( !(_la==QUOTED_STRING || _la==UNQUOTED_SOURCE) ) {
      _errHandler.recoverInline(this);
      }
      else {
        if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
        _errHandler.reportMatch(this);
        consume();
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class MetadataContext extends ParserRuleContext {
    public TerminalNode METADATA() { return getToken(EsqlBaseParser.METADATA, 0); }
    public List<TerminalNode> UNQUOTED_SOURCE() { return getTokens(EsqlBaseParser.UNQUOTED_SOURCE); }
    public TerminalNode UNQUOTED_SOURCE(int i) {
      return getToken(EsqlBaseParser.UNQUOTED_SOURCE, i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public MetadataContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_metadata; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterMetadata(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitMetadata(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitMetadata(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MetadataContext metadata() throws RecognitionException {
    MetadataContext _localctx = new MetadataContext(_ctx, getState());
    enterRule(_localctx, 32, RULE_metadata);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(263);
      match(METADATA);
      setState(264);
      match(UNQUOTED_SOURCE);
      setState(269);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,10,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(265);
          match(COMMA);
          setState(266);
          match(UNQUOTED_SOURCE);
          }
          } 
        }
        setState(271);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,10,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class EvalCommandContext extends ParserRuleContext {
    public TerminalNode EVAL() { return getToken(EsqlBaseParser.EVAL, 0); }
    public FieldsContext fields() {
      return getRuleContext(FieldsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public EvalCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_evalCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterEvalCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitEvalCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitEvalCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final EvalCommandContext evalCommand() throws RecognitionException {
    EvalCommandContext _localctx = new EvalCommandContext(_ctx, getState());
    enterRule(_localctx, 34, RULE_evalCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(272);
      match(EVAL);
      setState(273);
      fields();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class StatsCommandContext extends ParserRuleContext {
    public AggFieldsContext stats;
    public FieldsContext grouping;
    public TerminalNode STATS() { return getToken(EsqlBaseParser.STATS, 0); }
    public TerminalNode BY() { return getToken(EsqlBaseParser.BY, 0); }
    public AggFieldsContext aggFields() {
      return getRuleContext(AggFieldsContext.class,0);
    }
    public FieldsContext fields() {
      return getRuleContext(FieldsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public StatsCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_statsCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterStatsCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitStatsCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitStatsCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final StatsCommandContext statsCommand() throws RecognitionException {
    StatsCommandContext _localctx = new StatsCommandContext(_ctx, getState());
    enterRule(_localctx, 36, RULE_statsCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(275);
      match(STATS);
      setState(277);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,11,_ctx) ) {
      case 1:
        {
        setState(276);
        ((StatsCommandContext)_localctx).stats = aggFields();
        }
        break;
      }
      setState(281);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
      case 1:
        {
        setState(279);
        match(BY);
        setState(280);
        ((StatsCommandContext)_localctx).grouping = fields();
        }
        break;
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class AggFieldsContext extends ParserRuleContext {
    public List<AggFieldContext> aggField() {
      return getRuleContexts(AggFieldContext.class);
    }
    public AggFieldContext aggField(int i) {
      return getRuleContext(AggFieldContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public AggFieldsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_aggFields; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterAggFields(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitAggFields(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitAggFields(this);
      else return visitor.visitChildren(this);
    }
  }

  public final AggFieldsContext aggFields() throws RecognitionException {
    AggFieldsContext _localctx = new AggFieldsContext(_ctx, getState());
    enterRule(_localctx, 38, RULE_aggFields);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(283);
      aggField();
      setState(288);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,13,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(284);
          match(COMMA);
          setState(285);
          aggField();
          }
          } 
        }
        setState(290);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,13,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class AggFieldContext extends ParserRuleContext {
    public FieldContext field() {
      return getRuleContext(FieldContext.class,0);
    }
    public TerminalNode WHERE() { return getToken(EsqlBaseParser.WHERE, 0); }
    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public AggFieldContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_aggField; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterAggField(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitAggField(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitAggField(this);
      else return visitor.visitChildren(this);
    }
  }

  public final AggFieldContext aggField() throws RecognitionException {
    AggFieldContext _localctx = new AggFieldContext(_ctx, getState());
    enterRule(_localctx, 40, RULE_aggField);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(291);
      field();
      setState(294);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,14,_ctx) ) {
      case 1:
        {
        setState(292);
        match(WHERE);
        setState(293);
        booleanExpression(0);
        }
        break;
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class QualifiedNameContext extends ParserRuleContext {
    public List<IdentifierOrParameterContext> identifierOrParameter() {
      return getRuleContexts(IdentifierOrParameterContext.class);
    }
    public IdentifierOrParameterContext identifierOrParameter(int i) {
      return getRuleContext(IdentifierOrParameterContext.class,i);
    }
    public List<TerminalNode> DOT() { return getTokens(EsqlBaseParser.DOT); }
    public TerminalNode DOT(int i) {
      return getToken(EsqlBaseParser.DOT, i);
    }
    @SuppressWarnings("this-escape")
    public QualifiedNameContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_qualifiedName; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterQualifiedName(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitQualifiedName(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitQualifiedName(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QualifiedNameContext qualifiedName() throws RecognitionException {
    QualifiedNameContext _localctx = new QualifiedNameContext(_ctx, getState());
    enterRule(_localctx, 42, RULE_qualifiedName);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(296);
      identifierOrParameter();
      setState(301);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,15,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(297);
          match(DOT);
          setState(298);
          identifierOrParameter();
          }
          } 
        }
        setState(303);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,15,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class QualifiedNamePatternContext extends ParserRuleContext {
    public List<IdentifierPatternContext> identifierPattern() {
      return getRuleContexts(IdentifierPatternContext.class);
    }
    public IdentifierPatternContext identifierPattern(int i) {
      return getRuleContext(IdentifierPatternContext.class,i);
    }
    public List<TerminalNode> DOT() { return getTokens(EsqlBaseParser.DOT); }
    public TerminalNode DOT(int i) {
      return getToken(EsqlBaseParser.DOT, i);
    }
    @SuppressWarnings("this-escape")
    public QualifiedNamePatternContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_qualifiedNamePattern; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterQualifiedNamePattern(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitQualifiedNamePattern(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitQualifiedNamePattern(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QualifiedNamePatternContext qualifiedNamePattern() throws RecognitionException {
    QualifiedNamePatternContext _localctx = new QualifiedNamePatternContext(_ctx, getState());
    enterRule(_localctx, 44, RULE_qualifiedNamePattern);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(304);
      identifierPattern();
      setState(309);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,16,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(305);
          match(DOT);
          setState(306);
          identifierPattern();
          }
          } 
        }
        setState(311);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,16,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class QualifiedNamePatternsContext extends ParserRuleContext {
    public List<QualifiedNamePatternContext> qualifiedNamePattern() {
      return getRuleContexts(QualifiedNamePatternContext.class);
    }
    public QualifiedNamePatternContext qualifiedNamePattern(int i) {
      return getRuleContext(QualifiedNamePatternContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public QualifiedNamePatternsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_qualifiedNamePatterns; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterQualifiedNamePatterns(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitQualifiedNamePatterns(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitQualifiedNamePatterns(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QualifiedNamePatternsContext qualifiedNamePatterns() throws RecognitionException {
    QualifiedNamePatternsContext _localctx = new QualifiedNamePatternsContext(_ctx, getState());
    enterRule(_localctx, 46, RULE_qualifiedNamePatterns);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(312);
      qualifiedNamePattern();
      setState(317);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,17,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(313);
          match(COMMA);
          setState(314);
          qualifiedNamePattern();
          }
          } 
        }
        setState(319);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,17,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class IdentifierContext extends ParserRuleContext {
    public TerminalNode UNQUOTED_IDENTIFIER() { return getToken(EsqlBaseParser.UNQUOTED_IDENTIFIER, 0); }
    public TerminalNode QUOTED_IDENTIFIER() { return getToken(EsqlBaseParser.QUOTED_IDENTIFIER, 0); }
    @SuppressWarnings("this-escape")
    public IdentifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_identifier; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIdentifier(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIdentifier(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIdentifier(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IdentifierContext identifier() throws RecognitionException {
    IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
    enterRule(_localctx, 48, RULE_identifier);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(320);
      _la = _input.LA(1);
      if ( !(_la==UNQUOTED_IDENTIFIER || _la==QUOTED_IDENTIFIER) ) {
      _errHandler.recoverInline(this);
      }
      else {
        if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
        _errHandler.reportMatch(this);
        consume();
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class IdentifierPatternContext extends ParserRuleContext {
    public TerminalNode ID_PATTERN() { return getToken(EsqlBaseParser.ID_PATTERN, 0); }
    public ParameterContext parameter() {
      return getRuleContext(ParameterContext.class,0);
    }
    public DoubleParameterContext doubleParameter() {
      return getRuleContext(DoubleParameterContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public IdentifierPatternContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_identifierPattern; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIdentifierPattern(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIdentifierPattern(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIdentifierPattern(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IdentifierPatternContext identifierPattern() throws RecognitionException {
    IdentifierPatternContext _localctx = new IdentifierPatternContext(_ctx, getState());
    enterRule(_localctx, 50, RULE_identifierPattern);
    try {
      setState(325);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case ID_PATTERN:
        enterOuterAlt(_localctx, 1);
        {
        setState(322);
        match(ID_PATTERN);
        }
        break;
      case PARAM:
      case NAMED_OR_POSITIONAL_PARAM:
        enterOuterAlt(_localctx, 2);
        {
        setState(323);
        parameter();
        }
        break;
      case DOUBLE_PARAMS:
      case NAMED_OR_POSITIONAL_DOUBLE_PARAMS:
        enterOuterAlt(_localctx, 3);
        {
        setState(324);
        doubleParameter();
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ParameterContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public ParameterContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_parameter; }
   
    @SuppressWarnings("this-escape")
    public ParameterContext() { }
    public void copyFrom(ParameterContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class InputNamedOrPositionalParamContext extends ParameterContext {
    public TerminalNode NAMED_OR_POSITIONAL_PARAM() { return getToken(EsqlBaseParser.NAMED_OR_POSITIONAL_PARAM, 0); }
    @SuppressWarnings("this-escape")
    public InputNamedOrPositionalParamContext(ParameterContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterInputNamedOrPositionalParam(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitInputNamedOrPositionalParam(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitInputNamedOrPositionalParam(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class InputParamContext extends ParameterContext {
    public TerminalNode PARAM() { return getToken(EsqlBaseParser.PARAM, 0); }
    @SuppressWarnings("this-escape")
    public InputParamContext(ParameterContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterInputParam(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitInputParam(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitInputParam(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ParameterContext parameter() throws RecognitionException {
    ParameterContext _localctx = new ParameterContext(_ctx, getState());
    enterRule(_localctx, 52, RULE_parameter);
    try {
      setState(329);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case PARAM:
        _localctx = new InputParamContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(327);
        match(PARAM);
        }
        break;
      case NAMED_OR_POSITIONAL_PARAM:
        _localctx = new InputNamedOrPositionalParamContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(328);
        match(NAMED_OR_POSITIONAL_PARAM);
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class DoubleParameterContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public DoubleParameterContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_doubleParameter; }
   
    @SuppressWarnings("this-escape")
    public DoubleParameterContext() { }
    public void copyFrom(DoubleParameterContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class InputDoubleParamsContext extends DoubleParameterContext {
    public TerminalNode DOUBLE_PARAMS() { return getToken(EsqlBaseParser.DOUBLE_PARAMS, 0); }
    @SuppressWarnings("this-escape")
    public InputDoubleParamsContext(DoubleParameterContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterInputDoubleParams(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitInputDoubleParams(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitInputDoubleParams(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class InputNamedOrPositionalDoubleParamsContext extends DoubleParameterContext {
    public TerminalNode NAMED_OR_POSITIONAL_DOUBLE_PARAMS() { return getToken(EsqlBaseParser.NAMED_OR_POSITIONAL_DOUBLE_PARAMS, 0); }
    @SuppressWarnings("this-escape")
    public InputNamedOrPositionalDoubleParamsContext(DoubleParameterContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterInputNamedOrPositionalDoubleParams(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitInputNamedOrPositionalDoubleParams(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitInputNamedOrPositionalDoubleParams(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DoubleParameterContext doubleParameter() throws RecognitionException {
    DoubleParameterContext _localctx = new DoubleParameterContext(_ctx, getState());
    enterRule(_localctx, 54, RULE_doubleParameter);
    try {
      setState(333);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case DOUBLE_PARAMS:
        _localctx = new InputDoubleParamsContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(331);
        match(DOUBLE_PARAMS);
        }
        break;
      case NAMED_OR_POSITIONAL_DOUBLE_PARAMS:
        _localctx = new InputNamedOrPositionalDoubleParamsContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(332);
        match(NAMED_OR_POSITIONAL_DOUBLE_PARAMS);
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class IdentifierOrParameterContext extends ParserRuleContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public ParameterContext parameter() {
      return getRuleContext(ParameterContext.class,0);
    }
    public DoubleParameterContext doubleParameter() {
      return getRuleContext(DoubleParameterContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public IdentifierOrParameterContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_identifierOrParameter; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIdentifierOrParameter(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIdentifierOrParameter(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIdentifierOrParameter(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IdentifierOrParameterContext identifierOrParameter() throws RecognitionException {
    IdentifierOrParameterContext _localctx = new IdentifierOrParameterContext(_ctx, getState());
    enterRule(_localctx, 56, RULE_identifierOrParameter);
    try {
      setState(338);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case UNQUOTED_IDENTIFIER:
      case QUOTED_IDENTIFIER:
        enterOuterAlt(_localctx, 1);
        {
        setState(335);
        identifier();
        }
        break;
      case PARAM:
      case NAMED_OR_POSITIONAL_PARAM:
        enterOuterAlt(_localctx, 2);
        {
        setState(336);
        parameter();
        }
        break;
      case DOUBLE_PARAMS:
      case NAMED_OR_POSITIONAL_DOUBLE_PARAMS:
        enterOuterAlt(_localctx, 3);
        {
        setState(337);
        doubleParameter();
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class LimitCommandContext extends ParserRuleContext {
    public TerminalNode LIMIT() { return getToken(EsqlBaseParser.LIMIT, 0); }
    public TerminalNode INTEGER_LITERAL() { return getToken(EsqlBaseParser.INTEGER_LITERAL, 0); }
    @SuppressWarnings("this-escape")
    public LimitCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_limitCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterLimitCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitLimitCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitLimitCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LimitCommandContext limitCommand() throws RecognitionException {
    LimitCommandContext _localctx = new LimitCommandContext(_ctx, getState());
    enterRule(_localctx, 58, RULE_limitCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(340);
      match(LIMIT);
      setState(341);
      match(INTEGER_LITERAL);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class SortCommandContext extends ParserRuleContext {
    public TerminalNode SORT() { return getToken(EsqlBaseParser.SORT, 0); }
    public List<OrderExpressionContext> orderExpression() {
      return getRuleContexts(OrderExpressionContext.class);
    }
    public OrderExpressionContext orderExpression(int i) {
      return getRuleContext(OrderExpressionContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public SortCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_sortCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterSortCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitSortCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitSortCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SortCommandContext sortCommand() throws RecognitionException {
    SortCommandContext _localctx = new SortCommandContext(_ctx, getState());
    enterRule(_localctx, 60, RULE_sortCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(343);
      match(SORT);
      setState(344);
      orderExpression();
      setState(349);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,22,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(345);
          match(COMMA);
          setState(346);
          orderExpression();
          }
          } 
        }
        setState(351);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,22,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class OrderExpressionContext extends ParserRuleContext {
    public Token ordering;
    public Token nullOrdering;
    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class,0);
    }
    public TerminalNode NULLS() { return getToken(EsqlBaseParser.NULLS, 0); }
    public TerminalNode ASC() { return getToken(EsqlBaseParser.ASC, 0); }
    public TerminalNode DESC() { return getToken(EsqlBaseParser.DESC, 0); }
    public TerminalNode FIRST() { return getToken(EsqlBaseParser.FIRST, 0); }
    public TerminalNode LAST() { return getToken(EsqlBaseParser.LAST, 0); }
    @SuppressWarnings("this-escape")
    public OrderExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_orderExpression; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterOrderExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitOrderExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitOrderExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final OrderExpressionContext orderExpression() throws RecognitionException {
    OrderExpressionContext _localctx = new OrderExpressionContext(_ctx, getState());
    enterRule(_localctx, 62, RULE_orderExpression);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(352);
      booleanExpression(0);
      setState(354);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,23,_ctx) ) {
      case 1:
        {
        setState(353);
        ((OrderExpressionContext)_localctx).ordering = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==ASC || _la==DESC) ) {
          ((OrderExpressionContext)_localctx).ordering = (Token)_errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
        }
        break;
      }
      setState(358);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,24,_ctx) ) {
      case 1:
        {
        setState(356);
        match(NULLS);
        setState(357);
        ((OrderExpressionContext)_localctx).nullOrdering = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==FIRST || _la==LAST) ) {
          ((OrderExpressionContext)_localctx).nullOrdering = (Token)_errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
        }
        break;
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class KeepCommandContext extends ParserRuleContext {
    public TerminalNode KEEP() { return getToken(EsqlBaseParser.KEEP, 0); }
    public QualifiedNamePatternsContext qualifiedNamePatterns() {
      return getRuleContext(QualifiedNamePatternsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public KeepCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_keepCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterKeepCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitKeepCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitKeepCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final KeepCommandContext keepCommand() throws RecognitionException {
    KeepCommandContext _localctx = new KeepCommandContext(_ctx, getState());
    enterRule(_localctx, 64, RULE_keepCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(360);
      match(KEEP);
      setState(361);
      qualifiedNamePatterns();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class DropCommandContext extends ParserRuleContext {
    public TerminalNode DROP() { return getToken(EsqlBaseParser.DROP, 0); }
    public QualifiedNamePatternsContext qualifiedNamePatterns() {
      return getRuleContext(QualifiedNamePatternsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public DropCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_dropCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterDropCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitDropCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitDropCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DropCommandContext dropCommand() throws RecognitionException {
    DropCommandContext _localctx = new DropCommandContext(_ctx, getState());
    enterRule(_localctx, 66, RULE_dropCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(363);
      match(DROP);
      setState(364);
      qualifiedNamePatterns();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class RenameCommandContext extends ParserRuleContext {
    public TerminalNode RENAME() { return getToken(EsqlBaseParser.RENAME, 0); }
    public List<RenameClauseContext> renameClause() {
      return getRuleContexts(RenameClauseContext.class);
    }
    public RenameClauseContext renameClause(int i) {
      return getRuleContext(RenameClauseContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public RenameCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_renameCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterRenameCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitRenameCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitRenameCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final RenameCommandContext renameCommand() throws RecognitionException {
    RenameCommandContext _localctx = new RenameCommandContext(_ctx, getState());
    enterRule(_localctx, 68, RULE_renameCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(366);
      match(RENAME);
      setState(367);
      renameClause();
      setState(372);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,25,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(368);
          match(COMMA);
          setState(369);
          renameClause();
          }
          } 
        }
        setState(374);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,25,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class RenameClauseContext extends ParserRuleContext {
    public QualifiedNamePatternContext oldName;
    public QualifiedNamePatternContext newName;
    public TerminalNode AS() { return getToken(EsqlBaseParser.AS, 0); }
    public List<QualifiedNamePatternContext> qualifiedNamePattern() {
      return getRuleContexts(QualifiedNamePatternContext.class);
    }
    public QualifiedNamePatternContext qualifiedNamePattern(int i) {
      return getRuleContext(QualifiedNamePatternContext.class,i);
    }
    @SuppressWarnings("this-escape")
    public RenameClauseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_renameClause; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterRenameClause(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitRenameClause(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitRenameClause(this);
      else return visitor.visitChildren(this);
    }
  }

  public final RenameClauseContext renameClause() throws RecognitionException {
    RenameClauseContext _localctx = new RenameClauseContext(_ctx, getState());
    enterRule(_localctx, 70, RULE_renameClause);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(375);
      ((RenameClauseContext)_localctx).oldName = qualifiedNamePattern();
      setState(376);
      match(AS);
      setState(377);
      ((RenameClauseContext)_localctx).newName = qualifiedNamePattern();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class DissectCommandContext extends ParserRuleContext {
    public TerminalNode DISSECT() { return getToken(EsqlBaseParser.DISSECT, 0); }
    public PrimaryExpressionContext primaryExpression() {
      return getRuleContext(PrimaryExpressionContext.class,0);
    }
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    public CommandOptionsContext commandOptions() {
      return getRuleContext(CommandOptionsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public DissectCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_dissectCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterDissectCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitDissectCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitDissectCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DissectCommandContext dissectCommand() throws RecognitionException {
    DissectCommandContext _localctx = new DissectCommandContext(_ctx, getState());
    enterRule(_localctx, 72, RULE_dissectCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(379);
      match(DISSECT);
      setState(380);
      primaryExpression(0);
      setState(381);
      string();
      setState(383);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,26,_ctx) ) {
      case 1:
        {
        setState(382);
        commandOptions();
        }
        break;
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class GrokCommandContext extends ParserRuleContext {
    public TerminalNode GROK() { return getToken(EsqlBaseParser.GROK, 0); }
    public PrimaryExpressionContext primaryExpression() {
      return getRuleContext(PrimaryExpressionContext.class,0);
    }
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public GrokCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_grokCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterGrokCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitGrokCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitGrokCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final GrokCommandContext grokCommand() throws RecognitionException {
    GrokCommandContext _localctx = new GrokCommandContext(_ctx, getState());
    enterRule(_localctx, 74, RULE_grokCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(385);
      match(GROK);
      setState(386);
      primaryExpression(0);
      setState(387);
      string();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class MvExpandCommandContext extends ParserRuleContext {
    public TerminalNode MV_EXPAND() { return getToken(EsqlBaseParser.MV_EXPAND, 0); }
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public MvExpandCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_mvExpandCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterMvExpandCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitMvExpandCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitMvExpandCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MvExpandCommandContext mvExpandCommand() throws RecognitionException {
    MvExpandCommandContext _localctx = new MvExpandCommandContext(_ctx, getState());
    enterRule(_localctx, 76, RULE_mvExpandCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(389);
      match(MV_EXPAND);
      setState(390);
      qualifiedName();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class CommandOptionsContext extends ParserRuleContext {
    public List<CommandOptionContext> commandOption() {
      return getRuleContexts(CommandOptionContext.class);
    }
    public CommandOptionContext commandOption(int i) {
      return getRuleContext(CommandOptionContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public CommandOptionsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_commandOptions; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterCommandOptions(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitCommandOptions(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitCommandOptions(this);
      else return visitor.visitChildren(this);
    }
  }

  public final CommandOptionsContext commandOptions() throws RecognitionException {
    CommandOptionsContext _localctx = new CommandOptionsContext(_ctx, getState());
    enterRule(_localctx, 78, RULE_commandOptions);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(392);
      commandOption();
      setState(397);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,27,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(393);
          match(COMMA);
          setState(394);
          commandOption();
          }
          } 
        }
        setState(399);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,27,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class CommandOptionContext extends ParserRuleContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class,0);
    }
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public CommandOptionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_commandOption; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterCommandOption(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitCommandOption(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitCommandOption(this);
      else return visitor.visitChildren(this);
    }
  }

  public final CommandOptionContext commandOption() throws RecognitionException {
    CommandOptionContext _localctx = new CommandOptionContext(_ctx, getState());
    enterRule(_localctx, 80, RULE_commandOption);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(400);
      identifier();
      setState(401);
      match(ASSIGN);
      setState(402);
      constant();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ExplainCommandContext extends ParserRuleContext {
    public TerminalNode EXPLAIN() { return getToken(EsqlBaseParser.EXPLAIN, 0); }
    public SubqueryExpressionContext subqueryExpression() {
      return getRuleContext(SubqueryExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public ExplainCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_explainCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterExplainCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitExplainCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitExplainCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ExplainCommandContext explainCommand() throws RecognitionException {
    ExplainCommandContext _localctx = new ExplainCommandContext(_ctx, getState());
    enterRule(_localctx, 82, RULE_explainCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(404);
      match(EXPLAIN);
      setState(405);
      subqueryExpression();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class SubqueryExpressionContext extends ParserRuleContext {
    public TerminalNode OPENING_BRACKET() { return getToken(EsqlBaseParser.OPENING_BRACKET, 0); }
    public QueryContext query() {
      return getRuleContext(QueryContext.class,0);
    }
    public TerminalNode CLOSING_BRACKET() { return getToken(EsqlBaseParser.CLOSING_BRACKET, 0); }
    @SuppressWarnings("this-escape")
    public SubqueryExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_subqueryExpression; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterSubqueryExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitSubqueryExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitSubqueryExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SubqueryExpressionContext subqueryExpression() throws RecognitionException {
    SubqueryExpressionContext _localctx = new SubqueryExpressionContext(_ctx, getState());
    enterRule(_localctx, 84, RULE_subqueryExpression);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(407);
      match(OPENING_BRACKET);
      setState(408);
      query(0);
      setState(409);
      match(CLOSING_BRACKET);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ShowCommandContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public ShowCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_showCommand; }
   
    @SuppressWarnings("this-escape")
    public ShowCommandContext() { }
    public void copyFrom(ShowCommandContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ShowInfoContext extends ShowCommandContext {
    public TerminalNode SHOW() { return getToken(EsqlBaseParser.SHOW, 0); }
    public TerminalNode INFO() { return getToken(EsqlBaseParser.INFO, 0); }
    @SuppressWarnings("this-escape")
    public ShowInfoContext(ShowCommandContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterShowInfo(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitShowInfo(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitShowInfo(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ShowCommandContext showCommand() throws RecognitionException {
    ShowCommandContext _localctx = new ShowCommandContext(_ctx, getState());
    enterRule(_localctx, 86, RULE_showCommand);
    try {
      _localctx = new ShowInfoContext(_localctx);
      enterOuterAlt(_localctx, 1);
      {
      setState(411);
      match(SHOW);
      setState(412);
      match(INFO);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class EnrichCommandContext extends ParserRuleContext {
    public Token policyName;
    public QualifiedNamePatternContext matchField;
    public TerminalNode ENRICH() { return getToken(EsqlBaseParser.ENRICH, 0); }
    public TerminalNode ENRICH_POLICY_NAME() { return getToken(EsqlBaseParser.ENRICH_POLICY_NAME, 0); }
    public TerminalNode ON() { return getToken(EsqlBaseParser.ON, 0); }
    public TerminalNode WITH() { return getToken(EsqlBaseParser.WITH, 0); }
    public List<EnrichWithClauseContext> enrichWithClause() {
      return getRuleContexts(EnrichWithClauseContext.class);
    }
    public EnrichWithClauseContext enrichWithClause(int i) {
      return getRuleContext(EnrichWithClauseContext.class,i);
    }
    public QualifiedNamePatternContext qualifiedNamePattern() {
      return getRuleContext(QualifiedNamePatternContext.class,0);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public EnrichCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_enrichCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterEnrichCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitEnrichCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitEnrichCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final EnrichCommandContext enrichCommand() throws RecognitionException {
    EnrichCommandContext _localctx = new EnrichCommandContext(_ctx, getState());
    enterRule(_localctx, 88, RULE_enrichCommand);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(414);
      match(ENRICH);
      setState(415);
      ((EnrichCommandContext)_localctx).policyName = match(ENRICH_POLICY_NAME);
      setState(418);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,28,_ctx) ) {
      case 1:
        {
        setState(416);
        match(ON);
        setState(417);
        ((EnrichCommandContext)_localctx).matchField = qualifiedNamePattern();
        }
        break;
      }
      setState(429);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,30,_ctx) ) {
      case 1:
        {
        setState(420);
        match(WITH);
        setState(421);
        enrichWithClause();
        setState(426);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,29,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(422);
            match(COMMA);
            setState(423);
            enrichWithClause();
            }
            } 
          }
          setState(428);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,29,_ctx);
        }
        }
        break;
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class EnrichWithClauseContext extends ParserRuleContext {
    public QualifiedNamePatternContext newName;
    public QualifiedNamePatternContext enrichField;
    public List<QualifiedNamePatternContext> qualifiedNamePattern() {
      return getRuleContexts(QualifiedNamePatternContext.class);
    }
    public QualifiedNamePatternContext qualifiedNamePattern(int i) {
      return getRuleContext(QualifiedNamePatternContext.class,i);
    }
    public TerminalNode ASSIGN() { return getToken(EsqlBaseParser.ASSIGN, 0); }
    @SuppressWarnings("this-escape")
    public EnrichWithClauseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_enrichWithClause; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterEnrichWithClause(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitEnrichWithClause(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitEnrichWithClause(this);
      else return visitor.visitChildren(this);
    }
  }

  public final EnrichWithClauseContext enrichWithClause() throws RecognitionException {
    EnrichWithClauseContext _localctx = new EnrichWithClauseContext(_ctx, getState());
    enterRule(_localctx, 90, RULE_enrichWithClause);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(434);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,31,_ctx) ) {
      case 1:
        {
        setState(431);
        ((EnrichWithClauseContext)_localctx).newName = qualifiedNamePattern();
        setState(432);
        match(ASSIGN);
        }
        break;
      }
      setState(436);
      ((EnrichWithClauseContext)_localctx).enrichField = qualifiedNamePattern();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class LookupCommandContext extends ParserRuleContext {
    public IndexPatternContext tableName;
    public QualifiedNamePatternsContext matchFields;
    public TerminalNode DEV_LOOKUP() { return getToken(EsqlBaseParser.DEV_LOOKUP, 0); }
    public TerminalNode ON() { return getToken(EsqlBaseParser.ON, 0); }
    public IndexPatternContext indexPattern() {
      return getRuleContext(IndexPatternContext.class,0);
    }
    public QualifiedNamePatternsContext qualifiedNamePatterns() {
      return getRuleContext(QualifiedNamePatternsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public LookupCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_lookupCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterLookupCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitLookupCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitLookupCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LookupCommandContext lookupCommand() throws RecognitionException {
    LookupCommandContext _localctx = new LookupCommandContext(_ctx, getState());
    enterRule(_localctx, 92, RULE_lookupCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(438);
      match(DEV_LOOKUP);
      setState(439);
      ((LookupCommandContext)_localctx).tableName = indexPattern();
      setState(440);
      match(ON);
      setState(441);
      ((LookupCommandContext)_localctx).matchFields = qualifiedNamePatterns();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class InlinestatsCommandContext extends ParserRuleContext {
    public AggFieldsContext stats;
    public FieldsContext grouping;
    public TerminalNode DEV_INLINESTATS() { return getToken(EsqlBaseParser.DEV_INLINESTATS, 0); }
    public AggFieldsContext aggFields() {
      return getRuleContext(AggFieldsContext.class,0);
    }
    public TerminalNode BY() { return getToken(EsqlBaseParser.BY, 0); }
    public FieldsContext fields() {
      return getRuleContext(FieldsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public InlinestatsCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_inlinestatsCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterInlinestatsCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitInlinestatsCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitInlinestatsCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final InlinestatsCommandContext inlinestatsCommand() throws RecognitionException {
    InlinestatsCommandContext _localctx = new InlinestatsCommandContext(_ctx, getState());
    enterRule(_localctx, 94, RULE_inlinestatsCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(443);
      match(DEV_INLINESTATS);
      setState(444);
      ((InlinestatsCommandContext)_localctx).stats = aggFields();
      setState(447);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,32,_ctx) ) {
      case 1:
        {
        setState(445);
        match(BY);
        setState(446);
        ((InlinestatsCommandContext)_localctx).grouping = fields();
        }
        break;
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ChangePointCommandContext extends ParserRuleContext {
    public QualifiedNameContext value;
    public QualifiedNameContext key;
    public QualifiedNameContext targetType;
    public QualifiedNameContext targetPvalue;
    public TerminalNode DEV_CHANGE_POINT() { return getToken(EsqlBaseParser.DEV_CHANGE_POINT, 0); }
    public List<QualifiedNameContext> qualifiedName() {
      return getRuleContexts(QualifiedNameContext.class);
    }
    public QualifiedNameContext qualifiedName(int i) {
      return getRuleContext(QualifiedNameContext.class,i);
    }
    public TerminalNode ON() { return getToken(EsqlBaseParser.ON, 0); }
    public TerminalNode AS() { return getToken(EsqlBaseParser.AS, 0); }
    public TerminalNode COMMA() { return getToken(EsqlBaseParser.COMMA, 0); }
    @SuppressWarnings("this-escape")
    public ChangePointCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_changePointCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterChangePointCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitChangePointCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitChangePointCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ChangePointCommandContext changePointCommand() throws RecognitionException {
    ChangePointCommandContext _localctx = new ChangePointCommandContext(_ctx, getState());
    enterRule(_localctx, 96, RULE_changePointCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(449);
      match(DEV_CHANGE_POINT);
      setState(450);
      ((ChangePointCommandContext)_localctx).value = qualifiedName();
      setState(453);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,33,_ctx) ) {
      case 1:
        {
        setState(451);
        match(ON);
        setState(452);
        ((ChangePointCommandContext)_localctx).key = qualifiedName();
        }
        break;
      }
      setState(460);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,34,_ctx) ) {
      case 1:
        {
        setState(455);
        match(AS);
        setState(456);
        ((ChangePointCommandContext)_localctx).targetType = qualifiedName();
        setState(457);
        match(COMMA);
        setState(458);
        ((ChangePointCommandContext)_localctx).targetPvalue = qualifiedName();
        }
        break;
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class InsistCommandContext extends ParserRuleContext {
    public TerminalNode DEV_INSIST() { return getToken(EsqlBaseParser.DEV_INSIST, 0); }
    public QualifiedNamePatternsContext qualifiedNamePatterns() {
      return getRuleContext(QualifiedNamePatternsContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public InsistCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_insistCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterInsistCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitInsistCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitInsistCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final InsistCommandContext insistCommand() throws RecognitionException {
    InsistCommandContext _localctx = new InsistCommandContext(_ctx, getState());
    enterRule(_localctx, 98, RULE_insistCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(462);
      match(DEV_INSIST);
      setState(463);
      qualifiedNamePatterns();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ForkCommandContext extends ParserRuleContext {
    public TerminalNode DEV_FORK() { return getToken(EsqlBaseParser.DEV_FORK, 0); }
    public ForkSubQueriesContext forkSubQueries() {
      return getRuleContext(ForkSubQueriesContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public ForkCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_forkCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterForkCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitForkCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitForkCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ForkCommandContext forkCommand() throws RecognitionException {
    ForkCommandContext _localctx = new ForkCommandContext(_ctx, getState());
    enterRule(_localctx, 100, RULE_forkCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(465);
      match(DEV_FORK);
      setState(466);
      forkSubQueries();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ForkSubQueriesContext extends ParserRuleContext {
    public List<ForkSubQueryContext> forkSubQuery() {
      return getRuleContexts(ForkSubQueryContext.class);
    }
    public ForkSubQueryContext forkSubQuery(int i) {
      return getRuleContext(ForkSubQueryContext.class,i);
    }
    @SuppressWarnings("this-escape")
    public ForkSubQueriesContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_forkSubQueries; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterForkSubQueries(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitForkSubQueries(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitForkSubQueries(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ForkSubQueriesContext forkSubQueries() throws RecognitionException {
    ForkSubQueriesContext _localctx = new ForkSubQueriesContext(_ctx, getState());
    enterRule(_localctx, 102, RULE_forkSubQueries);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(469); 
      _errHandler.sync(this);
      _alt = 1;
      do {
        switch (_alt) {
        case 1:
          {
          {
          setState(468);
          forkSubQuery();
          }
          }
          break;
        default:
          throw new NoViableAltException(this);
        }
        setState(471); 
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,35,_ctx);
      } while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ForkSubQueryContext extends ParserRuleContext {
    public TerminalNode LP() { return getToken(EsqlBaseParser.LP, 0); }
    public ForkSubQueryCommandContext forkSubQueryCommand() {
      return getRuleContext(ForkSubQueryCommandContext.class,0);
    }
    public TerminalNode RP() { return getToken(EsqlBaseParser.RP, 0); }
    @SuppressWarnings("this-escape")
    public ForkSubQueryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_forkSubQuery; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterForkSubQuery(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitForkSubQuery(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitForkSubQuery(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ForkSubQueryContext forkSubQuery() throws RecognitionException {
    ForkSubQueryContext _localctx = new ForkSubQueryContext(_ctx, getState());
    enterRule(_localctx, 104, RULE_forkSubQuery);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(473);
      match(LP);
      setState(474);
      forkSubQueryCommand(0);
      setState(475);
      match(RP);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ForkSubQueryCommandContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public ForkSubQueryCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_forkSubQueryCommand; }
   
    @SuppressWarnings("this-escape")
    public ForkSubQueryCommandContext() { }
    public void copyFrom(ForkSubQueryCommandContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class SingleForkSubQueryCommandContext extends ForkSubQueryCommandContext {
    public ForkSubQueryProcessingCommandContext forkSubQueryProcessingCommand() {
      return getRuleContext(ForkSubQueryProcessingCommandContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public SingleForkSubQueryCommandContext(ForkSubQueryCommandContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterSingleForkSubQueryCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitSingleForkSubQueryCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitSingleForkSubQueryCommand(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class CompositeForkSubQueryContext extends ForkSubQueryCommandContext {
    public ForkSubQueryCommandContext forkSubQueryCommand() {
      return getRuleContext(ForkSubQueryCommandContext.class,0);
    }
    public TerminalNode PIPE() { return getToken(EsqlBaseParser.PIPE, 0); }
    public ForkSubQueryProcessingCommandContext forkSubQueryProcessingCommand() {
      return getRuleContext(ForkSubQueryProcessingCommandContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public CompositeForkSubQueryContext(ForkSubQueryCommandContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterCompositeForkSubQuery(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitCompositeForkSubQuery(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitCompositeForkSubQuery(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ForkSubQueryCommandContext forkSubQueryCommand() throws RecognitionException {
    return forkSubQueryCommand(0);
  }

  private ForkSubQueryCommandContext forkSubQueryCommand(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    ForkSubQueryCommandContext _localctx = new ForkSubQueryCommandContext(_ctx, _parentState);
    ForkSubQueryCommandContext _prevctx = _localctx;
    int _startState = 106;
    enterRecursionRule(_localctx, 106, RULE_forkSubQueryCommand, _p);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      {
      _localctx = new SingleForkSubQueryCommandContext(_localctx);
      _ctx = _localctx;
      _prevctx = _localctx;

      setState(478);
      forkSubQueryProcessingCommand();
      }
      _ctx.stop = _input.LT(-1);
      setState(485);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,36,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          {
          _localctx = new CompositeForkSubQueryContext(new ForkSubQueryCommandContext(_parentctx, _parentState));
          pushNewRecursionContext(_localctx, _startState, RULE_forkSubQueryCommand);
          setState(480);
          if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(481);
          match(PIPE);
          setState(482);
          forkSubQueryProcessingCommand();
          }
          } 
        }
        setState(487);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,36,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ForkSubQueryProcessingCommandContext extends ParserRuleContext {
    public WhereCommandContext whereCommand() {
      return getRuleContext(WhereCommandContext.class,0);
    }
    public SortCommandContext sortCommand() {
      return getRuleContext(SortCommandContext.class,0);
    }
    public LimitCommandContext limitCommand() {
      return getRuleContext(LimitCommandContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public ForkSubQueryProcessingCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_forkSubQueryProcessingCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterForkSubQueryProcessingCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitForkSubQueryProcessingCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitForkSubQueryProcessingCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ForkSubQueryProcessingCommandContext forkSubQueryProcessingCommand() throws RecognitionException {
    ForkSubQueryProcessingCommandContext _localctx = new ForkSubQueryProcessingCommandContext(_ctx, getState());
    enterRule(_localctx, 108, RULE_forkSubQueryProcessingCommand);
    try {
      setState(491);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case WHERE:
        enterOuterAlt(_localctx, 1);
        {
        setState(488);
        whereCommand();
        }
        break;
      case SORT:
        enterOuterAlt(_localctx, 2);
        {
        setState(489);
        sortCommand();
        }
        break;
      case LIMIT:
        enterOuterAlt(_localctx, 3);
        {
        setState(490);
        limitCommand();
        }
        break;
      default:
        throw new NoViableAltException(this);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class RrfCommandContext extends ParserRuleContext {
    public TerminalNode DEV_RRF() { return getToken(EsqlBaseParser.DEV_RRF, 0); }
    @SuppressWarnings("this-escape")
    public RrfCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_rrfCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterRrfCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitRrfCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitRrfCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final RrfCommandContext rrfCommand() throws RecognitionException {
    RrfCommandContext _localctx = new RrfCommandContext(_ctx, getState());
    enterRule(_localctx, 110, RULE_rrfCommand);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(493);
      match(DEV_RRF);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class BooleanExpressionContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public BooleanExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_booleanExpression; }
   
    @SuppressWarnings("this-escape")
    public BooleanExpressionContext() { }
    public void copyFrom(BooleanExpressionContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class MatchExpressionContext extends BooleanExpressionContext {
    public MatchBooleanExpressionContext matchBooleanExpression() {
      return getRuleContext(MatchBooleanExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public MatchExpressionContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterMatchExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitMatchExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitMatchExpression(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class LogicalNotContext extends BooleanExpressionContext {
    public TerminalNode NOT() { return getToken(EsqlBaseParser.NOT, 0); }
    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public LogicalNotContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterLogicalNot(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitLogicalNot(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitLogicalNot(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class BooleanDefaultContext extends BooleanExpressionContext {
    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public BooleanDefaultContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterBooleanDefault(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitBooleanDefault(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitBooleanDefault(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class IsNullContext extends BooleanExpressionContext {
    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class,0);
    }
    public TerminalNode IS() { return getToken(EsqlBaseParser.IS, 0); }
    public TerminalNode NULL() { return getToken(EsqlBaseParser.NULL, 0); }
    public TerminalNode NOT() { return getToken(EsqlBaseParser.NOT, 0); }
    @SuppressWarnings("this-escape")
    public IsNullContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIsNull(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIsNull(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIsNull(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class RegexExpressionContext extends BooleanExpressionContext {
    public RegexBooleanExpressionContext regexBooleanExpression() {
      return getRuleContext(RegexBooleanExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public RegexExpressionContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterRegexExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitRegexExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitRegexExpression(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class LogicalInContext extends BooleanExpressionContext {
    public List<ValueExpressionContext> valueExpression() {
      return getRuleContexts(ValueExpressionContext.class);
    }
    public ValueExpressionContext valueExpression(int i) {
      return getRuleContext(ValueExpressionContext.class,i);
    }
    public TerminalNode IN() { return getToken(EsqlBaseParser.IN, 0); }
    public TerminalNode LP() { return getToken(EsqlBaseParser.LP, 0); }
    public TerminalNode RP() { return getToken(EsqlBaseParser.RP, 0); }
    public TerminalNode NOT() { return getToken(EsqlBaseParser.NOT, 0); }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public LogicalInContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterLogicalIn(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitLogicalIn(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitLogicalIn(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class LogicalBinaryContext extends BooleanExpressionContext {
    public BooleanExpressionContext left;
    public Token operator;
    public BooleanExpressionContext right;
    public List<BooleanExpressionContext> booleanExpression() {
      return getRuleContexts(BooleanExpressionContext.class);
    }
    public BooleanExpressionContext booleanExpression(int i) {
      return getRuleContext(BooleanExpressionContext.class,i);
    }
    public TerminalNode AND() { return getToken(EsqlBaseParser.AND, 0); }
    public TerminalNode OR() { return getToken(EsqlBaseParser.OR, 0); }
    @SuppressWarnings("this-escape")
    public LogicalBinaryContext(BooleanExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterLogicalBinary(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitLogicalBinary(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitLogicalBinary(this);
      else return visitor.visitChildren(this);
    }
  }

  public final BooleanExpressionContext booleanExpression() throws RecognitionException {
    return booleanExpression(0);
  }

  private BooleanExpressionContext booleanExpression(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    BooleanExpressionContext _localctx = new BooleanExpressionContext(_ctx, _parentState);
    BooleanExpressionContext _prevctx = _localctx;
    int _startState = 112;
    enterRecursionRule(_localctx, 112, RULE_booleanExpression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(524);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,41,_ctx) ) {
      case 1:
        {
        _localctx = new LogicalNotContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(496);
        match(NOT);
        setState(497);
        booleanExpression(8);
        }
        break;
      case 2:
        {
        _localctx = new BooleanDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(498);
        valueExpression();
        }
        break;
      case 3:
        {
        _localctx = new RegexExpressionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(499);
        regexBooleanExpression();
        }
        break;
      case 4:
        {
        _localctx = new LogicalInContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(500);
        valueExpression();
        setState(502);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(501);
          match(NOT);
          }
        }

        setState(504);
        match(IN);
        setState(505);
        match(LP);
        setState(506);
        valueExpression();
        setState(511);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(507);
          match(COMMA);
          setState(508);
          valueExpression();
          }
          }
          setState(513);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(514);
        match(RP);
        }
        break;
      case 5:
        {
        _localctx = new IsNullContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(516);
        valueExpression();
        setState(517);
        match(IS);
        setState(519);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(518);
          match(NOT);
          }
        }

        setState(521);
        match(NULL);
        }
        break;
      case 6:
        {
        _localctx = new MatchExpressionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(523);
        matchBooleanExpression();
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(534);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,43,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(532);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,42,_ctx) ) {
          case 1:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(526);
            if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
            setState(527);
            ((LogicalBinaryContext)_localctx).operator = match(AND);
            setState(528);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(6);
            }
            break;
          case 2:
            {
            _localctx = new LogicalBinaryContext(new BooleanExpressionContext(_parentctx, _parentState));
            ((LogicalBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
            setState(529);
            if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
            setState(530);
            ((LogicalBinaryContext)_localctx).operator = match(OR);
            setState(531);
            ((LogicalBinaryContext)_localctx).right = booleanExpression(5);
            }
            break;
          }
          } 
        }
        setState(536);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,43,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class RegexBooleanExpressionContext extends ParserRuleContext {
    public Token kind;
    public StringContext pattern;
    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class,0);
    }
    public TerminalNode LIKE() { return getToken(EsqlBaseParser.LIKE, 0); }
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    public TerminalNode NOT() { return getToken(EsqlBaseParser.NOT, 0); }
    public TerminalNode RLIKE() { return getToken(EsqlBaseParser.RLIKE, 0); }
    @SuppressWarnings("this-escape")
    public RegexBooleanExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_regexBooleanExpression; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterRegexBooleanExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitRegexBooleanExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitRegexBooleanExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final RegexBooleanExpressionContext regexBooleanExpression() throws RecognitionException {
    RegexBooleanExpressionContext _localctx = new RegexBooleanExpressionContext(_ctx, getState());
    enterRule(_localctx, 114, RULE_regexBooleanExpression);
    int _la;
    try {
      setState(551);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,46,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(537);
        valueExpression();
        setState(539);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(538);
          match(NOT);
          }
        }

        setState(541);
        ((RegexBooleanExpressionContext)_localctx).kind = match(LIKE);
        setState(542);
        ((RegexBooleanExpressionContext)_localctx).pattern = string();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(544);
        valueExpression();
        setState(546);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==NOT) {
          {
          setState(545);
          match(NOT);
          }
        }

        setState(548);
        ((RegexBooleanExpressionContext)_localctx).kind = match(RLIKE);
        setState(549);
        ((RegexBooleanExpressionContext)_localctx).pattern = string();
        }
        break;
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class MatchBooleanExpressionContext extends ParserRuleContext {
    public QualifiedNameContext fieldExp;
    public DataTypeContext fieldType;
    public ConstantContext matchQuery;
    public TerminalNode COLON() { return getToken(EsqlBaseParser.COLON, 0); }
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class,0);
    }
    public TerminalNode CAST_OP() { return getToken(EsqlBaseParser.CAST_OP, 0); }
    public DataTypeContext dataType() {
      return getRuleContext(DataTypeContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public MatchBooleanExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_matchBooleanExpression; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterMatchBooleanExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitMatchBooleanExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitMatchBooleanExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MatchBooleanExpressionContext matchBooleanExpression() throws RecognitionException {
    MatchBooleanExpressionContext _localctx = new MatchBooleanExpressionContext(_ctx, getState());
    enterRule(_localctx, 116, RULE_matchBooleanExpression);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(553);
      ((MatchBooleanExpressionContext)_localctx).fieldExp = qualifiedName();
      setState(556);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==CAST_OP) {
        {
        setState(554);
        match(CAST_OP);
        setState(555);
        ((MatchBooleanExpressionContext)_localctx).fieldType = dataType();
        }
      }

      setState(558);
      match(COLON);
      setState(559);
      ((MatchBooleanExpressionContext)_localctx).matchQuery = constant();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ValueExpressionContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public ValueExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_valueExpression; }
   
    @SuppressWarnings("this-escape")
    public ValueExpressionContext() { }
    public void copyFrom(ValueExpressionContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ValueExpressionDefaultContext extends ValueExpressionContext {
    public OperatorExpressionContext operatorExpression() {
      return getRuleContext(OperatorExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public ValueExpressionDefaultContext(ValueExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterValueExpressionDefault(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitValueExpressionDefault(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitValueExpressionDefault(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ComparisonContext extends ValueExpressionContext {
    public OperatorExpressionContext left;
    public OperatorExpressionContext right;
    public ComparisonOperatorContext comparisonOperator() {
      return getRuleContext(ComparisonOperatorContext.class,0);
    }
    public List<OperatorExpressionContext> operatorExpression() {
      return getRuleContexts(OperatorExpressionContext.class);
    }
    public OperatorExpressionContext operatorExpression(int i) {
      return getRuleContext(OperatorExpressionContext.class,i);
    }
    @SuppressWarnings("this-escape")
    public ComparisonContext(ValueExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterComparison(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitComparison(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitComparison(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ValueExpressionContext valueExpression() throws RecognitionException {
    ValueExpressionContext _localctx = new ValueExpressionContext(_ctx, getState());
    enterRule(_localctx, 118, RULE_valueExpression);
    try {
      setState(566);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,48,_ctx) ) {
      case 1:
        _localctx = new ValueExpressionDefaultContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(561);
        operatorExpression(0);
        }
        break;
      case 2:
        _localctx = new ComparisonContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(562);
        ((ComparisonContext)_localctx).left = operatorExpression(0);
        setState(563);
        comparisonOperator();
        setState(564);
        ((ComparisonContext)_localctx).right = operatorExpression(0);
        }
        break;
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class OperatorExpressionContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public OperatorExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_operatorExpression; }
   
    @SuppressWarnings("this-escape")
    public OperatorExpressionContext() { }
    public void copyFrom(OperatorExpressionContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class OperatorExpressionDefaultContext extends OperatorExpressionContext {
    public PrimaryExpressionContext primaryExpression() {
      return getRuleContext(PrimaryExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public OperatorExpressionDefaultContext(OperatorExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterOperatorExpressionDefault(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitOperatorExpressionDefault(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitOperatorExpressionDefault(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ArithmeticBinaryContext extends OperatorExpressionContext {
    public OperatorExpressionContext left;
    public Token operator;
    public OperatorExpressionContext right;
    public List<OperatorExpressionContext> operatorExpression() {
      return getRuleContexts(OperatorExpressionContext.class);
    }
    public OperatorExpressionContext operatorExpression(int i) {
      return getRuleContext(OperatorExpressionContext.class,i);
    }
    public TerminalNode ASTERISK() { return getToken(EsqlBaseParser.ASTERISK, 0); }
    public TerminalNode SLASH() { return getToken(EsqlBaseParser.SLASH, 0); }
    public TerminalNode PERCENT() { return getToken(EsqlBaseParser.PERCENT, 0); }
    public TerminalNode PLUS() { return getToken(EsqlBaseParser.PLUS, 0); }
    public TerminalNode MINUS() { return getToken(EsqlBaseParser.MINUS, 0); }
    @SuppressWarnings("this-escape")
    public ArithmeticBinaryContext(OperatorExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterArithmeticBinary(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitArithmeticBinary(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitArithmeticBinary(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ArithmeticUnaryContext extends OperatorExpressionContext {
    public Token operator;
    public OperatorExpressionContext operatorExpression() {
      return getRuleContext(OperatorExpressionContext.class,0);
    }
    public TerminalNode MINUS() { return getToken(EsqlBaseParser.MINUS, 0); }
    public TerminalNode PLUS() { return getToken(EsqlBaseParser.PLUS, 0); }
    @SuppressWarnings("this-escape")
    public ArithmeticUnaryContext(OperatorExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterArithmeticUnary(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitArithmeticUnary(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitArithmeticUnary(this);
      else return visitor.visitChildren(this);
    }
  }

  public final OperatorExpressionContext operatorExpression() throws RecognitionException {
    return operatorExpression(0);
  }

  private OperatorExpressionContext operatorExpression(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    OperatorExpressionContext _localctx = new OperatorExpressionContext(_ctx, _parentState);
    OperatorExpressionContext _prevctx = _localctx;
    int _startState = 120;
    enterRecursionRule(_localctx, 120, RULE_operatorExpression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(572);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,49,_ctx) ) {
      case 1:
        {
        _localctx = new OperatorExpressionDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(569);
        primaryExpression(0);
        }
        break;
      case 2:
        {
        _localctx = new ArithmeticUnaryContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(570);
        ((ArithmeticUnaryContext)_localctx).operator = _input.LT(1);
        _la = _input.LA(1);
        if ( !(_la==PLUS || _la==MINUS) ) {
          ((ArithmeticUnaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
        setState(571);
        operatorExpression(3);
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(582);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,51,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          setState(580);
          _errHandler.sync(this);
          switch ( getInterpreter().adaptivePredict(_input,50,_ctx) ) {
          case 1:
            {
            _localctx = new ArithmeticBinaryContext(new OperatorExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_operatorExpression);
            setState(574);
            if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
            setState(575);
            ((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
            _la = _input.LA(1);
            if ( !(((((_la - 86)) & ~0x3f) == 0 && ((1L << (_la - 86)) & 7L) != 0)) ) {
              ((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
            }
            else {
              if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
            setState(576);
            ((ArithmeticBinaryContext)_localctx).right = operatorExpression(3);
            }
            break;
          case 2:
            {
            _localctx = new ArithmeticBinaryContext(new OperatorExpressionContext(_parentctx, _parentState));
            ((ArithmeticBinaryContext)_localctx).left = _prevctx;
            pushNewRecursionContext(_localctx, _startState, RULE_operatorExpression);
            setState(577);
            if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
            setState(578);
            ((ArithmeticBinaryContext)_localctx).operator = _input.LT(1);
            _la = _input.LA(1);
            if ( !(_la==PLUS || _la==MINUS) ) {
              ((ArithmeticBinaryContext)_localctx).operator = (Token)_errHandler.recoverInline(this);
            }
            else {
              if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
            setState(579);
            ((ArithmeticBinaryContext)_localctx).right = operatorExpression(2);
            }
            break;
          }
          } 
        }
        setState(584);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,51,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class PrimaryExpressionContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public PrimaryExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_primaryExpression; }
   
    @SuppressWarnings("this-escape")
    public PrimaryExpressionContext() { }
    public void copyFrom(PrimaryExpressionContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class DereferenceContext extends PrimaryExpressionContext {
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public DereferenceContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterDereference(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitDereference(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitDereference(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class InlineCastContext extends PrimaryExpressionContext {
    public PrimaryExpressionContext primaryExpression() {
      return getRuleContext(PrimaryExpressionContext.class,0);
    }
    public TerminalNode CAST_OP() { return getToken(EsqlBaseParser.CAST_OP, 0); }
    public DataTypeContext dataType() {
      return getRuleContext(DataTypeContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public InlineCastContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterInlineCast(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitInlineCast(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitInlineCast(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ConstantDefaultContext extends PrimaryExpressionContext {
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public ConstantDefaultContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterConstantDefault(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitConstantDefault(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitConstantDefault(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class ParenthesizedExpressionContext extends PrimaryExpressionContext {
    public TerminalNode LP() { return getToken(EsqlBaseParser.LP, 0); }
    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class,0);
    }
    public TerminalNode RP() { return getToken(EsqlBaseParser.RP, 0); }
    @SuppressWarnings("this-escape")
    public ParenthesizedExpressionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterParenthesizedExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitParenthesizedExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitParenthesizedExpression(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class FunctionContext extends PrimaryExpressionContext {
    public FunctionExpressionContext functionExpression() {
      return getRuleContext(FunctionExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public FunctionContext(PrimaryExpressionContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterFunction(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitFunction(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitFunction(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PrimaryExpressionContext primaryExpression() throws RecognitionException {
    return primaryExpression(0);
  }

  private PrimaryExpressionContext primaryExpression(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    PrimaryExpressionContext _localctx = new PrimaryExpressionContext(_ctx, _parentState);
    PrimaryExpressionContext _prevctx = _localctx;
    int _startState = 122;
    enterRecursionRule(_localctx, 122, RULE_primaryExpression, _p);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(593);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,52,_ctx) ) {
      case 1:
        {
        _localctx = new ConstantDefaultContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;

        setState(586);
        constant();
        }
        break;
      case 2:
        {
        _localctx = new DereferenceContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(587);
        qualifiedName();
        }
        break;
      case 3:
        {
        _localctx = new FunctionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(588);
        functionExpression();
        }
        break;
      case 4:
        {
        _localctx = new ParenthesizedExpressionContext(_localctx);
        _ctx = _localctx;
        _prevctx = _localctx;
        setState(589);
        match(LP);
        setState(590);
        booleanExpression(0);
        setState(591);
        match(RP);
        }
        break;
      }
      _ctx.stop = _input.LT(-1);
      setState(600);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,53,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          if ( _parseListeners!=null ) triggerExitRuleEvent();
          _prevctx = _localctx;
          {
          {
          _localctx = new InlineCastContext(new PrimaryExpressionContext(_parentctx, _parentState));
          pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
          setState(595);
          if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
          setState(596);
          match(CAST_OP);
          setState(597);
          dataType();
          }
          } 
        }
        setState(602);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,53,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class FunctionExpressionContext extends ParserRuleContext {
    public FunctionNameContext functionName() {
      return getRuleContext(FunctionNameContext.class,0);
    }
    public TerminalNode LP() { return getToken(EsqlBaseParser.LP, 0); }
    public TerminalNode RP() { return getToken(EsqlBaseParser.RP, 0); }
    public TerminalNode ASTERISK() { return getToken(EsqlBaseParser.ASTERISK, 0); }
    public List<BooleanExpressionContext> booleanExpression() {
      return getRuleContexts(BooleanExpressionContext.class);
    }
    public BooleanExpressionContext booleanExpression(int i) {
      return getRuleContext(BooleanExpressionContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    public MapExpressionContext mapExpression() {
      return getRuleContext(MapExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public FunctionExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_functionExpression; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterFunctionExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitFunctionExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitFunctionExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FunctionExpressionContext functionExpression() throws RecognitionException {
    FunctionExpressionContext _localctx = new FunctionExpressionContext(_ctx, getState());
    enterRule(_localctx, 124, RULE_functionExpression);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(603);
      functionName();
      setState(604);
      match(LP);
      setState(618);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
      case ASTERISK:
        {
        setState(605);
        match(ASTERISK);
        }
        break;
      case QUOTED_STRING:
      case INTEGER_LITERAL:
      case DECIMAL_LITERAL:
      case FALSE:
      case NOT:
      case NULL:
      case PARAM:
      case TRUE:
      case PLUS:
      case MINUS:
      case DOUBLE_PARAMS:
      case NAMED_OR_POSITIONAL_PARAM:
      case NAMED_OR_POSITIONAL_DOUBLE_PARAMS:
      case OPENING_BRACKET:
      case LP:
      case UNQUOTED_IDENTIFIER:
      case QUOTED_IDENTIFIER:
        {
        {
        setState(606);
        booleanExpression(0);
        setState(611);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,54,_ctx);
        while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
          if ( _alt==1 ) {
            {
            {
            setState(607);
            match(COMMA);
            setState(608);
            booleanExpression(0);
            }
            } 
          }
          setState(613);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input,54,_ctx);
        }
        setState(616);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la==COMMA) {
          {
          setState(614);
          match(COMMA);
          setState(615);
          mapExpression();
          }
        }

        }
        }
        break;
      case RP:
        break;
      default:
        break;
      }
      setState(620);
      match(RP);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class FunctionNameContext extends ParserRuleContext {
    public IdentifierOrParameterContext identifierOrParameter() {
      return getRuleContext(IdentifierOrParameterContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public FunctionNameContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_functionName; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterFunctionName(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitFunctionName(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitFunctionName(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FunctionNameContext functionName() throws RecognitionException {
    FunctionNameContext _localctx = new FunctionNameContext(_ctx, getState());
    enterRule(_localctx, 126, RULE_functionName);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(622);
      identifierOrParameter();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class MapExpressionContext extends ParserRuleContext {
    public TerminalNode LEFT_BRACES() { return getToken(EsqlBaseParser.LEFT_BRACES, 0); }
    public List<EntryExpressionContext> entryExpression() {
      return getRuleContexts(EntryExpressionContext.class);
    }
    public EntryExpressionContext entryExpression(int i) {
      return getRuleContext(EntryExpressionContext.class,i);
    }
    public TerminalNode RIGHT_BRACES() { return getToken(EsqlBaseParser.RIGHT_BRACES, 0); }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public MapExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_mapExpression; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterMapExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitMapExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitMapExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MapExpressionContext mapExpression() throws RecognitionException {
    MapExpressionContext _localctx = new MapExpressionContext(_ctx, getState());
    enterRule(_localctx, 128, RULE_mapExpression);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(624);
      match(LEFT_BRACES);
      setState(625);
      entryExpression();
      setState(630);
      _errHandler.sync(this);
      _la = _input.LA(1);
      while (_la==COMMA) {
        {
        {
        setState(626);
        match(COMMA);
        setState(627);
        entryExpression();
        }
        }
        setState(632);
        _errHandler.sync(this);
        _la = _input.LA(1);
      }
      setState(633);
      match(RIGHT_BRACES);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class EntryExpressionContext extends ParserRuleContext {
    public StringContext key;
    public ConstantContext value;
    public TerminalNode COLON() { return getToken(EsqlBaseParser.COLON, 0); }
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public EntryExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_entryExpression; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterEntryExpression(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitEntryExpression(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitEntryExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final EntryExpressionContext entryExpression() throws RecognitionException {
    EntryExpressionContext _localctx = new EntryExpressionContext(_ctx, getState());
    enterRule(_localctx, 130, RULE_entryExpression);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(635);
      ((EntryExpressionContext)_localctx).key = string();
      setState(636);
      match(COLON);
      setState(637);
      ((EntryExpressionContext)_localctx).value = constant();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ConstantContext extends ParserRuleContext {
    @SuppressWarnings("this-escape")
    public ConstantContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_constant; }
   
    @SuppressWarnings("this-escape")
    public ConstantContext() { }
    public void copyFrom(ConstantContext ctx) {
      super.copyFrom(ctx);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class BooleanArrayLiteralContext extends ConstantContext {
    public TerminalNode OPENING_BRACKET() { return getToken(EsqlBaseParser.OPENING_BRACKET, 0); }
    public List<BooleanValueContext> booleanValue() {
      return getRuleContexts(BooleanValueContext.class);
    }
    public BooleanValueContext booleanValue(int i) {
      return getRuleContext(BooleanValueContext.class,i);
    }
    public TerminalNode CLOSING_BRACKET() { return getToken(EsqlBaseParser.CLOSING_BRACKET, 0); }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public BooleanArrayLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterBooleanArrayLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitBooleanArrayLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitBooleanArrayLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class DecimalLiteralContext extends ConstantContext {
    public DecimalValueContext decimalValue() {
      return getRuleContext(DecimalValueContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public DecimalLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterDecimalLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitDecimalLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitDecimalLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class NullLiteralContext extends ConstantContext {
    public TerminalNode NULL() { return getToken(EsqlBaseParser.NULL, 0); }
    @SuppressWarnings("this-escape")
    public NullLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterNullLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitNullLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitNullLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class QualifiedIntegerLiteralContext extends ConstantContext {
    public IntegerValueContext integerValue() {
      return getRuleContext(IntegerValueContext.class,0);
    }
    public TerminalNode UNQUOTED_IDENTIFIER() { return getToken(EsqlBaseParser.UNQUOTED_IDENTIFIER, 0); }
    @SuppressWarnings("this-escape")
    public QualifiedIntegerLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterQualifiedIntegerLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitQualifiedIntegerLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitQualifiedIntegerLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class StringArrayLiteralContext extends ConstantContext {
    public TerminalNode OPENING_BRACKET() { return getToken(EsqlBaseParser.OPENING_BRACKET, 0); }
    public List<StringContext> string() {
      return getRuleContexts(StringContext.class);
    }
    public StringContext string(int i) {
      return getRuleContext(StringContext.class,i);
    }
    public TerminalNode CLOSING_BRACKET() { return getToken(EsqlBaseParser.CLOSING_BRACKET, 0); }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public StringArrayLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterStringArrayLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitStringArrayLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitStringArrayLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class InputParameterContext extends ConstantContext {
    public ParameterContext parameter() {
      return getRuleContext(ParameterContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public InputParameterContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterInputParameter(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitInputParameter(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitInputParameter(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class StringLiteralContext extends ConstantContext {
    public StringContext string() {
      return getRuleContext(StringContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public StringLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterStringLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitStringLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitStringLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class NumericArrayLiteralContext extends ConstantContext {
    public TerminalNode OPENING_BRACKET() { return getToken(EsqlBaseParser.OPENING_BRACKET, 0); }
    public List<NumericValueContext> numericValue() {
      return getRuleContexts(NumericValueContext.class);
    }
    public NumericValueContext numericValue(int i) {
      return getRuleContext(NumericValueContext.class,i);
    }
    public TerminalNode CLOSING_BRACKET() { return getToken(EsqlBaseParser.CLOSING_BRACKET, 0); }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public NumericArrayLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterNumericArrayLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitNumericArrayLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitNumericArrayLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class IntegerLiteralContext extends ConstantContext {
    public IntegerValueContext integerValue() {
      return getRuleContext(IntegerValueContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public IntegerLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIntegerLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIntegerLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIntegerLiteral(this);
      else return visitor.visitChildren(this);
    }
  }
  @SuppressWarnings("CheckReturnValue")
  public static class BooleanLiteralContext extends ConstantContext {
    public BooleanValueContext booleanValue() {
      return getRuleContext(BooleanValueContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public BooleanLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterBooleanLiteral(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitBooleanLiteral(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitBooleanLiteral(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ConstantContext constant() throws RecognitionException {
    ConstantContext _localctx = new ConstantContext(_ctx, getState());
    enterRule(_localctx, 132, RULE_constant);
    int _la;
    try {
      setState(681);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,61,_ctx) ) {
      case 1:
        _localctx = new NullLiteralContext(_localctx);
        enterOuterAlt(_localctx, 1);
        {
        setState(639);
        match(NULL);
        }
        break;
      case 2:
        _localctx = new QualifiedIntegerLiteralContext(_localctx);
        enterOuterAlt(_localctx, 2);
        {
        setState(640);
        integerValue();
        setState(641);
        match(UNQUOTED_IDENTIFIER);
        }
        break;
      case 3:
        _localctx = new DecimalLiteralContext(_localctx);
        enterOuterAlt(_localctx, 3);
        {
        setState(643);
        decimalValue();
        }
        break;
      case 4:
        _localctx = new IntegerLiteralContext(_localctx);
        enterOuterAlt(_localctx, 4);
        {
        setState(644);
        integerValue();
        }
        break;
      case 5:
        _localctx = new BooleanLiteralContext(_localctx);
        enterOuterAlt(_localctx, 5);
        {
        setState(645);
        booleanValue();
        }
        break;
      case 6:
        _localctx = new InputParameterContext(_localctx);
        enterOuterAlt(_localctx, 6);
        {
        setState(646);
        parameter();
        }
        break;
      case 7:
        _localctx = new StringLiteralContext(_localctx);
        enterOuterAlt(_localctx, 7);
        {
        setState(647);
        string();
        }
        break;
      case 8:
        _localctx = new NumericArrayLiteralContext(_localctx);
        enterOuterAlt(_localctx, 8);
        {
        setState(648);
        match(OPENING_BRACKET);
        setState(649);
        numericValue();
        setState(654);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(650);
          match(COMMA);
          setState(651);
          numericValue();
          }
          }
          setState(656);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(657);
        match(CLOSING_BRACKET);
        }
        break;
      case 9:
        _localctx = new BooleanArrayLiteralContext(_localctx);
        enterOuterAlt(_localctx, 9);
        {
        setState(659);
        match(OPENING_BRACKET);
        setState(660);
        booleanValue();
        setState(665);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(661);
          match(COMMA);
          setState(662);
          booleanValue();
          }
          }
          setState(667);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(668);
        match(CLOSING_BRACKET);
        }
        break;
      case 10:
        _localctx = new StringArrayLiteralContext(_localctx);
        enterOuterAlt(_localctx, 10);
        {
        setState(670);
        match(OPENING_BRACKET);
        setState(671);
        string();
        setState(676);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la==COMMA) {
          {
          {
          setState(672);
          match(COMMA);
          setState(673);
          string();
          }
          }
          setState(678);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(679);
        match(CLOSING_BRACKET);
        }
        break;
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class BooleanValueContext extends ParserRuleContext {
    public TerminalNode TRUE() { return getToken(EsqlBaseParser.TRUE, 0); }
    public TerminalNode FALSE() { return getToken(EsqlBaseParser.FALSE, 0); }
    @SuppressWarnings("this-escape")
    public BooleanValueContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_booleanValue; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterBooleanValue(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitBooleanValue(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitBooleanValue(this);
      else return visitor.visitChildren(this);
    }
  }

  public final BooleanValueContext booleanValue() throws RecognitionException {
    BooleanValueContext _localctx = new BooleanValueContext(_ctx, getState());
    enterRule(_localctx, 134, RULE_booleanValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(683);
      _la = _input.LA(1);
      if ( !(_la==FALSE || _la==TRUE) ) {
      _errHandler.recoverInline(this);
      }
      else {
        if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
        _errHandler.reportMatch(this);
        consume();
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class NumericValueContext extends ParserRuleContext {
    public DecimalValueContext decimalValue() {
      return getRuleContext(DecimalValueContext.class,0);
    }
    public IntegerValueContext integerValue() {
      return getRuleContext(IntegerValueContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public NumericValueContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_numericValue; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterNumericValue(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitNumericValue(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitNumericValue(this);
      else return visitor.visitChildren(this);
    }
  }

  public final NumericValueContext numericValue() throws RecognitionException {
    NumericValueContext _localctx = new NumericValueContext(_ctx, getState());
    enterRule(_localctx, 136, RULE_numericValue);
    try {
      setState(687);
      _errHandler.sync(this);
      switch ( getInterpreter().adaptivePredict(_input,62,_ctx) ) {
      case 1:
        enterOuterAlt(_localctx, 1);
        {
        setState(685);
        decimalValue();
        }
        break;
      case 2:
        enterOuterAlt(_localctx, 2);
        {
        setState(686);
        integerValue();
        }
        break;
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class DecimalValueContext extends ParserRuleContext {
    public TerminalNode DECIMAL_LITERAL() { return getToken(EsqlBaseParser.DECIMAL_LITERAL, 0); }
    public TerminalNode PLUS() { return getToken(EsqlBaseParser.PLUS, 0); }
    public TerminalNode MINUS() { return getToken(EsqlBaseParser.MINUS, 0); }
    @SuppressWarnings("this-escape")
    public DecimalValueContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_decimalValue; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterDecimalValue(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitDecimalValue(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitDecimalValue(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DecimalValueContext decimalValue() throws RecognitionException {
    DecimalValueContext _localctx = new DecimalValueContext(_ctx, getState());
    enterRule(_localctx, 138, RULE_decimalValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(690);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==PLUS || _la==MINUS) {
        {
        setState(689);
        _la = _input.LA(1);
        if ( !(_la==PLUS || _la==MINUS) ) {
        _errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
        }
      }

      setState(692);
      match(DECIMAL_LITERAL);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class IntegerValueContext extends ParserRuleContext {
    public TerminalNode INTEGER_LITERAL() { return getToken(EsqlBaseParser.INTEGER_LITERAL, 0); }
    public TerminalNode PLUS() { return getToken(EsqlBaseParser.PLUS, 0); }
    public TerminalNode MINUS() { return getToken(EsqlBaseParser.MINUS, 0); }
    @SuppressWarnings("this-escape")
    public IntegerValueContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_integerValue; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterIntegerValue(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitIntegerValue(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitIntegerValue(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IntegerValueContext integerValue() throws RecognitionException {
    IntegerValueContext _localctx = new IntegerValueContext(_ctx, getState());
    enterRule(_localctx, 140, RULE_integerValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(695);
      _errHandler.sync(this);
      _la = _input.LA(1);
      if (_la==PLUS || _la==MINUS) {
        {
        setState(694);
        _la = _input.LA(1);
        if ( !(_la==PLUS || _la==MINUS) ) {
        _errHandler.recoverInline(this);
        }
        else {
          if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
        }
      }

      setState(697);
      match(INTEGER_LITERAL);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class StringContext extends ParserRuleContext {
    public TerminalNode QUOTED_STRING() { return getToken(EsqlBaseParser.QUOTED_STRING, 0); }
    @SuppressWarnings("this-escape")
    public StringContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_string; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterString(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitString(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitString(this);
      else return visitor.visitChildren(this);
    }
  }

  public final StringContext string() throws RecognitionException {
    StringContext _localctx = new StringContext(_ctx, getState());
    enterRule(_localctx, 142, RULE_string);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(699);
      match(QUOTED_STRING);
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class ComparisonOperatorContext extends ParserRuleContext {
    public TerminalNode EQ() { return getToken(EsqlBaseParser.EQ, 0); }
    public TerminalNode NEQ() { return getToken(EsqlBaseParser.NEQ, 0); }
    public TerminalNode LT() { return getToken(EsqlBaseParser.LT, 0); }
    public TerminalNode LTE() { return getToken(EsqlBaseParser.LTE, 0); }
    public TerminalNode GT() { return getToken(EsqlBaseParser.GT, 0); }
    public TerminalNode GTE() { return getToken(EsqlBaseParser.GTE, 0); }
    @SuppressWarnings("this-escape")
    public ComparisonOperatorContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_comparisonOperator; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterComparisonOperator(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitComparisonOperator(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitComparisonOperator(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ComparisonOperatorContext comparisonOperator() throws RecognitionException {
    ComparisonOperatorContext _localctx = new ComparisonOperatorContext(_ctx, getState());
    enterRule(_localctx, 144, RULE_comparisonOperator);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(701);
      _la = _input.LA(1);
      if ( !(((((_la - 77)) & ~0x3f) == 0 && ((1L << (_la - 77)) & 125L) != 0)) ) {
      _errHandler.recoverInline(this);
      }
      else {
        if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
        _errHandler.reportMatch(this);
        consume();
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class JoinCommandContext extends ParserRuleContext {
    public Token type;
    public TerminalNode JOIN() { return getToken(EsqlBaseParser.JOIN, 0); }
    public JoinTargetContext joinTarget() {
      return getRuleContext(JoinTargetContext.class,0);
    }
    public JoinConditionContext joinCondition() {
      return getRuleContext(JoinConditionContext.class,0);
    }
    public TerminalNode JOIN_LOOKUP() { return getToken(EsqlBaseParser.JOIN_LOOKUP, 0); }
    public TerminalNode DEV_JOIN_LEFT() { return getToken(EsqlBaseParser.DEV_JOIN_LEFT, 0); }
    public TerminalNode DEV_JOIN_RIGHT() { return getToken(EsqlBaseParser.DEV_JOIN_RIGHT, 0); }
    @SuppressWarnings("this-escape")
    public JoinCommandContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_joinCommand; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterJoinCommand(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitJoinCommand(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitJoinCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public final JoinCommandContext joinCommand() throws RecognitionException {
    JoinCommandContext _localctx = new JoinCommandContext(_ctx, getState());
    enterRule(_localctx, 146, RULE_joinCommand);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(703);
      ((JoinCommandContext)_localctx).type = _input.LT(1);
      _la = _input.LA(1);
      if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & 6815744L) != 0)) ) {
        ((JoinCommandContext)_localctx).type = (Token)_errHandler.recoverInline(this);
      }
      else {
        if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
        _errHandler.reportMatch(this);
        consume();
      }
      setState(704);
      match(JOIN);
      setState(705);
      joinTarget();
      setState(706);
      joinCondition();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class JoinTargetContext extends ParserRuleContext {
    public IndexPatternContext index;
    public IndexPatternContext indexPattern() {
      return getRuleContext(IndexPatternContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public JoinTargetContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_joinTarget; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterJoinTarget(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitJoinTarget(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitJoinTarget(this);
      else return visitor.visitChildren(this);
    }
  }

  public final JoinTargetContext joinTarget() throws RecognitionException {
    JoinTargetContext _localctx = new JoinTargetContext(_ctx, getState());
    enterRule(_localctx, 148, RULE_joinTarget);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(708);
      ((JoinTargetContext)_localctx).index = indexPattern();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class JoinConditionContext extends ParserRuleContext {
    public TerminalNode ON() { return getToken(EsqlBaseParser.ON, 0); }
    public List<JoinPredicateContext> joinPredicate() {
      return getRuleContexts(JoinPredicateContext.class);
    }
    public JoinPredicateContext joinPredicate(int i) {
      return getRuleContext(JoinPredicateContext.class,i);
    }
    public List<TerminalNode> COMMA() { return getTokens(EsqlBaseParser.COMMA); }
    public TerminalNode COMMA(int i) {
      return getToken(EsqlBaseParser.COMMA, i);
    }
    @SuppressWarnings("this-escape")
    public JoinConditionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_joinCondition; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterJoinCondition(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitJoinCondition(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitJoinCondition(this);
      else return visitor.visitChildren(this);
    }
  }

  public final JoinConditionContext joinCondition() throws RecognitionException {
    JoinConditionContext _localctx = new JoinConditionContext(_ctx, getState());
    enterRule(_localctx, 150, RULE_joinCondition);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
      setState(710);
      match(ON);
      setState(711);
      joinPredicate();
      setState(716);
      _errHandler.sync(this);
      _alt = getInterpreter().adaptivePredict(_input,65,_ctx);
      while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
        if ( _alt==1 ) {
          {
          {
          setState(712);
          match(COMMA);
          setState(713);
          joinPredicate();
          }
          } 
        }
        setState(718);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input,65,_ctx);
      }
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  @SuppressWarnings("CheckReturnValue")
  public static class JoinPredicateContext extends ParserRuleContext {
    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class,0);
    }
    @SuppressWarnings("this-escape")
    public JoinPredicateContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }
    @Override public int getRuleIndex() { return RULE_joinPredicate; }
    @Override
    public void enterRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).enterJoinPredicate(this);
    }
    @Override
    public void exitRule(ParseTreeListener listener) {
      if ( listener instanceof EsqlBaseParserListener ) ((EsqlBaseParserListener)listener).exitJoinPredicate(this);
    }
    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if ( visitor instanceof EsqlBaseParserVisitor ) return ((EsqlBaseParserVisitor<? extends T>)visitor).visitJoinPredicate(this);
      else return visitor.visitChildren(this);
    }
  }

  public final JoinPredicateContext joinPredicate() throws RecognitionException {
    JoinPredicateContext _localctx = new JoinPredicateContext(_ctx, getState());
    enterRule(_localctx, 152, RULE_joinPredicate);
    try {
      enterOuterAlt(_localctx, 1);
      {
      setState(719);
      valueExpression();
      }
    }
    catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    }
    finally {
      exitRule();
    }
    return _localctx;
  }

  public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
    switch (ruleIndex) {
    case 1:
      return query_sempred((QueryContext)_localctx, predIndex);
    case 2:
      return sourceCommand_sempred((SourceCommandContext)_localctx, predIndex);
    case 3:
      return processingCommand_sempred((ProcessingCommandContext)_localctx, predIndex);
    case 12:
      return indexPattern_sempred((IndexPatternContext)_localctx, predIndex);
    case 53:
      return forkSubQueryCommand_sempred((ForkSubQueryCommandContext)_localctx, predIndex);
    case 56:
      return booleanExpression_sempred((BooleanExpressionContext)_localctx, predIndex);
    case 60:
      return operatorExpression_sempred((OperatorExpressionContext)_localctx, predIndex);
    case 61:
      return primaryExpression_sempred((PrimaryExpressionContext)_localctx, predIndex);
    }
    return true;
  }
  private boolean query_sempred(QueryContext _localctx, int predIndex) {
    switch (predIndex) {
    case 0:
      return precpred(_ctx, 1);
    }
    return true;
  }
  private boolean sourceCommand_sempred(SourceCommandContext _localctx, int predIndex) {
    switch (predIndex) {
    case 1:
      return this.isDevVersion();
    }
    return true;
  }
  private boolean processingCommand_sempred(ProcessingCommandContext _localctx, int predIndex) {
    switch (predIndex) {
    case 2:
      return this.isDevVersion();
    case 3:
      return this.isDevVersion();
    case 4:
      return this.isDevVersion();
    case 5:
      return this.isDevVersion();
    case 6:
      return this.isDevVersion();
    case 7:
      return this.isDevVersion();
    }
    return true;
  }
  private boolean indexPattern_sempred(IndexPatternContext _localctx, int predIndex) {
    switch (predIndex) {
    case 8:
      return this.isDevVersion();
    }
    return true;
  }
  private boolean forkSubQueryCommand_sempred(ForkSubQueryCommandContext _localctx, int predIndex) {
    switch (predIndex) {
    case 9:
      return precpred(_ctx, 1);
    }
    return true;
  }
  private boolean booleanExpression_sempred(BooleanExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
    case 10:
      return precpred(_ctx, 5);
    case 11:
      return precpred(_ctx, 4);
    }
    return true;
  }
  private boolean operatorExpression_sempred(OperatorExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
    case 12:
      return precpred(_ctx, 2);
    case 13:
      return precpred(_ctx, 1);
    }
    return true;
  }
  private boolean primaryExpression_sempred(PrimaryExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
    case 14:
      return precpred(_ctx, 1);
    }
    return true;
  }

  public static final String _serializedATN =
    "\u0004\u0001\u0088\u02d2\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001"+
    "\u0002\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004"+
    "\u0002\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007"+
    "\u0002\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b"+
    "\u0002\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002\u000f\u0007"+
    "\u000f\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002\u0012\u0007"+
    "\u0012\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0002\u0015\u0007"+
    "\u0015\u0002\u0016\u0007\u0016\u0002\u0017\u0007\u0017\u0002\u0018\u0007"+
    "\u0018\u0002\u0019\u0007\u0019\u0002\u001a\u0007\u001a\u0002\u001b\u0007"+
    "\u001b\u0002\u001c\u0007\u001c\u0002\u001d\u0007\u001d\u0002\u001e\u0007"+
    "\u001e\u0002\u001f\u0007\u001f\u0002 \u0007 \u0002!\u0007!\u0002\"\u0007"+
    "\"\u0002#\u0007#\u0002$\u0007$\u0002%\u0007%\u0002&\u0007&\u0002\'\u0007"+
    "\'\u0002(\u0007(\u0002)\u0007)\u0002*\u0007*\u0002+\u0007+\u0002,\u0007"+
    ",\u0002-\u0007-\u0002.\u0007.\u0002/\u0007/\u00020\u00070\u00021\u0007"+
    "1\u00022\u00072\u00023\u00073\u00024\u00074\u00025\u00075\u00026\u0007"+
    "6\u00027\u00077\u00028\u00078\u00029\u00079\u0002:\u0007:\u0002;\u0007"+
    ";\u0002<\u0007<\u0002=\u0007=\u0002>\u0007>\u0002?\u0007?\u0002@\u0007"+
    "@\u0002A\u0007A\u0002B\u0007B\u0002C\u0007C\u0002D\u0007D\u0002E\u0007"+
    "E\u0002F\u0007F\u0002G\u0007G\u0002H\u0007H\u0002I\u0007I\u0002J\u0007"+
    "J\u0002K\u0007K\u0002L\u0007L\u0001\u0000\u0001\u0000\u0001\u0000\u0001"+
    "\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0005"+
    "\u0001\u00a4\b\u0001\n\u0001\f\u0001\u00a7\t\u0001\u0001\u0002\u0001\u0002"+
    "\u0001\u0002\u0001\u0002\u0001\u0002\u0001\u0002\u0003\u0002\u00af\b\u0002"+
    "\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
    "\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
    "\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
    "\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
    "\u0001\u0003\u0003\u0003\u00ca\b\u0003\u0001\u0004\u0001\u0004\u0001\u0004"+
    "\u0001\u0005\u0001\u0005\u0001\u0006\u0001\u0006\u0001\u0006\u0001\u0007"+
    "\u0001\u0007\u0001\u0007\u0005\u0007\u00d7\b\u0007\n\u0007\f\u0007\u00da"+
    "\t\u0007\u0001\b\u0001\b\u0001\b\u0003\b\u00df\b\b\u0001\b\u0001\b\u0001"+
    "\t\u0001\t\u0001\t\u0001\n\u0001\n\u0001\n\u0001\u000b\u0001\u000b\u0001"+
    "\u000b\u0005\u000b\u00ec\b\u000b\n\u000b\f\u000b\u00ef\t\u000b\u0001\u000b"+
    "\u0003\u000b\u00f2\b\u000b\u0001\f\u0001\f\u0001\f\u0003\f\u00f7\b\f\u0001"+
    "\f\u0001\f\u0001\f\u0001\f\u0001\f\u0003\f\u00fe\b\f\u0003\f\u0100\b\f"+
    "\u0001\r\u0001\r\u0001\u000e\u0001\u000e\u0001\u000f\u0001\u000f\u0001"+
    "\u0010\u0001\u0010\u0001\u0010\u0001\u0010\u0005\u0010\u010c\b\u0010\n"+
    "\u0010\f\u0010\u010f\t\u0010\u0001\u0011\u0001\u0011\u0001\u0011\u0001"+
    "\u0012\u0001\u0012\u0003\u0012\u0116\b\u0012\u0001\u0012\u0001\u0012\u0003"+
    "\u0012\u011a\b\u0012\u0001\u0013\u0001\u0013\u0001\u0013\u0005\u0013\u011f"+
    "\b\u0013\n\u0013\f\u0013\u0122\t\u0013\u0001\u0014\u0001\u0014\u0001\u0014"+
    "\u0003\u0014\u0127\b\u0014\u0001\u0015\u0001\u0015\u0001\u0015\u0005\u0015"+
    "\u012c\b\u0015\n\u0015\f\u0015\u012f\t\u0015\u0001\u0016\u0001\u0016\u0001"+
    "\u0016\u0005\u0016\u0134\b\u0016\n\u0016\f\u0016\u0137\t\u0016\u0001\u0017"+
    "\u0001\u0017\u0001\u0017\u0005\u0017\u013c\b\u0017\n\u0017\f\u0017\u013f"+
    "\t\u0017\u0001\u0018\u0001\u0018\u0001\u0019\u0001\u0019\u0001\u0019\u0003"+
    "\u0019\u0146\b\u0019\u0001\u001a\u0001\u001a\u0003\u001a\u014a\b\u001a"+
    "\u0001\u001b\u0001\u001b\u0003\u001b\u014e\b\u001b\u0001\u001c\u0001\u001c"+
    "\u0001\u001c\u0003\u001c\u0153\b\u001c\u0001\u001d\u0001\u001d\u0001\u001d"+
    "\u0001\u001e\u0001\u001e\u0001\u001e\u0001\u001e\u0005\u001e\u015c\b\u001e"+
    "\n\u001e\f\u001e\u015f\t\u001e\u0001\u001f\u0001\u001f\u0003\u001f\u0163"+
    "\b\u001f\u0001\u001f\u0001\u001f\u0003\u001f\u0167\b\u001f\u0001 \u0001"+
    " \u0001 \u0001!\u0001!\u0001!\u0001\"\u0001\"\u0001\"\u0001\"\u0005\""+
    "\u0173\b\"\n\"\f\"\u0176\t\"\u0001#\u0001#\u0001#\u0001#\u0001$\u0001"+
    "$\u0001$\u0001$\u0003$\u0180\b$\u0001%\u0001%\u0001%\u0001%\u0001&\u0001"+
    "&\u0001&\u0001\'\u0001\'\u0001\'\u0005\'\u018c\b\'\n\'\f\'\u018f\t\'\u0001"+
    "(\u0001(\u0001(\u0001(\u0001)\u0001)\u0001)\u0001*\u0001*\u0001*\u0001"+
    "*\u0001+\u0001+\u0001+\u0001,\u0001,\u0001,\u0001,\u0003,\u01a3\b,\u0001"+
    ",\u0001,\u0001,\u0001,\u0005,\u01a9\b,\n,\f,\u01ac\t,\u0003,\u01ae\b,"+
    "\u0001-\u0001-\u0001-\u0003-\u01b3\b-\u0001-\u0001-\u0001.\u0001.\u0001"+
    ".\u0001.\u0001.\u0001/\u0001/\u0001/\u0001/\u0003/\u01c0\b/\u00010\u0001"+
    "0\u00010\u00010\u00030\u01c6\b0\u00010\u00010\u00010\u00010\u00010\u0003"+
    "0\u01cd\b0\u00011\u00011\u00011\u00012\u00012\u00012\u00013\u00043\u01d6"+
    "\b3\u000b3\f3\u01d7\u00014\u00014\u00014\u00014\u00015\u00015\u00015\u0001"+
    "5\u00015\u00015\u00055\u01e4\b5\n5\f5\u01e7\t5\u00016\u00016\u00016\u0003"+
    "6\u01ec\b6\u00017\u00017\u00018\u00018\u00018\u00018\u00018\u00018\u0001"+
    "8\u00038\u01f7\b8\u00018\u00018\u00018\u00018\u00018\u00058\u01fe\b8\n"+
    "8\f8\u0201\t8\u00018\u00018\u00018\u00018\u00018\u00038\u0208\b8\u0001"+
    "8\u00018\u00018\u00038\u020d\b8\u00018\u00018\u00018\u00018\u00018\u0001"+
    "8\u00058\u0215\b8\n8\f8\u0218\t8\u00019\u00019\u00039\u021c\b9\u00019"+
    "\u00019\u00019\u00019\u00019\u00039\u0223\b9\u00019\u00019\u00019\u0003"+
    "9\u0228\b9\u0001:\u0001:\u0001:\u0003:\u022d\b:\u0001:\u0001:\u0001:\u0001"+
    ";\u0001;\u0001;\u0001;\u0001;\u0003;\u0237\b;\u0001<\u0001<\u0001<\u0001"+
    "<\u0003<\u023d\b<\u0001<\u0001<\u0001<\u0001<\u0001<\u0001<\u0005<\u0245"+
    "\b<\n<\f<\u0248\t<\u0001=\u0001=\u0001=\u0001=\u0001=\u0001=\u0001=\u0001"+
    "=\u0003=\u0252\b=\u0001=\u0001=\u0001=\u0005=\u0257\b=\n=\f=\u025a\t="+
    "\u0001>\u0001>\u0001>\u0001>\u0001>\u0001>\u0005>\u0262\b>\n>\f>\u0265"+
    "\t>\u0001>\u0001>\u0003>\u0269\b>\u0003>\u026b\b>\u0001>\u0001>\u0001"+
    "?\u0001?\u0001@\u0001@\u0001@\u0001@\u0005@\u0275\b@\n@\f@\u0278\t@\u0001"+
    "@\u0001@\u0001A\u0001A\u0001A\u0001A\u0001B\u0001B\u0001B\u0001B\u0001"+
    "B\u0001B\u0001B\u0001B\u0001B\u0001B\u0001B\u0001B\u0001B\u0005B\u028d"+
    "\bB\nB\fB\u0290\tB\u0001B\u0001B\u0001B\u0001B\u0001B\u0001B\u0005B\u0298"+
    "\bB\nB\fB\u029b\tB\u0001B\u0001B\u0001B\u0001B\u0001B\u0001B\u0005B\u02a3"+
    "\bB\nB\fB\u02a6\tB\u0001B\u0001B\u0003B\u02aa\bB\u0001C\u0001C\u0001D"+
    "\u0001D\u0003D\u02b0\bD\u0001E\u0003E\u02b3\bE\u0001E\u0001E\u0001F\u0003"+
    "F\u02b8\bF\u0001F\u0001F\u0001G\u0001G\u0001H\u0001H\u0001I\u0001I\u0001"+
    "I\u0001I\u0001I\u0001J\u0001J\u0001K\u0001K\u0001K\u0001K\u0005K\u02cb"+
    "\bK\nK\fK\u02ce\tK\u0001L\u0001L\u0001L\u0000\u0005\u0002jpxzM\u0000\u0002"+
    "\u0004\u0006\b\n\f\u000e\u0010\u0012\u0014\u0016\u0018\u001a\u001c\u001e"+
    " \"$&(*,.02468:<>@BDFHJLNPRTVXZ\\^`bdfhjlnprtvxz|~\u0080\u0082\u0084\u0086"+
    "\u0088\u008a\u008c\u008e\u0090\u0092\u0094\u0096\u0098\u0000\t\u0002\u0000"+
    "44hh\u0001\u0000bc\u0002\u000099>>\u0002\u0000AADD\u0001\u0000TU\u0001"+
    "\u0000VX\u0002\u0000@@LL\u0002\u0000MMOS\u0002\u0000\u0013\u0013\u0015"+
    "\u0016\u02ec\u0000\u009a\u0001\u0000\u0000\u0000\u0002\u009d\u0001\u0000"+
    "\u0000\u0000\u0004\u00ae\u0001\u0000\u0000\u0000\u0006\u00c9\u0001\u0000"+
    "\u0000\u0000\b\u00cb\u0001\u0000\u0000\u0000\n\u00ce\u0001\u0000\u0000"+
    "\u0000\f\u00d0\u0001\u0000\u0000\u0000\u000e\u00d3\u0001\u0000\u0000\u0000"+
    "\u0010\u00de\u0001\u0000\u0000\u0000\u0012\u00e2\u0001\u0000\u0000\u0000"+
    "\u0014\u00e5\u0001\u0000\u0000\u0000\u0016\u00e8\u0001\u0000\u0000\u0000"+
    "\u0018\u00ff\u0001\u0000\u0000\u0000\u001a\u0101\u0001\u0000\u0000\u0000"+
    "\u001c\u0103\u0001\u0000\u0000\u0000\u001e\u0105\u0001\u0000\u0000\u0000"+
    " \u0107\u0001\u0000\u0000\u0000\"\u0110\u0001\u0000\u0000\u0000$\u0113"+
    "\u0001\u0000\u0000\u0000&\u011b\u0001\u0000\u0000\u0000(\u0123\u0001\u0000"+
    "\u0000\u0000*\u0128\u0001\u0000\u0000\u0000,\u0130\u0001\u0000\u0000\u0000"+
    ".\u0138\u0001\u0000\u0000\u00000\u0140\u0001\u0000\u0000\u00002\u0145"+
    "\u0001\u0000\u0000\u00004\u0149\u0001\u0000\u0000\u00006\u014d\u0001\u0000"+
    "\u0000\u00008\u0152\u0001\u0000\u0000\u0000:\u0154\u0001\u0000\u0000\u0000"+
    "<\u0157\u0001\u0000\u0000\u0000>\u0160\u0001\u0000\u0000\u0000@\u0168"+
    "\u0001\u0000\u0000\u0000B\u016b\u0001\u0000\u0000\u0000D\u016e\u0001\u0000"+
    "\u0000\u0000F\u0177\u0001\u0000\u0000\u0000H\u017b\u0001\u0000\u0000\u0000"+
    "J\u0181\u0001\u0000\u0000\u0000L\u0185\u0001\u0000\u0000\u0000N\u0188"+
    "\u0001\u0000\u0000\u0000P\u0190\u0001\u0000\u0000\u0000R\u0194\u0001\u0000"+
    "\u0000\u0000T\u0197\u0001\u0000\u0000\u0000V\u019b\u0001\u0000\u0000\u0000"+
    "X\u019e\u0001\u0000\u0000\u0000Z\u01b2\u0001\u0000\u0000\u0000\\\u01b6"+
    "\u0001\u0000\u0000\u0000^\u01bb\u0001\u0000\u0000\u0000`\u01c1\u0001\u0000"+
    "\u0000\u0000b\u01ce\u0001\u0000\u0000\u0000d\u01d1\u0001\u0000\u0000\u0000"+
    "f\u01d5\u0001\u0000\u0000\u0000h\u01d9\u0001\u0000\u0000\u0000j\u01dd"+
    "\u0001\u0000\u0000\u0000l\u01eb\u0001\u0000\u0000\u0000n\u01ed\u0001\u0000"+
    "\u0000\u0000p\u020c\u0001\u0000\u0000\u0000r\u0227\u0001\u0000\u0000\u0000"+
    "t\u0229\u0001\u0000\u0000\u0000v\u0236\u0001\u0000\u0000\u0000x\u023c"+
    "\u0001\u0000\u0000\u0000z\u0251\u0001\u0000\u0000\u0000|\u025b\u0001\u0000"+
    "\u0000\u0000~\u026e\u0001\u0000\u0000\u0000\u0080\u0270\u0001\u0000\u0000"+
    "\u0000\u0082\u027b\u0001\u0000\u0000\u0000\u0084\u02a9\u0001\u0000\u0000"+
    "\u0000\u0086\u02ab\u0001\u0000\u0000\u0000\u0088\u02af\u0001\u0000\u0000"+
    "\u0000\u008a\u02b2\u0001\u0000\u0000\u0000\u008c\u02b7\u0001\u0000\u0000"+
    "\u0000\u008e\u02bb\u0001\u0000\u0000\u0000\u0090\u02bd\u0001\u0000\u0000"+
    "\u0000\u0092\u02bf\u0001\u0000\u0000\u0000\u0094\u02c4\u0001\u0000\u0000"+
    "\u0000\u0096\u02c6\u0001\u0000\u0000\u0000\u0098\u02cf\u0001\u0000\u0000"+
    "\u0000\u009a\u009b\u0003\u0002\u0001\u0000\u009b\u009c\u0005\u0000\u0000"+
    "\u0001\u009c\u0001\u0001\u0000\u0000\u0000\u009d\u009e\u0006\u0001\uffff"+
    "\uffff\u0000\u009e\u009f\u0003\u0004\u0002\u0000\u009f\u00a5\u0001\u0000"+
    "\u0000\u0000\u00a0\u00a1\n\u0001\u0000\u0000\u00a1\u00a2\u00053\u0000"+
    "\u0000\u00a2\u00a4\u0003\u0006\u0003\u0000\u00a3\u00a0\u0001\u0000\u0000"+
    "\u0000\u00a4\u00a7\u0001\u0000\u0000\u0000\u00a5\u00a3\u0001\u0000\u0000"+
    "\u0000\u00a5\u00a6\u0001\u0000\u0000\u0000\u00a6\u0003\u0001\u0000\u0000"+
    "\u0000\u00a7\u00a5\u0001\u0000\u0000\u0000\u00a8\u00af\u0003R)\u0000\u00a9"+
    "\u00af\u0003\u0012\t\u0000\u00aa\u00af\u0003\f\u0006\u0000\u00ab\u00af"+
    "\u0003V+\u0000\u00ac\u00ad\u0004\u0002\u0001\u0000\u00ad\u00af\u0003\u0014"+
    "\n\u0000\u00ae\u00a8\u0001\u0000\u0000\u0000\u00ae\u00a9\u0001\u0000\u0000"+
    "\u0000\u00ae\u00aa\u0001\u0000\u0000\u0000\u00ae\u00ab\u0001\u0000\u0000"+
    "\u0000\u00ae\u00ac\u0001\u0000\u0000\u0000\u00af\u0005\u0001\u0000\u0000"+
    "\u0000\u00b0\u00ca\u0003\"\u0011\u0000\u00b1\u00ca\u0003\b\u0004\u0000"+
    "\u00b2\u00ca\u0003@ \u0000\u00b3\u00ca\u0003:\u001d\u0000\u00b4\u00ca"+
    "\u0003$\u0012\u0000\u00b5\u00ca\u0003<\u001e\u0000\u00b6\u00ca\u0003B"+
    "!\u0000\u00b7\u00ca\u0003D\"\u0000\u00b8\u00ca\u0003H$\u0000\u00b9\u00ca"+
    "\u0003J%\u0000\u00ba\u00ca\u0003X,\u0000\u00bb\u00ca\u0003L&\u0000\u00bc"+
    "\u00ca\u0003\u0092I\u0000\u00bd\u00be\u0004\u0003\u0002\u0000\u00be\u00ca"+
    "\u0003^/\u0000\u00bf\u00c0\u0004\u0003\u0003\u0000\u00c0\u00ca\u0003\\"+
    ".\u0000\u00c1\u00c2\u0004\u0003\u0004\u0000\u00c2\u00ca\u0003`0\u0000"+
    "\u00c3\u00c4\u0004\u0003\u0005\u0000\u00c4\u00ca\u0003b1\u0000\u00c5\u00c6"+
    "\u0004\u0003\u0006\u0000\u00c6\u00ca\u0003d2\u0000\u00c7\u00c8\u0004\u0003"+
    "\u0007\u0000\u00c8\u00ca\u0003n7\u0000\u00c9\u00b0\u0001\u0000\u0000\u0000"+
    "\u00c9\u00b1\u0001\u0000\u0000\u0000\u00c9\u00b2\u0001\u0000\u0000\u0000"+
    "\u00c9\u00b3\u0001\u0000\u0000\u0000\u00c9\u00b4\u0001\u0000\u0000\u0000"+
    "\u00c9\u00b5\u0001\u0000\u0000\u0000\u00c9\u00b6\u0001\u0000\u0000\u0000"+
    "\u00c9\u00b7\u0001\u0000\u0000\u0000\u00c9\u00b8\u0001\u0000\u0000\u0000"+
    "\u00c9\u00b9\u0001\u0000\u0000\u0000\u00c9\u00ba\u0001\u0000\u0000\u0000"+
    "\u00c9\u00bb\u0001\u0000\u0000\u0000\u00c9\u00bc\u0001\u0000\u0000\u0000"+
    "\u00c9\u00bd\u0001\u0000\u0000\u0000\u00c9\u00bf\u0001\u0000\u0000\u0000"+
    "\u00c9\u00c1\u0001\u0000\u0000\u0000\u00c9\u00c3\u0001\u0000\u0000\u0000"+
    "\u00c9\u00c5\u0001\u0000\u0000\u0000\u00c9\u00c7\u0001\u0000\u0000\u0000"+
    "\u00ca\u0007\u0001\u0000\u0000\u0000\u00cb\u00cc\u0005\u000e\u0000\u0000"+
    "\u00cc\u00cd\u0003p8\u0000\u00cd\t\u0001\u0000\u0000\u0000\u00ce\u00cf"+
    "\u00030\u0018\u0000\u00cf\u000b\u0001\u0000\u0000\u0000\u00d0\u00d1\u0005"+
    "\u000b\u0000\u0000\u00d1\u00d2\u0003\u000e\u0007\u0000\u00d2\r\u0001\u0000"+
    "\u0000\u0000\u00d3\u00d8\u0003\u0010\b\u0000\u00d4\u00d5\u0005=\u0000"+
    "\u0000\u00d5\u00d7\u0003\u0010\b\u0000\u00d6\u00d4\u0001\u0000\u0000\u0000"+
    "\u00d7\u00da\u0001\u0000\u0000\u0000\u00d8\u00d6\u0001\u0000\u0000\u0000"+
    "\u00d8\u00d9\u0001\u0000\u0000\u0000\u00d9\u000f\u0001\u0000\u0000\u0000"+
    "\u00da\u00d8\u0001\u0000\u0000\u0000\u00db\u00dc\u0003*\u0015\u0000\u00dc"+
    "\u00dd\u0005:\u0000\u0000\u00dd\u00df\u0001\u0000\u0000\u0000\u00de\u00db"+
    "\u0001\u0000\u0000\u0000\u00de\u00df\u0001\u0000\u0000\u0000\u00df\u00e0"+
    "\u0001\u0000\u0000\u0000\u00e0\u00e1\u0003p8\u0000\u00e1\u0011\u0001\u0000"+
    "\u0000\u0000\u00e2\u00e3\u0005\u0010\u0000\u0000\u00e3\u00e4\u0003\u0016"+
    "\u000b\u0000\u00e4\u0013\u0001\u0000\u0000\u0000\u00e5\u00e6\u0005\u0011"+
    "\u0000\u0000\u00e6\u00e7\u0003\u0016\u000b\u0000\u00e7\u0015\u0001\u0000"+
    "\u0000\u0000\u00e8\u00ed\u0003\u0018\f\u0000\u00e9\u00ea\u0005=\u0000"+
    "\u0000\u00ea\u00ec\u0003\u0018\f\u0000\u00eb\u00e9\u0001\u0000\u0000\u0000"+
    "\u00ec\u00ef\u0001\u0000\u0000\u0000\u00ed\u00eb\u0001\u0000\u0000\u0000"+
    "\u00ed\u00ee\u0001\u0000\u0000\u0000\u00ee\u00f1\u0001\u0000\u0000\u0000"+
    "\u00ef\u00ed\u0001\u0000\u0000\u0000\u00f0\u00f2\u0003 \u0010\u0000\u00f1"+
    "\u00f0\u0001\u0000\u0000\u0000\u00f1\u00f2\u0001\u0000\u0000\u0000\u00f2"+
    "\u0017\u0001\u0000\u0000\u0000\u00f3\u00f4\u0003\u001a\r\u0000\u00f4\u00f5"+
    "\u0005<\u0000\u0000\u00f5\u00f7\u0001\u0000\u0000\u0000\u00f6\u00f3\u0001"+
    "\u0000\u0000\u0000\u00f6\u00f7\u0001\u0000\u0000\u0000\u00f7\u00f8\u0001"+
    "\u0000\u0000\u0000\u00f8\u0100\u0003\u001e\u000f\u0000\u00f9\u00fa\u0004"+
    "\f\b\u0000\u00fa\u00fd\u0003\u001e\u000f\u0000\u00fb\u00fc\u0005;\u0000"+
    "\u0000\u00fc\u00fe\u0003\u001c\u000e\u0000\u00fd\u00fb\u0001\u0000\u0000"+
    "\u0000\u00fd\u00fe\u0001\u0000\u0000\u0000\u00fe\u0100\u0001\u0000\u0000"+
    "\u0000\u00ff\u00f6\u0001\u0000\u0000\u0000\u00ff\u00f9\u0001\u0000\u0000"+
    "\u0000\u0100\u0019\u0001\u0000\u0000\u0000\u0101\u0102\u0007\u0000\u0000"+
    "\u0000\u0102\u001b\u0001\u0000\u0000\u0000\u0103\u0104\u0007\u0000\u0000"+
    "\u0000\u0104\u001d\u0001\u0000\u0000\u0000\u0105\u0106\u0007\u0000\u0000"+
    "\u0000\u0106\u001f\u0001\u0000\u0000\u0000\u0107\u0108\u0005g\u0000\u0000"+
    "\u0108\u010d\u0005h\u0000\u0000\u0109\u010a\u0005=\u0000\u0000\u010a\u010c"+
    "\u0005h\u0000\u0000\u010b\u0109\u0001\u0000\u0000\u0000\u010c\u010f\u0001"+
    "\u0000\u0000\u0000\u010d\u010b\u0001\u0000\u0000\u0000\u010d\u010e\u0001"+
    "\u0000\u0000\u0000\u010e!\u0001\u0000\u0000\u0000\u010f\u010d\u0001\u0000"+
    "\u0000\u0000\u0110\u0111\u0005\b\u0000\u0000\u0111\u0112\u0003\u000e\u0007"+
    "\u0000\u0112#\u0001\u0000\u0000\u0000\u0113\u0115\u0005\r\u0000\u0000"+
    "\u0114\u0116\u0003&\u0013\u0000\u0115\u0114\u0001\u0000\u0000\u0000\u0115"+
    "\u0116\u0001\u0000\u0000\u0000\u0116\u0119\u0001\u0000\u0000\u0000\u0117"+
    "\u0118\u00057\u0000\u0000\u0118\u011a\u0003\u000e\u0007\u0000\u0119\u0117"+
    "\u0001\u0000\u0000\u0000\u0119\u011a\u0001\u0000\u0000\u0000\u011a%\u0001"+
    "\u0000\u0000\u0000\u011b\u0120\u0003(\u0014\u0000\u011c\u011d\u0005=\u0000"+
    "\u0000\u011d\u011f\u0003(\u0014\u0000\u011e\u011c\u0001\u0000\u0000\u0000"+
    "\u011f\u0122\u0001\u0000\u0000\u0000\u0120\u011e\u0001\u0000\u0000\u0000"+
    "\u0120\u0121\u0001\u0000\u0000\u0000\u0121\'\u0001\u0000\u0000\u0000\u0122"+
    "\u0120\u0001\u0000\u0000\u0000\u0123\u0126\u0003\u0010\b\u0000\u0124\u0125"+
    "\u0005\u000e\u0000\u0000\u0125\u0127\u0003p8\u0000\u0126\u0124\u0001\u0000"+
    "\u0000\u0000\u0126\u0127\u0001\u0000\u0000\u0000\u0127)\u0001\u0000\u0000"+
    "\u0000\u0128\u012d\u00038\u001c\u0000\u0129\u012a\u0005?\u0000\u0000\u012a"+
    "\u012c\u00038\u001c\u0000\u012b\u0129\u0001\u0000\u0000\u0000\u012c\u012f"+
    "\u0001\u0000\u0000\u0000\u012d\u012b\u0001\u0000\u0000\u0000\u012d\u012e"+
    "\u0001\u0000\u0000\u0000\u012e+\u0001\u0000\u0000\u0000\u012f\u012d\u0001"+
    "\u0000\u0000\u0000\u0130\u0135\u00032\u0019\u0000\u0131\u0132\u0005?\u0000"+
    "\u0000\u0132\u0134\u00032\u0019\u0000\u0133\u0131\u0001\u0000\u0000\u0000"+
    "\u0134\u0137\u0001\u0000\u0000\u0000\u0135\u0133\u0001\u0000\u0000\u0000"+
    "\u0135\u0136\u0001\u0000\u0000\u0000\u0136-\u0001\u0000\u0000\u0000\u0137"+
    "\u0135\u0001\u0000\u0000\u0000\u0138\u013d\u0003,\u0016\u0000\u0139\u013a"+
    "\u0005=\u0000\u0000\u013a\u013c\u0003,\u0016\u0000\u013b\u0139\u0001\u0000"+
    "\u0000\u0000\u013c\u013f\u0001\u0000\u0000\u0000\u013d\u013b\u0001\u0000"+
    "\u0000\u0000\u013d\u013e\u0001\u0000\u0000\u0000\u013e/\u0001\u0000\u0000"+
    "\u0000\u013f\u013d\u0001\u0000\u0000\u0000\u0140\u0141\u0007\u0001\u0000"+
    "\u0000\u01411\u0001\u0000\u0000\u0000\u0142\u0146\u0005}\u0000\u0000\u0143"+
    "\u0146\u00034\u001a\u0000\u0144\u0146\u00036\u001b\u0000\u0145\u0142\u0001"+
    "\u0000\u0000\u0000\u0145\u0143\u0001\u0000\u0000\u0000\u0145\u0144\u0001"+
    "\u0000\u0000\u0000\u01463\u0001\u0000\u0000\u0000\u0147\u014a\u0005J\u0000"+
    "\u0000\u0148\u014a\u0005\\\u0000\u0000\u0149\u0147\u0001\u0000\u0000\u0000"+
    "\u0149\u0148\u0001\u0000\u0000\u0000\u014a5\u0001\u0000\u0000\u0000\u014b"+
    "\u014e\u0005[\u0000\u0000\u014c\u014e\u0005]\u0000\u0000\u014d\u014b\u0001"+
    "\u0000\u0000\u0000\u014d\u014c\u0001\u0000\u0000\u0000\u014e7\u0001\u0000"+
    "\u0000\u0000\u014f\u0153\u00030\u0018\u0000\u0150\u0153\u00034\u001a\u0000"+
    "\u0151\u0153\u00036\u001b\u0000\u0152\u014f\u0001\u0000\u0000\u0000\u0152"+
    "\u0150\u0001\u0000\u0000\u0000\u0152\u0151\u0001\u0000\u0000\u0000\u0153"+
    "9\u0001\u0000\u0000\u0000\u0154\u0155\u0005\n\u0000\u0000\u0155\u0156"+
    "\u00055\u0000\u0000\u0156;\u0001\u0000\u0000\u0000\u0157\u0158\u0005\f"+
    "\u0000\u0000\u0158\u015d\u0003>\u001f\u0000\u0159\u015a\u0005=\u0000\u0000"+
    "\u015a\u015c\u0003>\u001f\u0000\u015b\u0159\u0001\u0000\u0000\u0000\u015c"+
    "\u015f\u0001\u0000\u0000\u0000\u015d\u015b\u0001\u0000\u0000\u0000\u015d"+
    "\u015e\u0001\u0000\u0000\u0000\u015e=\u0001\u0000\u0000\u0000\u015f\u015d"+
    "\u0001\u0000\u0000\u0000\u0160\u0162\u0003p8\u0000\u0161\u0163\u0007\u0002"+
    "\u0000\u0000\u0162\u0161\u0001\u0000\u0000\u0000\u0162\u0163\u0001\u0000"+
    "\u0000\u0000\u0163\u0166\u0001\u0000\u0000\u0000\u0164\u0165\u0005H\u0000"+
    "\u0000\u0165\u0167\u0007\u0003\u0000\u0000\u0166\u0164\u0001\u0000\u0000"+
    "\u0000\u0166\u0167\u0001\u0000\u0000\u0000\u0167?\u0001\u0000\u0000\u0000"+
    "\u0168\u0169\u0005\u001a\u0000\u0000\u0169\u016a\u0003.\u0017\u0000\u016a"+
    "A\u0001\u0000\u0000\u0000\u016b\u016c\u0005\u0019\u0000\u0000\u016c\u016d"+
    "\u0003.\u0017\u0000\u016dC\u0001\u0000\u0000\u0000\u016e\u016f\u0005\u001d"+
    "\u0000\u0000\u016f\u0174\u0003F#\u0000\u0170\u0171\u0005=\u0000\u0000"+
    "\u0171\u0173\u0003F#\u0000\u0172\u0170\u0001\u0000\u0000\u0000\u0173\u0176"+
    "\u0001\u0000\u0000\u0000\u0174\u0172\u0001\u0000\u0000\u0000\u0174\u0175"+
    "\u0001\u0000\u0000\u0000\u0175E\u0001\u0000\u0000\u0000\u0176\u0174\u0001"+
    "\u0000\u0000\u0000\u0177\u0178\u0003,\u0016\u0000\u0178\u0179\u0005\u0081"+
    "\u0000\u0000\u0179\u017a\u0003,\u0016\u0000\u017aG\u0001\u0000\u0000\u0000"+
    "\u017b\u017c\u0005\u0007\u0000\u0000\u017c\u017d\u0003z=\u0000\u017d\u017f"+
    "\u0003\u008eG\u0000\u017e\u0180\u0003N\'\u0000\u017f\u017e\u0001\u0000"+
    "\u0000\u0000\u017f\u0180\u0001\u0000\u0000\u0000\u0180I\u0001\u0000\u0000"+
    "\u0000\u0181\u0182\u0005\t\u0000\u0000\u0182\u0183\u0003z=\u0000\u0183"+
    "\u0184\u0003\u008eG\u0000\u0184K\u0001\u0000\u0000\u0000\u0185\u0186\u0005"+
    "\u0018\u0000\u0000\u0186\u0187\u0003*\u0015\u0000\u0187M\u0001\u0000\u0000"+
    "\u0000\u0188\u018d\u0003P(\u0000\u0189\u018a\u0005=\u0000\u0000\u018a"+
    "\u018c\u0003P(\u0000\u018b\u0189\u0001\u0000\u0000\u0000\u018c\u018f\u0001"+
    "\u0000\u0000\u0000\u018d\u018b\u0001\u0000\u0000\u0000\u018d\u018e\u0001"+
    "\u0000\u0000\u0000\u018eO\u0001\u0000\u0000\u0000\u018f\u018d\u0001\u0000"+
    "\u0000\u0000\u0190\u0191\u00030\u0018\u0000\u0191\u0192\u0005:\u0000\u0000"+
    "\u0192\u0193\u0003\u0084B\u0000\u0193Q\u0001\u0000\u0000\u0000\u0194\u0195"+
    "\u0005\u0006\u0000\u0000\u0195\u0196\u0003T*\u0000\u0196S\u0001\u0000"+
    "\u0000\u0000\u0197\u0198\u0005^\u0000\u0000\u0198\u0199\u0003\u0002\u0001"+
    "\u0000\u0199\u019a\u0005_\u0000\u0000\u019aU\u0001\u0000\u0000\u0000\u019b"+
    "\u019c\u0005\u001e\u0000\u0000\u019c\u019d\u0005\u0085\u0000\u0000\u019d"+
    "W\u0001\u0000\u0000\u0000\u019e\u019f\u0005\u0005\u0000\u0000\u019f\u01a2"+
    "\u0005%\u0000\u0000\u01a0\u01a1\u0005#\u0000\u0000\u01a1\u01a3\u0003,"+
    "\u0016\u0000\u01a2\u01a0\u0001\u0000\u0000\u0000\u01a2\u01a3\u0001\u0000"+
    "\u0000\u0000\u01a3\u01ad\u0001\u0000\u0000\u0000\u01a4\u01a5\u0005$\u0000"+
    "\u0000\u01a5\u01aa\u0003Z-\u0000\u01a6\u01a7\u0005=\u0000\u0000\u01a7"+
    "\u01a9\u0003Z-\u0000\u01a8\u01a6\u0001\u0000\u0000\u0000\u01a9\u01ac\u0001"+
    "\u0000\u0000\u0000\u01aa\u01a8\u0001\u0000\u0000\u0000\u01aa\u01ab\u0001"+
    "\u0000\u0000\u0000\u01ab\u01ae\u0001\u0000\u0000\u0000\u01ac\u01aa\u0001"+
    "\u0000\u0000\u0000\u01ad\u01a4\u0001\u0000\u0000\u0000\u01ad\u01ae\u0001"+
    "\u0000\u0000\u0000\u01aeY\u0001\u0000\u0000\u0000\u01af\u01b0\u0003,\u0016"+
    "\u0000\u01b0\u01b1\u0005:\u0000\u0000\u01b1\u01b3\u0001\u0000\u0000\u0000"+
    "\u01b2\u01af\u0001\u0000\u0000\u0000\u01b2\u01b3\u0001\u0000\u0000\u0000"+
    "\u01b3\u01b4\u0001\u0000\u0000\u0000\u01b4\u01b5\u0003,\u0016\u0000\u01b5"+
    "[\u0001\u0000\u0000\u0000\u01b6\u01b7\u0005\u0017\u0000\u0000\u01b7\u01b8"+
    "\u0003\u0018\f\u0000\u01b8\u01b9\u0005#\u0000\u0000\u01b9\u01ba\u0003"+
    ".\u0017\u0000\u01ba]\u0001\u0000\u0000\u0000\u01bb\u01bc\u0005\u000f\u0000"+
    "\u0000\u01bc\u01bf\u0003&\u0013\u0000\u01bd\u01be\u00057\u0000\u0000\u01be"+
    "\u01c0\u0003\u000e\u0007\u0000\u01bf\u01bd\u0001\u0000\u0000\u0000\u01bf"+
    "\u01c0\u0001\u0000\u0000\u0000\u01c0_\u0001\u0000\u0000\u0000\u01c1\u01c2"+
    "\u0005\u0004\u0000\u0000\u01c2\u01c5\u0003*\u0015\u0000\u01c3\u01c4\u0005"+
    "#\u0000\u0000\u01c4\u01c6\u0003*\u0015\u0000\u01c5\u01c3\u0001\u0000\u0000"+
    "\u0000\u01c5\u01c6\u0001\u0000\u0000\u0000\u01c6\u01cc\u0001\u0000\u0000"+
    "\u0000\u01c7\u01c8\u0005\u0081\u0000\u0000\u01c8\u01c9\u0003*\u0015\u0000"+
    "\u01c9\u01ca\u0005=\u0000\u0000\u01ca\u01cb\u0003*\u0015\u0000\u01cb\u01cd"+
    "\u0001\u0000\u0000\u0000\u01cc\u01c7\u0001\u0000\u0000\u0000\u01cc\u01cd"+
    "\u0001\u0000\u0000\u0000\u01cda\u0001\u0000\u0000\u0000\u01ce\u01cf\u0005"+
    "\u001b\u0000\u0000\u01cf\u01d0\u0003.\u0017\u0000\u01d0c\u0001\u0000\u0000"+
    "\u0000\u01d1\u01d2\u0005\u0012\u0000\u0000\u01d2\u01d3\u0003f3\u0000\u01d3"+
    "e\u0001\u0000\u0000\u0000\u01d4\u01d6\u0003h4\u0000\u01d5\u01d4\u0001"+
    "\u0000\u0000\u0000\u01d6\u01d7\u0001\u0000\u0000\u0000\u01d7\u01d5\u0001"+
    "\u0000\u0000\u0000\u01d7\u01d8\u0001\u0000\u0000\u0000\u01d8g\u0001\u0000"+
    "\u0000\u0000\u01d9\u01da\u0005`\u0000\u0000\u01da\u01db\u0003j5\u0000"+
    "\u01db\u01dc\u0005a\u0000\u0000\u01dci\u0001\u0000\u0000\u0000\u01dd\u01de"+
    "\u00065\uffff\uffff\u0000\u01de\u01df\u0003l6\u0000\u01df\u01e5\u0001"+
    "\u0000\u0000\u0000\u01e0\u01e1\n\u0001\u0000\u0000\u01e1\u01e2\u00053"+
    "\u0000\u0000\u01e2\u01e4\u0003l6\u0000\u01e3\u01e0\u0001\u0000\u0000\u0000"+
    "\u01e4\u01e7\u0001\u0000\u0000\u0000\u01e5\u01e3\u0001\u0000\u0000\u0000"+
    "\u01e5\u01e6\u0001\u0000\u0000\u0000\u01e6k\u0001\u0000\u0000\u0000\u01e7"+
    "\u01e5\u0001\u0000\u0000\u0000\u01e8\u01ec\u0003\b\u0004\u0000\u01e9\u01ec"+
    "\u0003<\u001e\u0000\u01ea\u01ec\u0003:\u001d\u0000\u01eb\u01e8\u0001\u0000"+
    "\u0000\u0000\u01eb\u01e9\u0001\u0000\u0000\u0000\u01eb\u01ea\u0001\u0000"+
    "\u0000\u0000\u01ecm\u0001\u0000\u0000\u0000\u01ed\u01ee\u0005\u001c\u0000"+
    "\u0000\u01eeo\u0001\u0000\u0000\u0000\u01ef\u01f0\u00068\uffff\uffff\u0000"+
    "\u01f0\u01f1\u0005F\u0000\u0000\u01f1\u020d\u0003p8\b\u01f2\u020d\u0003"+
    "v;\u0000\u01f3\u020d\u0003r9\u0000\u01f4\u01f6\u0003v;\u0000\u01f5\u01f7"+
    "\u0005F\u0000\u0000\u01f6\u01f5\u0001\u0000\u0000\u0000\u01f6\u01f7\u0001"+
    "\u0000\u0000\u0000\u01f7\u01f8\u0001\u0000\u0000\u0000\u01f8\u01f9\u0005"+
    "B\u0000\u0000\u01f9\u01fa\u0005`\u0000\u0000\u01fa\u01ff\u0003v;\u0000"+
    "\u01fb\u01fc\u0005=\u0000\u0000\u01fc\u01fe\u0003v;\u0000\u01fd\u01fb"+
    "\u0001\u0000\u0000\u0000\u01fe\u0201\u0001\u0000\u0000\u0000\u01ff\u01fd"+
    "\u0001\u0000\u0000\u0000\u01ff\u0200\u0001\u0000\u0000\u0000\u0200\u0202"+
    "\u0001\u0000\u0000\u0000\u0201\u01ff\u0001\u0000\u0000\u0000\u0202\u0203"+
    "\u0005a\u0000\u0000\u0203\u020d\u0001\u0000\u0000\u0000\u0204\u0205\u0003"+
    "v;\u0000\u0205\u0207\u0005C\u0000\u0000\u0206\u0208\u0005F\u0000\u0000"+
    "\u0207\u0206\u0001\u0000\u0000\u0000\u0207\u0208\u0001\u0000\u0000\u0000"+
    "\u0208\u0209\u0001\u0000\u0000\u0000\u0209\u020a\u0005G\u0000\u0000\u020a"+
    "\u020d\u0001\u0000\u0000\u0000\u020b\u020d\u0003t:\u0000\u020c\u01ef\u0001"+
    "\u0000\u0000\u0000\u020c\u01f2\u0001\u0000\u0000\u0000\u020c\u01f3\u0001"+
    "\u0000\u0000\u0000\u020c\u01f4\u0001\u0000\u0000\u0000\u020c\u0204\u0001"+
    "\u0000\u0000\u0000\u020c\u020b\u0001\u0000\u0000\u0000\u020d\u0216\u0001"+
    "\u0000\u0000\u0000\u020e\u020f\n\u0005\u0000\u0000\u020f\u0210\u00058"+
    "\u0000\u0000\u0210\u0215\u0003p8\u0006\u0211\u0212\n\u0004\u0000\u0000"+
    "\u0212\u0213\u0005I\u0000\u0000\u0213\u0215\u0003p8\u0005\u0214\u020e"+
    "\u0001\u0000\u0000\u0000\u0214\u0211\u0001\u0000\u0000\u0000\u0215\u0218"+
    "\u0001\u0000\u0000\u0000\u0216\u0214\u0001\u0000\u0000\u0000\u0216\u0217"+
    "\u0001\u0000\u0000\u0000\u0217q\u0001\u0000\u0000\u0000\u0218\u0216\u0001"+
    "\u0000\u0000\u0000\u0219\u021b\u0003v;\u0000\u021a\u021c\u0005F\u0000"+
    "\u0000\u021b\u021a\u0001\u0000\u0000\u0000\u021b\u021c\u0001\u0000\u0000"+
    "\u0000\u021c\u021d\u0001\u0000\u0000\u0000\u021d\u021e\u0005E\u0000\u0000"+
    "\u021e\u021f\u0003\u008eG\u0000\u021f\u0228\u0001\u0000\u0000\u0000\u0220"+
    "\u0222\u0003v;\u0000\u0221\u0223\u0005F\u0000\u0000\u0222\u0221\u0001"+
    "\u0000\u0000\u0000\u0222\u0223\u0001\u0000\u0000\u0000\u0223\u0224\u0001"+
    "\u0000\u0000\u0000\u0224\u0225\u0005K\u0000\u0000\u0225\u0226\u0003\u008e"+
    "G\u0000\u0226\u0228\u0001\u0000\u0000\u0000\u0227\u0219\u0001\u0000\u0000"+
    "\u0000\u0227\u0220\u0001\u0000\u0000\u0000\u0228s\u0001\u0000\u0000\u0000"+
    "\u0229\u022c\u0003*\u0015\u0000\u022a\u022b\u0005;\u0000\u0000\u022b\u022d"+
    "\u0003\n\u0005\u0000\u022c\u022a\u0001\u0000\u0000\u0000\u022c\u022d\u0001"+
    "\u0000\u0000\u0000\u022d\u022e\u0001\u0000\u0000\u0000\u022e\u022f\u0005"+
    "<\u0000\u0000\u022f\u0230\u0003\u0084B\u0000\u0230u\u0001\u0000\u0000"+
    "\u0000\u0231\u0237\u0003x<\u0000\u0232\u0233\u0003x<\u0000\u0233\u0234"+
    "\u0003\u0090H\u0000\u0234\u0235\u0003x<\u0000\u0235\u0237\u0001\u0000"+
    "\u0000\u0000\u0236\u0231\u0001\u0000\u0000\u0000\u0236\u0232\u0001\u0000"+
    "\u0000\u0000\u0237w\u0001\u0000\u0000\u0000\u0238\u0239\u0006<\uffff\uffff"+
    "\u0000\u0239\u023d\u0003z=\u0000\u023a\u023b\u0007\u0004\u0000\u0000\u023b"+
    "\u023d\u0003x<\u0003\u023c\u0238\u0001\u0000\u0000\u0000\u023c\u023a\u0001"+
    "\u0000\u0000\u0000\u023d\u0246\u0001\u0000\u0000\u0000\u023e\u023f\n\u0002"+
    "\u0000\u0000\u023f\u0240\u0007\u0005\u0000\u0000\u0240\u0245\u0003x<\u0003"+
    "\u0241\u0242\n\u0001\u0000\u0000\u0242\u0243\u0007\u0004\u0000\u0000\u0243"+
    "\u0245\u0003x<\u0002\u0244\u023e\u0001\u0000\u0000\u0000\u0244\u0241\u0001"+
    "\u0000\u0000\u0000\u0245\u0248\u0001\u0000\u0000\u0000\u0246\u0244\u0001"+
    "\u0000\u0000\u0000\u0246\u0247\u0001\u0000\u0000\u0000\u0247y\u0001\u0000"+
    "\u0000\u0000\u0248\u0246\u0001\u0000\u0000\u0000\u0249\u024a\u0006=\uffff"+
    "\uffff\u0000\u024a\u0252\u0003\u0084B\u0000\u024b\u0252\u0003*\u0015\u0000"+
    "\u024c\u0252\u0003|>\u0000\u024d\u024e\u0005`\u0000\u0000\u024e\u024f"+
    "\u0003p8\u0000\u024f\u0250\u0005a\u0000\u0000\u0250\u0252\u0001\u0000"+
    "\u0000\u0000\u0251\u0249\u0001\u0000\u0000\u0000\u0251\u024b\u0001\u0000"+
    "\u0000\u0000\u0251\u024c\u0001\u0000\u0000\u0000\u0251\u024d\u0001\u0000"+
    "\u0000\u0000\u0252\u0258\u0001\u0000\u0000\u0000\u0253\u0254\n\u0001\u0000"+
    "\u0000\u0254\u0255\u0005;\u0000\u0000\u0255\u0257\u0003\n\u0005\u0000"+
    "\u0256\u0253\u0001\u0000\u0000\u0000\u0257\u025a\u0001\u0000\u0000\u0000"+
    "\u0258\u0256\u0001\u0000\u0000\u0000\u0258\u0259\u0001\u0000\u0000\u0000"+
    "\u0259{\u0001\u0000\u0000\u0000\u025a\u0258\u0001\u0000\u0000\u0000\u025b"+
    "\u025c\u0003~?\u0000\u025c\u026a\u0005`\u0000\u0000\u025d\u026b\u0005"+
    "V\u0000\u0000\u025e\u0263\u0003p8\u0000\u025f\u0260\u0005=\u0000\u0000"+
    "\u0260\u0262\u0003p8\u0000\u0261\u025f\u0001\u0000\u0000\u0000\u0262\u0265"+
    "\u0001\u0000\u0000\u0000\u0263\u0261\u0001\u0000\u0000\u0000\u0263\u0264"+
    "\u0001\u0000\u0000\u0000\u0264\u0268\u0001\u0000\u0000\u0000\u0265\u0263"+
    "\u0001\u0000\u0000\u0000\u0266\u0267\u0005=\u0000\u0000\u0267\u0269\u0003"+
    "\u0080@\u0000\u0268\u0266\u0001\u0000\u0000\u0000\u0268\u0269\u0001\u0000"+
    "\u0000\u0000\u0269\u026b\u0001\u0000\u0000\u0000\u026a\u025d\u0001\u0000"+
    "\u0000\u0000\u026a\u025e\u0001\u0000\u0000\u0000\u026a\u026b\u0001\u0000"+
    "\u0000\u0000\u026b\u026c\u0001\u0000\u0000\u0000\u026c\u026d\u0005a\u0000"+
    "\u0000\u026d}\u0001\u0000\u0000\u0000\u026e\u026f\u00038\u001c\u0000\u026f"+
    "\u007f\u0001\u0000\u0000\u0000\u0270\u0271\u0005Y\u0000\u0000\u0271\u0276"+
    "\u0003\u0082A\u0000\u0272\u0273\u0005=\u0000\u0000\u0273\u0275\u0003\u0082"+
    "A\u0000\u0274\u0272\u0001\u0000\u0000\u0000\u0275\u0278\u0001\u0000\u0000"+
    "\u0000\u0276\u0274\u0001\u0000\u0000\u0000\u0276\u0277\u0001\u0000\u0000"+
    "\u0000\u0277\u0279\u0001\u0000\u0000\u0000\u0278\u0276\u0001\u0000\u0000"+
    "\u0000\u0279\u027a\u0005Z\u0000\u0000\u027a\u0081\u0001\u0000\u0000\u0000"+
    "\u027b\u027c\u0003\u008eG\u0000\u027c\u027d\u0005<\u0000\u0000\u027d\u027e"+
    "\u0003\u0084B\u0000\u027e\u0083\u0001\u0000\u0000\u0000\u027f\u02aa\u0005"+
    "G\u0000\u0000\u0280\u0281\u0003\u008cF\u0000\u0281\u0282\u0005b\u0000"+
    "\u0000\u0282\u02aa\u0001\u0000\u0000\u0000\u0283\u02aa\u0003\u008aE\u0000"+
    "\u0284\u02aa\u0003\u008cF\u0000\u0285\u02aa\u0003\u0086C\u0000\u0286\u02aa"+
    "\u00034\u001a\u0000\u0287\u02aa\u0003\u008eG\u0000\u0288\u0289\u0005^"+
    "\u0000\u0000\u0289\u028e\u0003\u0088D\u0000\u028a\u028b\u0005=\u0000\u0000"+
    "\u028b\u028d\u0003\u0088D\u0000\u028c\u028a\u0001\u0000\u0000\u0000\u028d"+
    "\u0290\u0001\u0000\u0000\u0000\u028e\u028c\u0001\u0000\u0000\u0000\u028e"+
    "\u028f\u0001\u0000\u0000\u0000\u028f\u0291\u0001\u0000\u0000\u0000\u0290"+
    "\u028e\u0001\u0000\u0000\u0000\u0291\u0292\u0005_\u0000\u0000\u0292\u02aa"+
    "\u0001\u0000\u0000\u0000\u0293\u0294\u0005^\u0000\u0000\u0294\u0299\u0003"+
    "\u0086C\u0000\u0295\u0296\u0005=\u0000\u0000\u0296\u0298\u0003\u0086C"+
    "\u0000\u0297\u0295\u0001\u0000\u0000\u0000\u0298\u029b\u0001\u0000\u0000"+
    "\u0000\u0299\u0297\u0001\u0000\u0000\u0000\u0299\u029a\u0001\u0000\u0000"+
    "\u0000\u029a\u029c\u0001\u0000\u0000\u0000\u029b\u0299\u0001\u0000\u0000"+
    "\u0000\u029c\u029d\u0005_\u0000\u0000\u029d\u02aa\u0001\u0000\u0000\u0000"+
    "\u029e\u029f\u0005^\u0000\u0000\u029f\u02a4\u0003\u008eG\u0000\u02a0\u02a1"+
    "\u0005=\u0000\u0000\u02a1\u02a3\u0003\u008eG\u0000\u02a2\u02a0\u0001\u0000"+
    "\u0000\u0000\u02a3\u02a6\u0001\u0000\u0000\u0000\u02a4\u02a2\u0001\u0000"+
    "\u0000\u0000\u02a4\u02a5\u0001\u0000\u0000\u0000\u02a5\u02a7\u0001\u0000"+
    "\u0000\u0000\u02a6\u02a4\u0001\u0000\u0000\u0000\u02a7\u02a8\u0005_\u0000"+
    "\u0000\u02a8\u02aa\u0001\u0000\u0000\u0000\u02a9\u027f\u0001\u0000\u0000"+
    "\u0000\u02a9\u0280\u0001\u0000\u0000\u0000\u02a9\u0283\u0001\u0000\u0000"+
    "\u0000\u02a9\u0284\u0001\u0000\u0000\u0000\u02a9\u0285\u0001\u0000\u0000"+
    "\u0000\u02a9\u0286\u0001\u0000\u0000\u0000\u02a9\u0287\u0001\u0000\u0000"+
    "\u0000\u02a9\u0288\u0001\u0000\u0000\u0000\u02a9\u0293\u0001\u0000\u0000"+
    "\u0000\u02a9\u029e\u0001\u0000\u0000\u0000\u02aa\u0085\u0001\u0000\u0000"+
    "\u0000\u02ab\u02ac\u0007\u0006\u0000\u0000\u02ac\u0087\u0001\u0000\u0000"+
    "\u0000\u02ad\u02b0\u0003\u008aE\u0000\u02ae\u02b0\u0003\u008cF\u0000\u02af"+
    "\u02ad\u0001\u0000\u0000\u0000\u02af\u02ae\u0001\u0000\u0000\u0000\u02b0"+
    "\u0089\u0001\u0000\u0000\u0000\u02b1\u02b3\u0007\u0004\u0000\u0000\u02b2"+
    "\u02b1\u0001\u0000\u0000\u0000\u02b2\u02b3\u0001\u0000\u0000\u0000\u02b3"+
    "\u02b4\u0001\u0000\u0000\u0000\u02b4\u02b5\u00056\u0000\u0000\u02b5\u008b"+
    "\u0001\u0000\u0000\u0000\u02b6\u02b8\u0007\u0004\u0000\u0000\u02b7\u02b6"+
    "\u0001\u0000\u0000\u0000\u02b7\u02b8\u0001\u0000\u0000\u0000\u02b8\u02b9"+
    "\u0001\u0000\u0000\u0000\u02b9\u02ba\u00055\u0000\u0000\u02ba\u008d\u0001"+
    "\u0000\u0000\u0000\u02bb\u02bc\u00054\u0000\u0000\u02bc\u008f\u0001\u0000"+
    "\u0000\u0000\u02bd\u02be\u0007\u0007\u0000\u0000\u02be\u0091\u0001\u0000"+
    "\u0000\u0000\u02bf\u02c0\u0007\b\u0000\u0000\u02c0\u02c1\u0005o\u0000"+
    "\u0000\u02c1\u02c2\u0003\u0094J\u0000\u02c2\u02c3\u0003\u0096K\u0000\u02c3"+
    "\u0093\u0001\u0000\u0000\u0000\u02c4\u02c5\u0003\u0018\f\u0000\u02c5\u0095"+
    "\u0001\u0000\u0000\u0000\u02c6\u02c7\u0005#\u0000\u0000\u02c7\u02cc\u0003"+
    "\u0098L\u0000\u02c8\u02c9\u0005=\u0000\u0000\u02c9\u02cb\u0003\u0098L"+
    "\u0000\u02ca\u02c8\u0001\u0000\u0000\u0000\u02cb\u02ce\u0001\u0000\u0000"+
    "\u0000\u02cc\u02ca\u0001\u0000\u0000\u0000\u02cc\u02cd\u0001\u0000\u0000"+
    "\u0000\u02cd\u0097\u0001\u0000\u0000\u0000\u02ce\u02cc\u0001\u0000\u0000"+
    "\u0000\u02cf\u02d0\u0003v;\u0000\u02d0\u0099\u0001\u0000\u0000\u0000B"+
    "\u00a5\u00ae\u00c9\u00d8\u00de\u00ed\u00f1\u00f6\u00fd\u00ff\u010d\u0115"+
    "\u0119\u0120\u0126\u012d\u0135\u013d\u0145\u0149\u014d\u0152\u015d\u0162"+
    "\u0166\u0174\u017f\u018d\u01a2\u01aa\u01ad\u01b2\u01bf\u01c5\u01cc\u01d7"+
    "\u01e5\u01eb\u01f6\u01ff\u0207\u020c\u0214\u0216\u021b\u0222\u0227\u022c"+
    "\u0236\u023c\u0244\u0246\u0251\u0258\u0263\u0268\u026a\u0276\u028e\u0299"+
    "\u02a4\u02a9\u02af\u02b2\u02b7\u02cc";
  public static final ATN _ATN =
    new ATNDeserializer().deserialize(_serializedATN.toCharArray());
  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}
