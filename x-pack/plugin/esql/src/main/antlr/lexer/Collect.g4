/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar Collect;

///
/// | COLLECT <name>
///
DEV_COLLECT : {this.isDevVersion()}? 'collect' -> pushMode(COLLECT_MODE);

mode COLLECT_MODE;

COLLECT_PIPE : PIPE -> type(PIPE), popMode;
COLLECT_QUOTED_IDENTIFIER: QUOTED_IDENTIFIER -> type(QUOTED_IDENTIFIER);
COLLECT_UNQUOTED_IDENTIFIER: UNQUOTED_IDENTIFIER -> type(UNQUOTED_IDENTIFIER);
COLLECT_LINE_COMMENT: LINE_COMMENT -> channel(HIDDEN);
COLLECT_MULTILINE_COMMENT: MULTILINE_COMMENT -> channel(HIDDEN);
COLLECT_WS: WS -> channel(HIDDEN);
