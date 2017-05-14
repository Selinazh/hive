package org.apache.hadoop.hive.ql.parse;

/*
  YHIVE-549 - This ensures that except add jar/create function queries, it should be
  possible to run all other kinds of queries. If something goes wrong, the user is
  responsible for that action since ServerSideSecurity is enabled for HS2 also.
 */
public class RestrictUserCodeSemanticAnalyzerHook extends AbstractSemanticAnalyzerHook {

  @Override
  public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast)
    throws SemanticException {

    boolean queryAllowed = true;

    switch (ast.getToken().getType()) {

    case HiveParser.TOK_CREATEFUNCTION:
      queryAllowed = false;
      break;

    default:
      break;
    }

    if (queryAllowed) {
      return ast;
    }
    throw new SemanticException("Operation restricted by HiveServer2, cannot execute");
  }
}