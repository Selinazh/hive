package org.apache.hadoop.hive.ql.parse;

/*
  BUG:6149257 - This ensures only select queries and some Metadata READ-ONLY
  queries are run and there is no way to insert data into the tables through HS2
  or change their metadata (create/delete/alter). Other approach would be to change
  Hive.g, but that's more involved and not worth it.
 */
public class TableauSemanticAnalyzerHook extends AbstractSemanticAnalyzerHook {

  @Override
  public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast)
      throws SemanticException {

    boolean queryAllowed = false;

    switch (ast.getToken().getType()) {
      case HiveParser.TOK_QUERY:
        /*
          The following AST check ensures there is an TOK_INSERT with all its
          descendents as tokens without any identifiers. This happens only with
          select * statement. Anything like insert overwrite or CTAS would not
          have this tree and will be rejected. Sleect * is the primary use case
          for DataOut to work.
         */
        ASTNode node;
        for (int childCount = 0; childCount < ast.getChildCount(); childCount++) {
          node = (ASTNode) ast.getChild(childCount);
          if (node.getToken().getType() == HiveParser.TOK_INSERT) {
            ASTNode destination = (ASTNode) node.getChild(0);
            if (destination.getToken().getType() == HiveParser.TOK_DESTINATION) {
              ASTNode dirNode = (ASTNode) destination.getChild(0);
              if (dirNode.getToken().getType() == HiveParser.TOK_DIR) {
                ASTNode tmpFileNode = (ASTNode) dirNode.getChild(0);
                if (tmpFileNode.getToken().getType() == HiveParser.TOK_TMP_FILE) {
                  queryAllowed = true;
                }
              }
            }
          }
        }
        break;

      /*
        The following has to be allowed to ensure BI tools work fine through ODBC
        driver when used from Tableau.
      */
      case HiveParser.TOK_SWITCHDATABASE:
      case HiveParser.TOK_SHOWDATABASES:
      case HiveParser.TOK_SHOWTABLES:
      case HiveParser.TOK_SHOWCOLUMNS:
      case HiveParser.TOK_SHOWPARTITIONS:
      case HiveParser.TOK_SHOW_CREATETABLE:
      case HiveParser.TOK_DESCDATABASE:
      case HiveParser.TOK_DESCTABLE:
      case HiveParser.TOK_EXPLAIN:
        queryAllowed = true;
        break;

      default:
        break;
    }
    if (queryAllowed) {
      return ast;
    }
    throw new SemanticException("Operation not supported by HiveServer for Tableau");
  }
}
