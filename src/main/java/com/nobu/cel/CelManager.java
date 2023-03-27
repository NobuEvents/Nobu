package com.nobu.cel;

import com.google.api.expr.v1alpha1.Decl;
import com.google.api.expr.v1alpha1.Expr;
import com.google.api.expr.v1alpha1.Type;
import org.jboss.logging.Logger;
import org.projectnessie.cel.Ast;
import org.projectnessie.cel.Env;
import org.projectnessie.cel.checker.Decls;
import org.projectnessie.cel.tools.Script;
import org.projectnessie.cel.tools.ScriptCreateException;
import org.projectnessie.cel.tools.ScriptHost;
import org.projectnessie.cel.types.jackson.JacksonRegistry;

import javax.inject.Singleton;
import java.util.*;

import static org.projectnessie.cel.Env.newCustomEnv;


@Singleton
public class CelManager {

  /**
   * A MultiMap of schema to list of scripts
   */
  private final SchemaValidatorMap map = new SchemaValidatorMap();

  private static final Logger LOG = Logger.getLogger(CelManager.class);

  public void addScript(String schema, String[] rules) {
    ScriptHost scriptHost = ScriptHost.newBuilder().build();
    Arrays.stream(rules).forEach(rule -> {
      try {
        var script = scriptHost.buildScript(rule)
            .withDeclarations(buildDecls(rule))
            .build();
        map.put(schema, script);
      } catch (ScriptCreateException e) {
        LOG.error("Error creating CEL script for schema:" + schema, e);
      }
    });
  }

  public List<Decl> buildDecls(String rule) {
    List<Decl> decls = new ArrayList<>();
    Env env = newCustomEnv(JacksonRegistry.newRegistry(), List.of());
    Env.AstIssuesTuple astIss = env.parse(rule);
    if (astIss.hasIssues()) {
      LOG.error("Error parsing schema validation rule:" + rule);
      return decls;
    }
    Ast ast = astIss.getAst();
    var callExpr = ast.getExpr().getCallExpr();
    return callExprRecursive(callExpr, ast.getExpr(), new HashSet<>(), decls);
  }

  private List<Decl> callExprRecursive(Expr.Call callExpr, Expr parentArg, Set<String> visitedNode, List<Decl> decls) {
    for (var argValue : callExpr.getArgsList()) {
      if (argValue.hasCallExpr()) {
        callExprRecursive(argValue.getCallExpr(), argValue, visitedNode, decls);
      } else {
        var argList = parentArg.getCallExpr().getArgsList();
        if (argList.size() != 2) {
          LOG.error("Error parsing schema validation rule:" + argList);
          return decls;
        }
        var left = argList.get(0);
        var right = argList.get(1);

        extractOperands(visitedNode, decls, left, right);
        extractIdent(visitedNode, decls, left, right);
      }
    }
    return decls;
  }

  private void extractIdent(Set<String> visitedNode, List<Decl> decls, Expr left, Expr right) {
    if (left.hasIdentExpr()) {
      String value = left.getIdentExpr().getName();
      if (!visitedNode.contains(value)) {
        decls.add(Decls.newVar(value, getType(right.getConstExpr().getConstantKindCase().name())));
      }
      visitedNode.add(value);
    }
  }

  private static void extractOperands(Set<String> visitedNode, List<Decl> decls, Expr left, Expr right) {
    if (left.hasSelectExpr()) {
      parseOperands(visitedNode, decls, left);
    }
    if (right.hasSelectExpr()) {
      parseOperands(visitedNode, decls, right);
    }
  }

  private static void parseOperands(Set<String> visitedNode, List<Decl> decls, Expr expr) {
    if (expr.hasSelectExpr() && expr.getSelectExpr().hasOperand() && expr.getSelectExpr().getOperand().hasIdentExpr()) {
      String value = expr.getSelectExpr().getOperand().getIdentExpr().getName();
      if (!visitedNode.contains(value)) {
        decls.add(Decls.newVar(value, Decls.newMapType(Decls.String, Decls.Any)));
      }
      visitedNode.add(value);
    }
  }

  private Type getType(String valueType) {
    if (valueType == null || valueType.isEmpty()) {
      return Decls.Any;
    }
    return switch (valueType.toLowerCase()) {
      case "string_value" -> Decls.String;
      case "int64_value" -> Decls.Int;
      case "double_value" -> Decls.Double;
      case "bool_value" -> Decls.Bool;
      default -> Decls.Any;
    };
  }

  public List<Script> getScript(String schema) {
    return map.getScript(schema);
  }

  private static class SchemaValidatorMap {
    private final Map<String, List<Script>> map = new HashMap<>();

    public void put(String key, Script value) {
      List<Script> values = map.computeIfAbsent(key, k -> new ArrayList<>());
      if (!values.contains(value)) {
        values.add(value);
      }
    }

    public List<Script> get(String schema) {
      return map.get(schema);
    }

    public List<Script> getScript(String schema) {
      return map.getOrDefault(schema, new ArrayList<>());
    }
  }
}
