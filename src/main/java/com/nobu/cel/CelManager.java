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
        try {
            for (String rule : rules) {
                Script script = scriptHost.buildScript(rule)
                        .withDeclarations(buildDecls(rule))
                        .build();
                map.put(schema, script);
            }
        } catch (ScriptCreateException e) {
            LOG.error("Error creating CEL script for schema:" + schema, e);
        }
    }

    public List<Decl> buildDecls(String rule) {
        List<Decl> decls = new ArrayList<>();
        Set<String> uniqueDeclNames = new HashSet<>();
        Env env = newCustomEnv(JacksonRegistry.newRegistry(), List.of());
        Env.AstIssuesTuple astIss = env.parse(rule);
        if (astIss.hasIssues()) {
            LOG.error("Error parsing schema validation rule:" + rule);
            return decls;
        }
        Ast ast = astIss.getAst();
        callExpr(ast.getExpr(), decls, uniqueDeclNames);
        return decls;
    }

    public void callExpr(Expr expr, List<Decl> decls, Set<String> uniqueDeclNames) {
        if (!expr.hasCallExpr()) {
            return;
        }
        var argList = expr.getCallExpr().getArgsList(); // List<Expr>

        String name = null;
        for (Expr arg : argList) {

            if (arg.hasSelectExpr() && arg.getSelectExpr().hasOperand() && arg.getSelectExpr().getOperand().hasIdentExpr()) {
                String value = arg.getSelectExpr().getOperand().getIdentExpr().getName();
                if (!uniqueDeclNames.contains(value)) {
                    decls.add(Decls.newVar(value, Decls.newMapType(Decls.String, Decls.Any)));
                }
                uniqueDeclNames.add(value);
            } else if (arg.hasIdentExpr()) {
                String value = arg.getIdentExpr().getName();
                if (!uniqueDeclNames.contains(value)) {
                    name = value;
                }
                uniqueDeclNames.add(name);
            } else if (arg.hasConstExpr()) {
                if (name != null) {
                    decls.add(Decls.newVar(name, getType(arg.getConstExpr().getConstantKindCase().name())));
                    name = null;
                }
            }
            callExpr(arg, decls, uniqueDeclNames);
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
