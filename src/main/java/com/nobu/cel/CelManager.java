package com.nobu.cel;

import com.google.api.expr.v1alpha1.Decl;
import com.google.api.expr.v1alpha1.Expr;
import org.jboss.logging.Logger;
import org.projectnessie.cel.Ast;
import org.projectnessie.cel.Env;
import org.projectnessie.cel.checker.Decls;
import org.projectnessie.cel.tools.Script;
import org.projectnessie.cel.tools.ScriptCreateException;
import org.projectnessie.cel.tools.ScriptHost;
import org.projectnessie.cel.types.jackson.JacksonRegistry;

import java.util.*;

import static org.projectnessie.cel.Env.newCustomEnv;


public class CelManager {

    private final SchemaValidatorMap map = new SchemaValidatorMap();
    private static final Logger LOG = Logger.getLogger(CelManager.class);

    public void addScript(String schema, String rule) {
        ScriptHost scriptHost = ScriptHost.newBuilder().build();
        try {
            Script script = scriptHost.buildScript(rule)
                    .withDeclarations(buildDecls(rule))
                    .build();
            map.put(schema, script);
        } catch (ScriptCreateException e) {
            e.printStackTrace();
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
                    decls.add(Decls.newVar(value, Decls.Any));
                }
                uniqueDeclNames.add(value);
            }

            callExpr(arg, decls, uniqueDeclNames);
        }

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
