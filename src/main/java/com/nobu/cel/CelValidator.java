package com.nobu.cel;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.expr.v1alpha1.Decl;
import com.nobu.event.NobuEvent;
import org.projectnessie.cel.Ast;
import org.projectnessie.cel.Env;
import org.projectnessie.cel.checker.Decls;
import org.projectnessie.cel.tools.Script;
import org.projectnessie.cel.tools.ScriptCreateException;
import org.projectnessie.cel.tools.ScriptException;
import org.projectnessie.cel.tools.ScriptHost;
import org.projectnessie.cel.types.jackson.JacksonRegistry;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.projectnessie.cel.Env.newCustomEnv;

public class CelValidator {


    public static boolean validate(final NobuEvent event) throws IOException, ScriptException {

        // Get Schema for the event type

        ObjectMapper mapper = new ObjectMapper();
        TypeReference<HashMap<String, Object>> typeRef
                = new TypeReference<>() {
        };
        Map<String, Object> map = mapper.readValue(event.getMessage(), typeRef);

        // System.out.println(map);

        //var scriptHost = ScriptHost.newBuilder().registry(JacksonRegistry.newRegistry()).build();

        Env env = newCustomEnv(JacksonRegistry.newRegistry(), List.of());

        String expr = """
                account.balance >= transaction.withdrawal
                    || (account.overdraftProtection
                    && account.overdraftLimit >= transaction.withdrawal  - account.balance)
                """;

        Env.AstIssuesTuple astIss = env.parse(expr);
        if (astIss.hasIssues()) {
            throw new ScriptCreateException("parse failed", astIss.getIssues());
        }
        Ast ast = astIss.getAst();

        ast.getSourceInfo().getPositionsMap().forEach((k, v) -> {
            System.out.println(k + " " + v);
        });

        //System.out.println(ast.getExpr());

        /*
        Script script = scriptHost.buildScript("event == signup")
                // Variable declarations - we need `inp` +  `checkName` in this example
                .withDeclarations(
                        //Decls.newVar("nobu", Decls.newObjectType(Map.class.getName())),
                        Decls.newVar("event", Decls.String),
                        Decls.newVar("signup", Decls.String)
                )
                // Register our Jackson object input type
                .withTypes(Map.class)
                .build();

        System.out.println("========");

        Map<String, Object> arguments = new HashMap<>();
        //arguments.put("nobu", map);
        arguments.put("event", "signup");

        Boolean result = script.execute(Boolean.class, arguments);
        System.out.println(result);*/

        return true;
    }

    public void sample() throws ScriptException {
        // Build the script factory
        ScriptHost scriptHost = ScriptHost.newBuilder().build();

        // create the script, will be parsed and checked
        Script script = scriptHost.buildScript("x == 'hello'")
                .withDeclarations(
                        // Variable declarations - we need `x` and `y` in this example
                        Decls.newVar("x", Decls.String),
                        Decls.newVar("y", Decls.String))
                //Decls.newVar("hello", Decls.String))
                .build();

        Map<String, Object> arguments = new HashMap<>();
        arguments.put("x", "hello");
        arguments.put("y", "world");

        Boolean result = script.execute(Boolean.class, arguments);

        System.out.println(result); // Prints "hello world"
    }

    public Boolean validateWithNobu(NobuEvent event) throws IOException, ScriptException {
        ObjectMapper mapper = new ObjectMapper();
        TypeReference<HashMap<String, Object>> typeRef
                = new TypeReference<>() {
        };
        Map<String, Object> map = mapper.readValue(event.getMessage(), typeRef);
        ScriptHost scriptHost = ScriptHost.newBuilder().build();
        Script script = scriptHost.buildScript("event == 'signup' && user.phone > 8")
                .withDeclarations(
                        // Variable declarations - we need `x` and `y` in this example
                        Decls.newVar("event", Decls.String),
                        Decls.newVar("client", Decls.String),
                        Decls.newVar("user", Decls.newMapType(Decls.String, Decls.Any))
                )
                .build();

        return script.execute(Boolean.class, map);

    }

}
