package com.nobu.cel;

import com.google.api.expr.v1alpha1.Decl;
import com.google.api.expr.v1alpha1.Type;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class CelManagerTest {


    @Test
    public void testComplexExpression() {
        String expr = """
                account.balance >= transaction.withdrawal
                    || (overdraftProtection
                    && book.overdraftLimit >= transaction.withdrawal  - account.balance)
                """;
        var celManager = new CelManager();
        var decls = celManager.buildDecls(expr);

        var mapType = decls.stream().filter(decl -> decl.getName().equalsIgnoreCase("account")
                && decl.getIdent().getType().hasMapType());

        assert mapType.count() == 1;

        var anyType = decls.stream().filter(decl -> decl.getName().equalsIgnoreCase("overdraftProtection")
                && decl.getIdent().getType().hasWellKnown()
                && decl.getIdent().getType().getWellKnown().equals(Type.WellKnownType.ANY)
        );
        assert anyType.count() == 1;

    }

    @Test
    public void testSimpleExpression() {

        String expr = "event == 'signup' && user.phone > 8";
        var celManager = new CelManager();
        var decls = celManager.buildDecls(expr);
        String[] expected = new String[]{"event", "user"};
        Arrays.sort(expected);
        var result = decls.stream().map(Decl::getName).sorted().toArray(String[]::new);
        assert Arrays.equals(expected, result);

        var typeCheck = decls.stream().filter(decl -> decl.getName().equalsIgnoreCase("user")
                && decl.getIdent().getType().hasMapType());

        assert typeCheck.count() == 1;
    }
}
