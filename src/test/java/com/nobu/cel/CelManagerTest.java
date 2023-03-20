package com.nobu.cel;

import com.google.api.expr.v1alpha1.Decl;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class CelManagerTest {


    @Test
    public void testComplexExpression() {
        String expr = """
                account.balance >= transaction.withdrawal
                    || (overdraftProtection == true
                    && book.overdraftLimit >= transaction.withdrawal  - account.balance)
                """;
        var celManager = new CelManager();
        var decls = celManager.buildDecls(expr);

        var mapType = decls.stream().filter(decl -> decl.getName().equalsIgnoreCase("account")
                && decl.getIdent().getType().hasMapType());

        assert mapType.count() == 1;

        var anyType = decls.stream().filter(decl -> decl.getName().equalsIgnoreCase("overdraftProtection")
                && decl.getIdent().getType().hasPrimitive()
        );
        assert anyType.count() == 1;

    }

    @Test
    public void testSimpleExpression() {

        String expr = "event == 'signup' && user.phone == -20 && account.balance >= transaction.withdrawal";
        var celManager = new CelManager();
        var decls = celManager.buildDecls(expr);
        String[] expected = new String[]{"event", "user", "account", "transaction"};
        Arrays.sort(expected);
        var result = decls.stream().map(Decl::getName).sorted().toArray(String[]::new);
        assert Arrays.equals(expected, result);

        var typeCheck = decls.stream().filter(decl -> decl.getName().equalsIgnoreCase("user")
                && decl.getIdent().getType().hasMapType());

        assert typeCheck.count() == 1;

    }

    @Test
    public void testTopLevelPrimitive() {
        String expr = "event == 'signup' && phone > 200 && balance < 20.0 && active == true";
        var celManager = new CelManager();
        var decls = celManager.buildDecls(expr);
        assert decls.stream().filter(decl -> decl.getName().equalsIgnoreCase("balance")
                && decl.getIdent().getType().hasPrimitive()).count() == 1;
    }

}
