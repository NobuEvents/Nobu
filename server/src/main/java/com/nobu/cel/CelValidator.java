package com.nobu.cel;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nobu.spi.event.NobuEvent;
import com.nobu.schema.SchemaLoader;
import org.jboss.logging.Logger;
import org.projectnessie.cel.tools.ScriptException;


import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;


@Singleton
public class CelValidator implements Predicate<NobuEvent> {

    private static final Logger LOG = Logger.getLogger(CelValidator.class);

    @Inject
    SchemaLoader schemaLoader;

    // Inject singleton ObjectMapper (Quarkus provides this)
    @Inject
    ObjectMapper objectMapper;

    private static final TypeReference<HashMap<String, Object>> TYPE_REF = 
        new TypeReference<>() {};

    /***
     * Execute the CEL script for the schema
     * @param event The event to be validated
     * @return true if the event passes the validation
     */
    @Override
    public boolean test(NobuEvent event) {
        try {
            // Reuse singleton ObjectMapper
            Map<String, Object> map = objectMapper.readValue(
                event.getMessage(), TYPE_REF);
            var scripts = schemaLoader.getCelManager().getScript(event.getSrn());

            int trueCount = 0;
            for (var script : scripts) {
                if (script.execute(Boolean.class, map)) {
                    trueCount++;
                }
            }
            return trueCount == scripts.size();

        } catch (IOException | ScriptException e) {
            LOG.error("Error executing CEL script for nobu schema" + event.getSrn(), e);
        }
        return false;
    }
}
