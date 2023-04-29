package com.nobu.cel;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nobu.event.NobuEvent;
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

    /***
     * Execute the CEL script for the schema
     * @param event The event to be validated
     * @return true if the event passes the validation
     */
    @Override
    public boolean test(NobuEvent event) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            TypeReference<HashMap<String, Object>> typeRef = new TypeReference<>() {

            };
            Map<String, Object> map = mapper.readValue(event.getMessage(), typeRef);
            var scripts = schemaLoader.getCelManager().getScript(event.getSchema());

            int trueCount = 0;
            for (var script : scripts) {
                if (script.execute(Boolean.class, map)) {
                    trueCount++;
                }
            }
            return trueCount == scripts.size();

        } catch (IOException | ScriptException e) {
            LOG.error("Error executing CEL script for nobu schema" + event.getSchema(), e);
        }
        return false;
    }
}
