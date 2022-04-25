package org.opensearch.sql.executor;

import lombok.RequiredArgsConstructor;

import java.util.Map;

@RequiredArgsConstructor
public class ExecutionEngineFactory {

    private final Map<String, ExecutionEngine> executionEngineMap;

    public ExecutionEngine getExecutionEngine(String connector) {
        return executionEngineMap.get(connector);
    }

}
