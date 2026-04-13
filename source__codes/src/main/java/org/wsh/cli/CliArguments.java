package org.wsh.cli;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class CliArguments {
    private final String command;
    private final Map<String, String> options;
    private final List<String> positionals;

    private CliArguments(String command, Map<String, String> options, List<String> positionals) {
        this.command = command;
        this.options = options;
        this.positionals = positionals;
    }

    public static CliArguments parse(String[] args) {
        if (args.length == 0) {
            throw new IllegalArgumentException("Missing command. Expected one of: generate-data, run, compare.");
        }
        String command = args[0];
        Map<String, String> options = new HashMap<>();
        List<String> positionals = new ArrayList<>();
        for (int i = 1; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("--")) {
                String key = arg.substring(2);
                String value = "true";
                if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                    value = args[++i];
                }
                options.put(key, value);
            } else {
                positionals.add(arg);
            }
        }
        return new CliArguments(command, Map.copyOf(options), List.copyOf(positionals));
    }

    public String command() {
        return command;
    }

    public String option(String key, String defaultValue) {
        return options.getOrDefault(key, defaultValue);
    }

    public int optionInt(String key, int defaultValue) {
        return Integer.parseInt(options.getOrDefault(key, Integer.toString(defaultValue)));
    }

    public boolean optionBoolean(String key, boolean defaultValue) {
        return Boolean.parseBoolean(options.getOrDefault(key, Boolean.toString(defaultValue)));
    }

    public Map<String, String> options() {
        return options;
    }

    public List<String> positionals() {
        return positionals;
    }
}
