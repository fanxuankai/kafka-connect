package com.fanxuankai.kafka.connect.sink.redis;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

class ValidCharset implements ConfigDef.Validator {
    private final Set<String> allowedCharsets;

    public ValidCharset() {
        this(Charset.availableCharsets().keySet());
    }

    public ValidCharset(String... charsets) {
        this(Arrays.asList(charsets));
    }

    public ValidCharset(Iterable<String> charsets) {
        allowedCharsets = new HashSet<>();
        for (String charset : charsets) {
            allowedCharsets.add(charset);
        }
    }

    static void validate(String config, String value) {
        try {
            Charset.forName(value);
        } catch (Exception ex) {
            ConfigException configException = new ConfigException(
                    config, value, "Charset is invalid."
            );
            configException.initCause(ex);

            throw configException;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void ensureValid(String config, Object value) {
        if (value instanceof String) {
            validate(config, (String) value);
        } else if (value instanceof List) {
            List<String> values = (List<String>) value;
            for (String v : values) {
                validate(config, v);
            }
        } else {
            throw new ConfigException(config, value, "Must be a string or list.");
        }
    }

    @Override
    public String toString() {
        final List<String> sortedCharsets = this.allowedCharsets.stream()
                .sorted()
                .collect(Collectors.toList());
        return "Valid values: '" +
                String.join("', '", sortedCharsets) +
                '\'';
    }
}
