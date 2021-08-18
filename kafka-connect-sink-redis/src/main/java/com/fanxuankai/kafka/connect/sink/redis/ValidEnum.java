package com.fanxuankai.kafka.connect.sink.redis;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Validator is used to ensure that the input string is an element in the enum.
 */
class ValidEnum implements ConfigDef.Validator {
    final Set<String> validEnums;
    final Class<?> enumClass;

    /**
     * Method is used to create a new INSTANCE of the enum validator.
     *
     * @param enumClass Enum class with the entries to validate for.
     * @param excludes  Enum entries to exclude from the validator.
     * @return ValidEnum
     */
    public static ValidEnum of(Class<?> enumClass, String... excludes) {
        return new ValidEnum(enumClass, excludes);
    }

    private ValidEnum(Class<?> enumClass, String... excludes) {
        if (enumClass == null) {
            throw new IllegalStateException("enumClass cannot be null");
        }
        if (!enumClass.isEnum()) {
            throw new IllegalStateException("enumClass must be an enum.");
        }
        Set<String> validEnums = new LinkedHashSet<>();
        for (Object o : enumClass.getEnumConstants()) {
            String key = o.toString();
            validEnums.add(key);
        }
        validEnums.removeAll(Arrays.asList(excludes));
        this.validEnums = validEnums;
        this.enumClass = enumClass;
    }

    @Override
    public void ensureValid(String s, Object o) {
        if (o instanceof String) {
            if (!validEnums.contains(o)) {
                throw new ConfigException(
                        s,
                        String.format(
                                "'%s' is not a valid value for %s. Valid values are %s.",
                                o,
                                enumClass.getSimpleName(),
                                Arrays.stream(enumClass.getEnumConstants())
                                        .map(Object::toString)
                                        .collect(Collectors.joining(", "))
                        )
                );
            }
        } else if (o instanceof List) {
            List<?> list = (List<?>) o;
            for (Object i : list) {
                ensureValid(s, i);
            }
        } else {
            throw new ConfigException(
                    s,
                    o,
                    "Must be a String or List"
            );
        }
    }
}