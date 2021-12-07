package com.fanxuankai.kafka.connect.transforms;

import com.google.common.base.CaseFormat;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;

/**
 * topic 格式转换
 *
 * @author fanxuankai
 */
public class CaseFormatRouter<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC = "Case format topic used <code>com.google.common.base.CaseFormat</code>.";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(CaseFormatRouter.ConfigName.FROM, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH, "From format.")
            .define(CaseFormatRouter.ConfigName.TO, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH, "To format.");

    private interface ConfigName {
        String FROM = "from";
        String TO = "to";
    }

    private String from;
    private String to;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        from = config.getString(CaseFormatRouter.ConfigName.FROM);
        to = config.getString(CaseFormatRouter.ConfigName.TO);
    }

    @Override
    public R apply(R record) {
        return record.newRecord(CaseFormat.valueOf(from).to(CaseFormat.valueOf(to), record.topic()),
                record.kafkaPartition(), record.keySchema(), record.key(),
                record.valueSchema(), record.value(), record.timestamp());
    }

    @Override
    public void close() {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
