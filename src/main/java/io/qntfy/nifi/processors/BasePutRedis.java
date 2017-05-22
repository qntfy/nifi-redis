package io.qntfy.nifi.processors;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Neil on 2017/5/22.
 * BasePutRedis
 */
public abstract class BasePutRedis extends AbstractProcessor {
    protected static final int DEFAULT_DATABASE = 0;
    protected static final int DEFAULT_MAX_TOTAL = 20;
    protected static final int DEFAULT_MAX_IDEL = 10;
    protected static final int DEFAULT_MIN_IDEL = 0;
    protected static final int DEFAULT_MAX_WAIT_MILLIES = -1;

    protected static final PropertyDescriptor REDIS_IP = new PropertyDescriptor.Builder()
            .name("redis-ip")
            .displayName("Redis IP")
            .description("Specifies the IP of Redis server to put data. " +
                    "For example, 127.0.0.1 . " +
                    "IP works with following property port as <IP>:<PORT>.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();

    protected static final PropertyDescriptor REDIS_PASSWORD = new PropertyDescriptor.Builder()
            .name("redis-password")
            .displayName("Redis Password")
            .description("Specifies the Password to use for put data into Redis server which has password security control.")
            .required(false)
            .expressionLanguageSupported(false)
            .build();

    protected static final PropertyDescriptor REDIS_DATABASE = new PropertyDescriptor.Builder()
            .name("redis-database")
            .displayName("Redis database")
            .description("Specified Database in Redis server. By DEFAULT it's 0. " +
                    "Notice that if password not set, the value of this configuration will be forced as 0.")
            .required(false)
            .defaultValue(DEFAULT_DATABASE + "")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();

    protected static final PropertyDescriptor REDIS_MAX_TOTAL = new PropertyDescriptor.Builder()
            .name("max-total-connection-pool")
            .displayName("Max Total Connections")
            .description("A configuration of JedisPool defined max total connections of the connection pool.")
            .required(false)
            .defaultValue(DEFAULT_MAX_TOTAL + "")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();

    protected static final PropertyDescriptor REDIS_MAX_IDEL = new PropertyDescriptor.Builder()
            .name("max-IDEL-of-connection-pool")
            .displayName("Max IDEL of Connection Pool")
            .description("A configuration of JedisPool defined max count idel connections of the connection pool.")
            .required(false)
            .defaultValue(DEFAULT_MAX_IDEL + "")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();

    protected static final PropertyDescriptor REDIS_MIN_IDEL = new PropertyDescriptor.Builder()
            .name("min-IDEL-of-connection-pool")
            .displayName("Min IDEL of Connection Pool")
            .description("A configuration of JedisPool defined min count idel connections of the connection pool.")
            .required(false)
            .defaultValue(DEFAULT_MIN_IDEL + "")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();

    protected static final PropertyDescriptor REDIS_MAX_WAIT_MILLIES = new PropertyDescriptor.Builder()
            .name("max-wait-millies-in-connection-pool")
            .displayName("Max wait millies")
            .description("A configuration of JedisPool defined max wait millies when no resource of the connection pool.")
            .required(false)
            .defaultValue(DEFAULT_MAX_WAIT_MILLIES + "")
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();

    protected static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("The Character Set in which the data is encoded")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    protected static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are written to Redis are routed to this relationship")
            .build();
    protected static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that cannot be written to Redis are routed to this relationship")
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(REDIS_IP);
        props.add(REDIS_PASSWORD);
        props.add(REDIS_DATABASE);
        props.add(CHARACTER_SET);
        props.add(REDIS_MAX_TOTAL);
        props.add(REDIS_MAX_IDEL);
        props.add(REDIS_MIN_IDEL);
        props.add(REDIS_MAX_WAIT_MILLIES);
        addSupportedPropertyDescriptors(props);
        return props;
    }

    protected abstract void addSupportedPropertyDescriptors(List<PropertyDescriptor> props);

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>(1);
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @OnScheduled
    public abstract void createRedisContext(final ProcessContext context);

    @OnStopped
    public abstract void destroyRedisContext(final ProcessContext context);
}
