package com.ramesh.ml;

import org.apache.hadoop.conf.Configuration;
public class ConfigurationFactory {
private ConfigurationFactory() {
}
private final static Configuration conf = new Configuration();
public static Configuration getInstance() {
return conf;
}
}