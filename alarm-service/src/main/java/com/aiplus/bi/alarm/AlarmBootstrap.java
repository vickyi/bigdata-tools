package com.aiplus.bi.alarm;

import com.aiplus.bi.log.JSchSLF4JLogger;
import org.apache.commons.cli.*;
import org.eclipse.jetty.server.Server;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.jetty.JettyHttpContainerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;

/**
 * 启动入口类.
 *
 * @author dev
 */
public class AlarmBootstrap {

    private static final String CMD_OPT_CONF = "conf";

    static {
        JSchSLF4JLogger.initJSchLogger();
    }

    public static void main(String[] args) {

        Options options = new Options();
        options.addOption(CMD_OPT_CONF, true, "configure file path");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        if (null == cmd) {
            System.out.println("Not parse command line args.");
            System.exit(-1);
        }
        final Configuration conf = new Configuration();
        if (cmd.hasOption(CMD_OPT_CONF)) {
            conf.loadFromFile(cmd.getOptionValue(CMD_OPT_CONF));
        } else {
            conf.loadFromClasspath();
        }

        URI baseUri = UriBuilder.fromUri("http://0.0.0.0").port(conf.getAlarmHttpPort()).build();
        ResourceConfig config = new ResourceConfig()
                .registerInstances(new AbstractBinder() {
                    @Override
                    protected void configure() {
                        bind(conf).to(Configuration.class);
                    }
                })
                .packages(false, new String[]{"com.aiplus.bi.alarm.resource"})
                .register(JacksonFeature.class);
        Server server = JettyHttpContainerFactory.createServer(baseUri, config);
        try {
            server.start();
            server.join();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
