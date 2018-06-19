package etl.server.config;


import org.springframework.core.env.Environment;

/**
 * application configuration
 */
public class AppConfig {

    public static final String IGNIS_HOST = "etl.host";

    public static final String IGNIS_HTTP_PORT = "server.port";

    private final Environment environment;


    public AppConfig(Environment environment) {
        this.environment = environment;
    }

    public String getHttpWebAddress(){
        return "http://" + environment.getProperty(IGNIS_HOST) + ":" + environment.getProperty(IGNIS_HTTP_PORT);
    }

    public String getStagingRemoteDir(){
        return environment.getProperty("staging.dataset.remotePath");
    }
}
