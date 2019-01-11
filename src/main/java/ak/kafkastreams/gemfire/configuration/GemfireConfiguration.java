package ak.kafkastreams.gemfire.configuration;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.data.gemfire.cache.GemfireCacheManager;
import org.springframework.data.gemfire.client.ClientRegionFactoryBean;
import org.springframework.cloud.Cloud;
import org.springframework.cloud.CloudFactory;

import java.net.URI;

public class GemfireConfiguration {
    private static final String SECURITY_CLIENT = "security-client-auth-init";
    private static final String SECURITY_USERNAME = "security-username";
    private static final String SECURITY_PASSWORD = "security-password";

    @Bean
    public ClientCache gemfireCache() {
        Cloud cloud = new CloudFactory().getCloud();
        ServiceInfo serviceInfo = (ServiceInfo) cloud.getServiceInfos().get(0);
        ClientCacheFactory factory = new ClientCacheFactory();
        for (URI locator : serviceInfo.getLocators()) {
            factory.addPoolLocator(locator.getHost(), locator.getPort());
        }

        factory.set(SECURITY_CLIENT, "io.pivotal.pccpizza.config.UserAuthInitialize.create");
        factory.set(SECURITY_USERNAME, serviceInfo.getUsername());
        factory.set(SECURITY_PASSWORD, serviceInfo.getPassword());
        factory.setPdxSerializer(new ReflectionBasedAutoSerializer("io.pivotal.model.PizzaOrder"));
        factory.setPoolSubscriptionEnabled(true); // to enable CQ
        return factory.create();
    }

    @Bean
    public GemfireCacheManager cacheManager(ClientCache gemfireCache) {
        GemfireCacheManager cacheManager = new GemfireCacheManager();
        cacheManager.setCache(gemfireCache);
        return cacheManager;
    }

    @Bean(name = "PizzaOrder")
    ClientRegionFactoryBean<Long, String> orderRegion(@Autowired ClientCache gemfireCache) {
        ClientRegionFactoryBean<Long, String> orderRegion = new ClientRegionFactoryBean<>();

        orderRegion.setCache(gemfireCache);
        orderRegion.setClose(false);
        orderRegion.setShortcut(ClientRegionShortcut.PROXY);
        orderRegion.setLookupEnabled(true);

        return orderRegion;
    }

}
