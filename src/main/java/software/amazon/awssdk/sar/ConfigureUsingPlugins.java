package software.amazon.awssdk.sar;

import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.core.SdkPlugin;
import software.amazon.awssdk.core.SdkServiceClientConfiguration;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;

import java.time.Duration;

/**
 * Plugins can be also used to configure {@link RetryStrategy}, per client and per request.
 */
public class ConfigureUsingPlugins {

    /**
     * Plugins can also be used to configure retry strategies.
     */
    public static DynamoDbClient clientWithRetryStrategy01() {
        StandardRetryStrategy retryStrategy = AwsRetryStrategy.standardRetryStrategy();
        return DynamoDbClient.builder()
                .addPlugin(new ConfigureRetryStrategy(retryStrategy))
                .build();
    }

    /**
     * Plugins can be also used to configure per request retry strategies. This can be used to make the adaptive retry
     * strategy useful as you can have one per resource (table) and operation that's used for all those calls
     */
    public static GetItemResponse perRequestRetryStrategyUsingPlugins(
            DynamoDbClient client,
            GetItemRequest.Builder getItemRequestBuilder
    ) {
        ConfigureRetryStrategy plugin = retryStrategyPlugin();
        return client.getItem(getItemRequestBuilder
                .overrideConfiguration(o -> o.addPlugin(plugin))
                .build());
    }

    private static ConfigureRetryStrategy retryStrategyPlugin() {
        return new ConfigureRetryStrategy(AwsRetryStrategy.adaptiveRetryStrategy());
    }

    /**
     * A plugin that configures the retry strategy of the client using the give at build time.
     */
    static class ConfigureRetryStrategy implements SdkPlugin {
        private final RetryStrategy<?, ?> strategy;

        public ConfigureRetryStrategy(RetryStrategy<?, ?> strategy) {
            this.strategy = strategy;
        }

        @Override
        public void configureClient(SdkServiceClientConfiguration.Builder builder) {
            // `o` here is an instance of ClientOverrideConfiguration.Builder. This method has the same
            // signature as `SdkClientBuilder.overrideConfiguration`. i.e.,
            // `overrideConfiguration(Consumer<ClientOverrideConfiguration.Builder> consumer)`
            builder.overrideConfiguration(o -> o.retryStrategy(strategy));

            // besides the retry strategy other things can be configured by a single plugin.
            builder.overrideConfiguration(o -> o.apiCallTimeout(Duration.ofSeconds(10)));
        }
    }
}
