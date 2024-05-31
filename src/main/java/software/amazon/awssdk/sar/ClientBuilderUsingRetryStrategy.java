package software.amazon.awssdk.sar;

import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.internal.retry.RetryPolicyAdapter;
import software.amazon.awssdk.core.internal.retry.SdkDefaultRetryStrategy;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.RetryUtils;
import software.amazon.awssdk.retries.AdaptiveRetryStrategy;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * Building clients using {@link RetryStrategy} is similar to using a {@link RetryPolicy}.
 */
public class ClientBuilderUsingRetryStrategy {
    /**
     * Using retry mode is handy and supported by
     * {@link ClientOverrideConfiguration.Builder#retryStrategy(RetryMode)}
     */
    public static DynamoDbClient clientWithRetryStrategy01() {
        return DynamoDbClient
                .builder()
                .overrideConfiguration(o -> o.retryStrategy(RetryMode.STANDARD))
                .build();
    }

    /**
     * The new {@link RetryMode#ADAPTIVE} is <em>also</em> supported by retry strategies, but we don't have a concrete
     * implementation of it, instead behind the scenes it uses the policy to strategy internal adapter
     * {@link RetryPolicyAdapter} built to support backwards compatibility but not expected to be used by customers (it
     * is an internal API).
     */
    public static DynamoDbClient clientWithRetryStrategy03() {
        // Does *not* throws UnsupportedOperationException.
        return DynamoDbClient
                .builder()
                .overrideConfiguration(o -> o.retryStrategy(RetryMode.ADAPTIVE))
                .build();
    }

    /**
     * RetryStrategy is an interface parametrized on its concrete implementation and its builder, the full signature of
     * the type is
     * {@snippet
     * public interface RetryStrategy<B extends CopyableBuilder<B, T> & RetryStrategy.Builder<B, T>,
     *                                T extends ToCopyableBuilder<B, T> & RetryStrategy<B, T>>
     *                      extends ToCopyableBuilder<B, T> { ⋯
     * }
     * }
     * <p>
     * This causes that when we don't use a concrete type of it we need to refer to it as {@code RetryStrategy<?, ?>}
     * 😔
     *
     * @see RetryStrategy
     * @see RetryStrategy.Builder
     */
    public static DynamoDbClient clientWithRetryStrategy04() {
        RetryStrategy<?, ?> strategy = AwsRetryStrategy.forRetryMode(RetryMode.ADAPTIVE_V2)
                // toBuilder RetryStrategy.Builder which can configure some basic properties
                .toBuilder()
                // Legacy numRetries(N) equivalent is
                // maxAttempts(N + 1) (includes the first attempt)
                .maxAttempts(121)
                // Legacy BackoffStrategy.none() equivalent is
                // BackoffStrategy.retryImmediately()
                .backoffStrategy(BackoffStrategy.retryImmediately())
                .build();

        return DynamoDbClient
                .builder()
                .overrideConfiguration(o -> o.retryStrategy(strategy))
                .build();
    }

    /**
     * We can create the concrete types using the corresponding methods in {@link AwsRetryStrategy} which builds on top
     * of {@link SdkDefaultRetryStrategy}, adding Sdk and AWS specific configurations to it.
     * <p>
     * Using the concrete types removes the need of using its types parameters and gives access to different settings in
     * the Builder.
     */
    public static DynamoDbClient clientWithRetryStrategy05() {
        // this will be "equivalent" to using RetryMode.ADAPTIVE_V2
        AdaptiveRetryStrategy strategy = AwsRetryStrategy.adaptiveRetryStrategy()
                // toBuilder returns AdaptiveRetryStrategy.Builder *not* RetryStrategy.Builder
                .toBuilder()
                // Legacy numRetries(N) equivalent is
                // maxAttempts(N + 1) (includes the first attempt)
                .maxAttempts(121)
                // Legacy BackoffStrategy.none() equivalent is
                // BackoffStrategy.retryImmediately()
                .backoffStrategy(BackoffStrategy.retryImmediately())
                // treatAsThrottling is only defined in AdaptiveRetryStrategy.Builder and
                // LegacyRetryStrategy.Builder. You cannot use if for, say, StandardRetryStrategy.Builder
                .treatAsThrottling(t -> t instanceof SdkException && RetryUtils.isThrottlingException((SdkException) t))
                .build();

        return DynamoDbClient
                .builder()
                .overrideConfiguration(o -> o.retryStrategy(strategy))
                .build();
    }

    /**
     * {@link AwsRetryStrategy#forRetryMode(RetryMode)} returns the super type of the retries strategies
     * {@code RetryStrategy<?, ?>} which cannot be configured using the specific type settings
     */
    public static DynamoDbClient clientWithRetryStrategy06() {
        RetryStrategy<?, ?> strategy = AwsRetryStrategy.forRetryMode(RetryMode.ADAPTIVE_V2)
                // toBuilder RetryStrategy.Builder which can configure some basic properties
                .toBuilder()
                // treatAsThrottling is only defined in AdaptiveRetryStrategy.Builder and
                // LegacyRetryStrategy.Builder. You cannot use if for, say, StandardRetryStrategy.Builder, in other words
                // if we uncomment the line above the code won't compile
                // .treatAsThrottling(t -> t instanceof SdkException && RetryUtils.isThrottlingException((SdkException) t))
                .build();

        // instead we need to create the specific type using
        AdaptiveRetryStrategy adaptiveRetryStrategy = AwsRetryStrategy.adaptiveRetryStrategy()
                .toBuilder()
                .treatAsThrottling(t -> t instanceof SdkException && RetryUtils.isThrottlingException((SdkException) t))
                .build();

        // or we need to cast the instance returned by `forRetryMode`, e.g.,
        RetryStrategy<?, ?> anotherAdaptiveRetryStrategy =
                ((AdaptiveRetryStrategy.Builder) AwsRetryStrategy.forRetryMode(RetryMode.ADAPTIVE_V2)
                        .toBuilder())
                        // treatAsThrottling is only defined in AdaptiveRetryStrategy.Builder and
                        // LegacyRetryStrategy.Builder. You cannot use if for, say, StandardRetryStrategy.Builder, in other words
                        // if we uncomment the line above the code won't compile
                        .treatAsThrottling(t -> t instanceof SdkException && RetryUtils.isThrottlingException((SdkException) t))
                        .build();

        return DynamoDbClient
                .builder()
                .overrideConfiguration(o -> o.retryStrategy(strategy))
                .build();
    }

    /**
     * The new {@link RetryMode#ADAPTIVE_V2} is <b>only</b> supported by retry strategies.
     */
    public static DynamoDbClient clientWithRetryStrategy02() {
        // Does *not* throws IllegalArgumentException.
        return DynamoDbClient
                .builder()
                .overrideConfiguration(o -> o.retryStrategy(RetryMode.ADAPTIVE_V2))
                .build();
    }
}
