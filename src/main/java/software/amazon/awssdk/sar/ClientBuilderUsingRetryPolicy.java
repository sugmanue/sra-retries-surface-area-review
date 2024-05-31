package software.amazon.awssdk.sar;

import software.amazon.awssdk.awscore.retry.AwsRetryPolicy;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * Building clients using {@link RetryPolicy} is still supported and backwards compatible.
 */
public final class ClientBuilderUsingRetryPolicy {

    /**
     * Using RetryMode to build retry policies is still supported.
     */
    public static DynamoDbClient clientWithRetryPolicy01() {
        return DynamoDbClient
                .builder()
                .overrideConfiguration(o -> o.retryPolicy(RetryMode.STANDARD))
                .build();
    }

    /**
     * Using concrete instances of retry policies is still supported.
     */
    public static DynamoDbClient clientWithRetryPolicy03() {
        RetryPolicy policy = AwsRetryPolicy.forRetryMode(RetryMode.ADAPTIVE)
                .toBuilder()
                .numRetries(120)
                .backoffStrategy(BackoffStrategy.none())
                .build();

        return DynamoDbClient
                .builder()
                .overrideConfiguration(o -> o.retryPolicy(policy))
                .build();
    }

    /**
     * Using the new {@link RetryMode#ADAPTIVE_V2} is <b>not</b> supported by retry policies. Attempts to use it will
     * throw an {@link UnsupportedOperationException} with a helpful message
     *
     * <pre>
     * throw new UnsupportedOperationException("ADAPTIVE_V2 is not supported by retry policies, use a RetryStrategy instead");
     * </pre>
     */
    public static DynamoDbClient clientWithRetryPolicy02() {
        // This call throws UnsupportedOperationException.
        return DynamoDbClient
                .builder()
                .overrideConfiguration(o -> o.retryPolicy(RetryMode.ADAPTIVE_V2))
                .build();
    }
}
