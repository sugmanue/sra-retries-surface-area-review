package software.amazon.awssdk.sar;

import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.internal.retry.SdkDefaultRetryStrategy;
import software.amazon.awssdk.core.retry.RetryUtils;
import software.amazon.awssdk.retries.LegacyRetryStrategy;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.retries.api.RetryStrategy;

import java.io.IOException;
import java.net.SocketException;
import java.time.Duration;

public final class RetryStrategies {

    /**
     * The default retry strategy builder interface offers the common knobs for all retry strategies.
     *
     * @see RetryStrategy.Builder
     */
    public static RetryStrategy<?, ?> configureGenericRetryStrategy(RetryStrategy<?, ?> retryStrategy) {
        RetryStrategy.Builder<?, ?> builder = retryStrategy.toBuilder();

        builder
                //
                // Max attempts, this includes the initial attempt, for instance, for standard this
                // default to 3, that is, first attempt and up to two retries (1 + 2 = 3).
                .maxAttempts(3)
                //
                // Backoff strategy, there are several preconfigured ones defined in BackoffStrategy
                // the default is exponentialDelay with a base delay of 100ms and max delay of 20 seconds
                .backoffStrategy(BackoffStrategy.exponentialDelay(Duration.ofMillis(100L), Duration.ofSeconds(20)))
                //
                // And several ways of configuring which exception to retry on
                //
                // Direct exception test, this configuration will only retry on SocketException's
                .retryOnException(SocketException.class)
                //
                // Exception instance of, this configuration will retry on SocketException since it extends IOException
                .retryOnExceptionInstanceOf(IOException.class)
                //
                // Exception or cause, this configuration will retry if the exception is thrown like
                // `throw new RuntimeException(new SocketException("ups"))`
                // since `cause` is an SocketException but not if the exception is throw as
                // `throw new RuntimeException(new ConnectException("ups"))`
                // even though ConnectException extends SocketException
                .retryOnExceptionOrCause(SocketException.class)
                //
                // Exception or cause instance of, this configuration will retry if the exception is thrown like
                // `throw new RuntimeException(new SocketException("ups"))`
                // since `cause` is an SocketException which extends IOException.
                .retryOnExceptionOrCauseInstanceOf(IOException.class)
                //
                // On root cause traverses the cause chain until a match is found or a cause is null, e.g.,
                // this configuration will retry if the exception is throw like
                // `throw new RuntimeException(new UncheckedIOException(new IOException("ups")))`
                // *but not* if the exception is thrown like
                // `throw new RuntimeException(new UncheckedIOException(new SocketException("ups")))`
                // `SocketException extends IOException` but is not the same as
                .retryOnRootCause(IOException.class)
                //
                // On root cause traverses the cause chain until a match is found or a cause is null, e.g.,
                // this configuration will retry if the exception is throw like
                // `throw new RuntimeException(new UncheckedIOException(new IOException("ups")))`
                // *and also* if the exception is thrown like (SocketException is an instance of IOException)
                // `throw new RuntimeException(new UncheckedIOException(new SocketException("ups")))`
                .retryOnRootCauseInstanceOf(IOException.class);
        return builder.build();
    }

    /**
     * StandardRetryStrategy is the <em>recommended</em> strategy for the general use-case. This is the default for
     * clients used inside Amazon but not for external clients that still uses the LegacyRetryStrategy as default to
     * keep backwards compatibility from back before it was introduced.
     */
    public static StandardRetryStrategy configureStandardStrategy(StandardRetryStrategy strategy) {
        // StandardRetryStrategy.Builder is a type on its own that extends RetryStrategy.Builder
        StandardRetryStrategy.Builder builder = strategy.toBuilder();
        builder
                // Other than the common settings this builder also allows the user to disable the circuit-breaker.
                //
                // The circuit breaker will prevent attempts (even below the {@link #maxAttempts(int)}) if a large
                // number of failures are observed. This is implemented using a token bucket, if disabled the
                // retries will not be disabled even if the tokens has been exhausted.
                //
                // NOTE: do not change or disable this without careful consideration of its consequences, if the service
                // is having a bad day retrying unconditionally will most likely make its day worse, in particular
                // when used at Amazon scale.
                .circuitBreakerEnabled(Boolean.TRUE);
        return builder.build();
    }

    /**
     * LegacyRetryStrategy is also <em>recommended</em> strategy for the general use-case. It's characterized by
     * treating throttling exceptions differently from non-throttling ones. In particular, throttling exceptions do not
     * withdraw from the token bucket and those have a different backoff strategy. While this might sound like a good
     * idea some analysis showed that it does not perform better than the standard mode and might be more prone to cause
     * retry storms.
     */
    public static LegacyRetryStrategy configureLegacyStrategy(LegacyRetryStrategy strategy) {
        // LegacyRetryStrategy.Builder is a type on its own that extends RetryStrategy.Builder
        LegacyRetryStrategy.Builder builder = strategy.toBuilder()
                //
                // Legacy retry strategy also shares this setting but only applies for non-throttling exceptions.
                // Throttling exceptions don't affect the token bucket.
                .circuitBreakerEnabled(Boolean.TRUE)
                //
                // Legacy retries strategy need a way to distinguish throttling exceptions and is configured
                // the `LegacyRetryStrategy.Builder#treatAsThrottling` method.
                .treatAsThrottling(e -> (e instanceof SdkException) && RetryUtils.isThrottlingException((SdkException) e))
                //
                // Legacy retry strategy can also set a separated backoff strategy for throttling exceptions and
                // it can be configured using the `LegacyRetryStrategy.Builder#throttlingBackoffStrategy` method.
                .throttlingBackoffStrategy(BackoffStrategy.exponentialDelay(Duration.ofMillis(500L), Duration.ofSeconds(20)));
        return builder.build();
    }

    /**
     * An SDK pre-configured standard retry strategy can be obtained using
     * {@link SdkDefaultRetryStrategy#standardRetryStrategy()}.
     */
    public static StandardRetryStrategy sdkStrategyStandard() {
        return SdkDefaultRetryStrategy.standardRetryStrategy();
    }

    /**
     * An AWS SDK pre-configured standard retry strategy can be obtained using
     * {@link SdkDefaultRetryStrategy#standardRetryStrategy()}
     *
     * <p>
     * In addition to the SDK retry settings, this strategy also knows about AWS specific error codes to retry on.
     *
     * @see AwsRetryStrategy#retryOnAwsRetryableErrors
     */
    public static StandardRetryStrategy awsSdkStrategyStandard() {
        return AwsRetryStrategy.standardRetryStrategy();
    }
}
