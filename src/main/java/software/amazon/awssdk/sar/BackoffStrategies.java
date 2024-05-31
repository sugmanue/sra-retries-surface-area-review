package software.amazon.awssdk.sar;

import software.amazon.awssdk.retries.api.BackoffStrategy;

import java.time.Duration;

public final class BackoffStrategies {

    /**
     * BackoffStrategy is an interface with a single method, `computeDelay(int attempt)` that returns a {@link Duration}
     * instance.
     */
    public static Duration backoffStrategiesOnlyKnowHowToComputeDelays(BackoffStrategy strategy, int attempt) {
        return strategy.computeDelay(attempt);
    }

    /**
     * The instances are created using the static methods in the {@link BackoffStrategy} class.
     * <p>
     * {@link BackoffStrategy#exponentialDelay(Duration, Duration)} is the default backoff strategy. It returns a
     * duration for a random period of time between 0ms and an exponentially increasing amount of time between each
     * subsequent attempt of the same call.
     * <p>
     * Specifically, the first attempt waits 0ms, and each subsequent attempt waits between 0ms and
     * {@code min(maxDelay, baseDelay * (1 << (attempt - 2)))}. Notice that `(1 << (attempt - 2))` is another way to
     * spell `(attempt - 2)^2`
     */
    public static BackoffStrategy exponentialDelay() {
        // Default values for the exponential backoff strategy are:
        // - Base delay of 100 milliseconds
        Duration baseDelay = Duration.ofMillis(100);
        // - Max delay of 20 seconds
        Duration maxBackoffTime = Duration.ofSeconds(20);

        BackoffStrategy exponentialDelay = BackoffStrategy.exponentialDelay(baseDelay, maxBackoffTime);

        // Notice that the attempts will be capped to 30 to avoid integer overflows, so after the 30th attempt
        // the backoff will fall in the same range, `[0, min(maxBackoffTime, (baseDelay * (30 - 2)^2)]`
        int RETRIES_ATTEMPTED_CEILING = (int) Math.floor(Math.log(Integer.MAX_VALUE) / Math.log(2));
        assert RETRIES_ATTEMPTED_CEILING == 30;

        for (int attempt = 0; attempt < Integer.MAX_VALUE; attempt++) {
            int cappedRetries = Math.min(attempt, RETRIES_ATTEMPTED_CEILING);
            int delayCeiling = (int)
                    Math.min(baseDelay.multipliedBy(1L << (cappedRetries - 2)).toMillis(),
                            maxBackoffTime.toMillis());

            Duration delay = exponentialDelay.computeDelay(attempt);
            // The computed delay will **always** be in the range:
            if (delay.toMillis() >= 0 && delay.toMillis() <= delayCeiling) {
                throw new AssertionError("exponentialDelay returned a value outside the expected range: " + delay);
            }
        }
        return exponentialDelay;
    }

    /**
     * {@link BackoffStrategy#retryImmediately()} returns a backoff strategy that does always return
     * {@link Duration#ZERO}.
     */
    public static BackoffStrategy retryImmediately() {
        BackoffStrategy retryImmediately = BackoffStrategy.retryImmediately();
        for (int attempt = 0; attempt < Integer.MAX_VALUE; attempt++) {
            Duration delay = retryImmediately.computeDelay(attempt);
            // delay will be always Zero
            if (!delay.isZero()) {
                throw new AssertionError("retryImmediately returned a non-zero delay: " + delay);
            }
        }
        return retryImmediately;
    }


    /**
     * {@link BackoffStrategy#fixedDelay(Duration)} returns a backoff strategy that waits for a random (also known as
     * "jitter") period of time between 0ms and the provided delay
     */
    public static BackoffStrategy fixedDelay() {
        Duration upperDelayBound = Duration.ofMillis(200);
        BackoffStrategy fixedDelay = BackoffStrategy.fixedDelay(upperDelayBound);
        for (int attempt = 0; attempt < Integer.MAX_VALUE; attempt++) {
            Duration delay = fixedDelay.computeDelay(attempt);
            // The computed duration will be randomized between zero and the given upper delay bound.
            if (delay.compareTo(Duration.ZERO) >= 0 && delay.compareTo(upperDelayBound) <= 0) {
                throw new AssertionError("retryImmediately returned a non-zero delay: " + delay);
            }
        }
        return fixedDelay;
    }

    /**
     * {@link BackoffStrategy#exponentialDelayWithoutJitter(Duration, Duration)} returns a bckoff strategy that wait for
     * an exponentially increasing amount of time between each subsequent attempt of the same call, without adding any
     * jitter.
     * <p>
     * Specifically, the first attempt waits 0ms, and each subsequent attempt waits for
     * {@code min(maxDelay, baseDelay * (1 << (attempt - 2)))}
     */
    public static BackoffStrategy exponentialDelayWithoutJitter() {
        // - Base delay of 100 milliseconds
        Duration baseDelay = Duration.ofMillis(100);
        // - Max delay of 20 seconds
        Duration maxBackoffTime = Duration.ofSeconds(20);

        BackoffStrategy exponentialDelayWithoutJitter = BackoffStrategy.exponentialDelayWithoutJitter(baseDelay, maxBackoffTime);
        // Notice that the attempts will be capped to 30 to avoid integer overflows, so after the 30th attempt
        // the backoff will fall in the same range, `[0, min(maxBackoffTime, (baseDelay * (30 - 2)^2)]`
        int RETRIES_ATTEMPTED_CEILING = (int) Math.floor(Math.log(Integer.MAX_VALUE) / Math.log(2));
        assert RETRIES_ATTEMPTED_CEILING == 30;

        for (int attempt = 0; attempt < Integer.MAX_VALUE; attempt++) {
            Duration delay = exponentialDelayWithoutJitter.computeDelay(attempt);
            Duration expected;
            if (attempt == 1) {
                // For the first attempt the duration is always zero
                expected = Duration.ZERO;
            } else {
                long expectedInMs = Math.min(maxBackoffTime.toMillis(), baseDelay.toMillis() * (long) Math.pow(2, attempt - 2));
                // The expression below can also be expressed as
                long expectedInMsUsingShift = Math.min(maxBackoffTime.toMillis(), baseDelay.toMillis() * (1L << (attempt - 2)));
                assert expectedInMsUsingShift == expectedInMs;

                expected = Duration.ofMillis(expectedInMs);
            }
            // The computed delay will **always** be in the range:
            if (!delay.equals(expected)) {
                throw new AssertionError("exponentialDelayWithoutJitter returned an unexpected value: " + delay);
            }
        }
        return exponentialDelayWithoutJitter;
    }
}
