import time

class TokenBucket(object):
    """Limits demand for a finite resource via keyed token buckets.
    A limiter manages a set of token buckets that have an identical
    rate, capacity, and storage backend. Each bucket is referenced
    by a key, allowing for the independent tracking and limiting
    of multiple consumers of a resource.
    Args:
        rate (float): Number of tokens per second to add to the
            bucket. Over time, the number of tokens that can be
            consumed is limited by this rate. Each token represents
            some percentage of a finite resource that may be
            utilized by a consumer.
        capacity (int): Maximum number of tokens that the bucket
            can hold. Once the bucket is full, additional tokens
            are discarded.
            The bucket capacity has a direct impact on burst duration.
            Let M be the maximum possible token request rate, r the
            token generation rate (tokens/sec), and b the bucket
            capacity.
            If r < M the maximum burst duration, in seconds, is:
                T = b / (M - r)
            Otherwise, if r >= M, it is not possible to exceed the
            replenishment rate, and therefore a consumer can burst
            at full speed indefinitely.
            The maximum number of tokens that any one burst may
            consume is:
                T * M
            See also: https://en.wikipedia.org/wiki/Token_bucket#Burst_size
        storage (token_bucket.StorageBase): A storage engine to use for
            persisting the token bucket data. The following engines are
            available out of the box:
                token_bucket.MemoryStorage
    """

    __slots__ = (
        '_rate',
        '_capacity',
        '_storage',
    )

    def __init__(self, rate, capacity):
        if not isinstance(rate, (float, int)):
            raise TypeError('rate must be an int or float')

        if rate <= 0:
            raise ValueError('rate must be > 0')

        if not isinstance(capacity, int):
            raise TypeError('capacity must be an int')

        if capacity < 1:
            raise ValueError('capacity must be >= 1')

        self._rate = rate
        self._capacity = capacity
        self._storage = MemoryStorage()
        
    def get_tokens_count(self):
        self._storage.replenish('bucket', self._rate, self._capacity)
        
        return self._storage.get_token_count('bucket')

    def consume(self, num_tokens=1):
        """Attempt to take one or more tokens from a bucket.
        If the specified token bucket does not yet exist, it will be
        created and initialized to full capacity before proceeding. 
        Keyword Args:
            num_tokens (int): The number of tokens to attempt to
                consume, defaulting to 1 if not specified. It may
                be appropriate to ask for more than one token according
                to the proportion of the resource that a given request
                will use, relative to other requests for the same
                resource.
        Returns:
            bool: True if the requested number of tokens were removed
            from the bucket (conforming), otherwise False (non-
            conforming). The entire number of tokens requested must
            be available in the bucket to be conforming. Otherwise,
            no tokens will be removed (it's all or nothing).
        """

        if num_tokens is None:
            raise TypeError('num_tokens may not be None')

        if num_tokens < 1:
            raise ValueError('num_tokens must be >= 1')

        self._storage.replenish('bucket', self._rate, self._capacity)
        return self._storage.consume('bucket', num_tokens)


class MemoryStorage():
    """In-memory token bucket storage engine.
    This storage engine is suitable for multi-threaded applications. For
    performance reasons, race conditions are mitigated but not completely
    eliminated. The remaining effects have the result of reducing the
    effective bucket capacity by a negligible amount. In practice this
    won't be noticeable for the vast majority of applications, but in
    the case that it is, the situation can be remedied by simply
    increasing the bucket capacity by a few tokens.
    """

    def __init__(self):
        self._buckets = {}

    def get_token_count(self, key):
        """Query the current token count for the given bucket.
        Note that the bucket is not replenished first, so the count
        will be what it was the last time replenish() was called.
        Args:
            key (str): Name of the bucket to query.
        Returns:
            float: Number of tokens currently in the bucket (may be
            fractional).
        """
        try:
            return self._buckets[key][0]
        except KeyError:
            pass

        return 0

    def replenish(self, key, rate, capacity):
        """Add tokens to a bucket per the given rate.
        This method is exposed for use by the token_bucket.Limiter
        class.
        """

        try:
            # NOTE(kgriffs): Correctness of this algorithm assumes
            #   that the calculation of the current time is performed
            #   in the same order as the updates based on that
            #   timestamp, across all threads. If an older "now"
            #   completes before a newer "now", the lower token
            #   count will overwrite the newer, effectively reducing
            #   the bucket's capacity temporarily, by a minor amount.
            #
            #   While a lock could be used to fix this race condition,
            #   one isn't used here for the following reasons:
            #
            #       1. The condition above will rarely occur, since
            #          the window of opportunity is quite small and
            #          even so requires many threads contending for a
            #          relatively small number of bucket keys.
            #       2. When the condition does occur, the difference
            #          in timestamps will be quite small, resulting in
            #          a negligible loss in tokens.
            #       3. Depending on the order in which instructions
            #          are interleaved between threads, the condition
            #          can be detected and mitigated by comparing
            #          timestamps. This mitigation is implemented below,
            #          and serves to further minimize the effect of this
            #          race condition to negligible levels.
            #       4. While locking introduces only a small amount of
            #          overhead (less than a microsecond), there's no
            #          reason to waste those CPU cycles in light of the
            #          points above.
            #       5. If a lock were used, it would only be held for
            #          a microsecond or less. We are unlikely to see
            #          much contention for the lock during such a short
            #          time window, but we might as well remove the
            #          possibility in light of the points above.

            tokens_in_bucket, last_replenished_at = self._buckets[key]

            now = time.monotonic()

            # NOTE(kgriffs): This will detect many, but not all,
            #   manifestations of the race condition. If a later
            #   timestamp was already used to update the bucket, don't
            #   regress by setting the token count to a smaller number.
            if now < last_replenished_at:  # pragma: no cover
                return

            self._buckets[key] = [
                # Limit to capacity
                min(
                    capacity,

                    # NOTE(kgriffs): The new value is the current number
                    #   of tokens in the bucket plus the number of
                    #   tokens generated since last time. Fractional
                    #   tokens are permitted in order to improve
                    #   accuracy (now is a float, and rate may be also).
                    tokens_in_bucket + (rate * (now - last_replenished_at))
                ),

                # Update the timestamp for use next time
                now
            ]

        except KeyError:
            self._buckets[key] = [capacity, time.monotonic()]

    def consume(self, key, num_tokens):
        """Attempt to take one or more tokens from a bucket.
        This method is exposed for use by the token_bucket.Limiter
        class.
        """

        # NOTE(kgriffs): Assume that the key will be present, since
        #   replenish() will always be called before consume().
        tokens_in_bucket = self._buckets[key][0]
        if tokens_in_bucket < num_tokens:
            return False

        # NOTE(kgriffs): In a multi-threaded application, it is
        #   possible for two threads to interleave such that they
        #   both pass the check above, while in reality if executed
        #   linearly, the second thread would not pass the check
        #   since the first thread was able to consume the remaining
        #   tokens in the bucket.
        #
        #   When this race condition occurs, the count in the bucket
        #   will go negative, effectively resulting in a slight
        #   reduction in capacity.
        #
        #   While a lock could be used to fix this race condition,
        #   one isn't used here for the following reasons:
        #
        #       1. The condition above will rarely occur, since
        #          the window of opportunity is quite small.
        #       2. When the condition does occur, the tokens will
        #          usually be quickly replenished since the rate tends
        #          to be much larger relative to the number of tokens
        #          that are consumed by any one request, and due to (1)
        #          the condition is very rarely likely to happen
        #          multiple times in a row.
        #       3. In the case of bursting across a large number of
        #          threads, the likelihood for this race condition
        #          will increase. Even so, the burst will be quickly
        #          negated as requests become non-conforming, allowing
        #          the bucket to be replenished.
        #       4. While locking introduces only a small amount of
        #          overhead (less than a microsecond), there's no
        #          reason to waste those CPU cycles in light of the
        #          points above.
        #       5. If a lock were used, it would only be held for
        #          less than a microsecond. We are unlikely to see
        #          much contention for the lock during such a short
        #          time window, but we might as well remove the
        #          possibility given the points above.

        self._buckets[key][0] -= num_tokens
        return True