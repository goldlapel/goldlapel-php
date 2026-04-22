<?php

namespace GoldLapel\Amp\Tests;

/**
 * Userland stream wrapper whose stream_write throws. Used by
 * StartCleanupGuardTest and BannerFailureCleanupTest to simulate a broken
 * stderr (closed fd, disk full on a piped target, FPM after
 * fastcgi_finish_request() has detached stderr) so printBanner() raises
 * an exception that we can verify does not leak the instance into
 * $liveInstances.
 *
 * fwrite() on a stream wrapper that throws from stream_write propagates
 * the exception up to the caller.
 *
 * Lives in its own file for PSR-4 autoloading — both the sync test and
 * the Amp test need it, and inlining it in either would require
 * require_once gymnastics.
 */
class ThrowingStreamWrapper
{
    /** @var resource|null */
    public $context;

    private static bool $registered = false;

    public static function register(): void
    {
        if (!self::$registered) {
            stream_wrapper_register('throwing-banner', self::class);
            self::$registered = true;
        }
    }

    public static function unregister(): void
    {
        if (self::$registered) {
            stream_wrapper_unregister('throwing-banner');
            self::$registered = false;
        }
    }

    public function stream_open(string $path, string $mode, int $options, ?string &$opened_path): bool
    {
        return true;
    }

    public function stream_write(string $data): int
    {
        throw new \RuntimeException('Synthetic banner-write failure');
    }

    public function stream_close(): void
    {
    }

    public function stream_flush(): bool
    {
        return true;
    }

    public function stream_stat(): array|false
    {
        return false;
    }
}
