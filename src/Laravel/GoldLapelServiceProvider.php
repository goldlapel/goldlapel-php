<?php

namespace GoldLapel\Laravel;

use GoldLapel\GoldLapel;
use Illuminate\Database\Connection;
use Illuminate\Support\ServiceProvider;

class GoldLapelServiceProvider extends ServiceProvider
{
    /**
     * Per-connection state for each Gold Lapel proxy spawned by boot().
     *
     * Keyed by Laravel connection name. Holds the live `GoldLapel` instance
     * so the terminating callback can call `->stop()` on it under long-lived
     * workers (Octane / Swoole / RoadRunner), plus the invalidation port the
     * matching `GoldLapelConnection` needs when it wraps its PDO with the L1
     * cache.
     *
     * @var array<string, array{proxy_port:int, invalidation_port:?int, instance:GoldLapel}>
     */
    private array $glConnections = [];

    public function boot(): void
    {
        $connections = config('database.connections', []);

        foreach ($connections as $name => $config) {
            if (($config['driver'] ?? '') !== 'pgsql') {
                continue;
            }

            $glConfig = $config['goldlapel'] ?? [];

            if (($glConfig['enabled'] ?? true) === false) {
                continue;
            }

            // YAML / config.php key follows the canonical snake_case name.
            $proxyPort = $glConfig['proxy_port'] ?? GoldLapel::DEFAULT_PROXY_PORT;
            $glOptions = $glConfig['config'] ?? [];
            $extraArgs = $glConfig['extra_args'] ?? [];
            $invalidationPort = $glConfig['invalidation_port'] ?? null;
            $logLevel = $glConfig['log_level'] ?? null;
            $mode = $glConfig['mode'] ?? null;

            try {
                $upstream = buildUpstreamUrl($config);
                // Use the connection-less factory variant — Laravel manages
                // its own PDOs via the Connection resolver below. We hold
                // onto the returned instance so the terminating callback
                // below can stop each subprocess deterministically at
                // worker shutdown (Octane/Swoole/RoadRunner).
                $startOptions = [
                    'proxy_port' => $proxyPort,
                    'client' => 'laravel',
                    'config' => $glOptions,
                    'extra_args' => $extraArgs,
                ];
                if ($invalidationPort !== null) {
                    $startOptions['invalidation_port'] = $invalidationPort;
                }
                if ($logLevel !== null) {
                    $startOptions['log_level'] = $logLevel;
                }
                if ($mode !== null) {
                    $startOptions['mode'] = $mode;
                }
                $instance = GoldLapel::startProxyOnly($upstream, $startOptions);
            } catch (\Exception $e) {
                logger()->warning("Gold Lapel failed to start for connection '{$name}': " . $e->getMessage());
                continue;
            }

            $this->glConnections[$name] = [
                'proxy_port' => $proxyPort,
                'invalidation_port' => $invalidationPort,
                'instance' => $instance,
            ];

            config([
                "database.connections.{$name}.host" => '127.0.0.1',
                "database.connections.{$name}.port" => $proxyPort,
                "database.connections.{$name}.url" => null,
                "database.connections.{$name}.sslmode" => 'prefer',
            ]);
        }

        if (!empty($this->glConnections)) {
            $glConnections = $this->glConnections;

            Connection::resolverFor('pgsql', function ($connection, $database, $prefix, $config) use ($glConnections) {
                $connName = $config['name'] ?? null;

                if ($connName !== null && array_key_exists($connName, $glConnections)) {
                    $conn = new GoldLapelConnection($connection, $database, $prefix, $config);
                    $conn->setInvalidationPort($glConnections[$connName]['invalidation_port']);
                    return $conn;
                }

                return new \Illuminate\Database\PostgresConnection($connection, $database, $prefix, $config);
            });

            // Register a terminating callback so Octane / Swoole / RoadRunner
            // worker shutdown releases each subprocess deterministically
            // rather than waiting for __destruct or the PHP shutdown hook
            // (which may never fire inside a long-lived worker until the
            // whole worker process exits).
            //
            // Octane invokes $app->terminate() between requests. We guard
            // against double-stop under that pattern by keying the callback
            // on this provider instance and stopping only instances still in
            // $this->glConnections — the first call clears them out.
            $this->app->terminating(function () {
                foreach ($this->glConnections as $name => $state) {
                    try {
                        $state['instance']->stop();
                    } catch (\Throwable $e) {
                        // Never let a stop() error abort worker shutdown.
                    }
                }
                $this->glConnections = [];
            });
        }
    }
}
