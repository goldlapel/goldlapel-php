<?php

namespace GoldLapel\Laravel;

use GoldLapel\GoldLapel;
use Illuminate\Support\ServiceProvider;

class GoldLapelServiceProvider extends ServiceProvider
{
    /**
     * Per-connection state for each Gold Lapel proxy spawned by boot().
     *
     * Keyed by Laravel connection name. Holds the live `GoldLapel` instance
     * so the terminating callback can call `->stop()` on it under long-lived
     * workers (Octane / Swoole / RoadRunner).
     *
     * @var array<string, array{proxy_port:int, instance:GoldLapel}>
     */
    private array $glConnections = [];

    public function boot(): void
    {
        $connections = config('database.connections', []);
        $proxies = []; // upstream URL => GoldLapel instance

        foreach ($connections as $name => $config) {
            if (($config['driver'] ?? '') !== 'pgsql') {
                continue;
            }

            $glConfig = $config['goldlapel'] ?? [];

            if (($glConfig['enabled'] ?? true) === false) {
                continue;
            }

            // The `goldlapel` block takes the same options as
            // GoldLapel::start(), passed through as-is (unset keys are left
            // to the core's defaults — notably proxy_port, which the core
            // allocates so several connections don't collide).
            $startOptions = array_filter(
                array_diff_key($glConfig, ['enabled' => true]),
                fn ($value) => $value !== null,
            ) + ['client' => 'laravel'];

            try {
                $upstream = buildUpstreamUrl($config);
                // Use the connection-less factory variant — Laravel opens
                // its own PDOs against the rewritten host/port. We hold
                // onto the returned instance so the terminating callback
                // below can stop each subprocess deterministically at
                // worker shutdown (Octane/Swoole/RoadRunner). Connections
                // pointing at the same database share one proxy; the first
                // connection's options win.
                $instance = $proxies[$upstream] ??= GoldLapel::startProxyOnly($upstream, $startOptions);
            } catch (\Exception $e) {
                logger()->warning("Gold Lapel failed to start for connection '{$name}': " . $e->getMessage());
                continue;
            }

            $proxyPort = $instance->getProxyPort();
            $this->glConnections[$name] = [
                'proxy_port' => $proxyPort,
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
                $stopped = [];
                foreach ($this->glConnections as $state) {
                    // A proxy shared by several connections stops once.
                    $id = spl_object_id($state['instance']);
                    if (isset($stopped[$id])) {
                        continue;
                    }
                    $stopped[$id] = true;
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
