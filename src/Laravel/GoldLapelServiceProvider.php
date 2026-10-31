<?php

namespace GoldLapel\Laravel;

use GoldLapel\GoldLapel;
use Illuminate\Support\ServiceProvider;

class GoldLapelServiceProvider extends ServiceProvider
{
    /**
     * The GoldLapel instance behind each Laravel connection, keyed by
     * connection name. Connections pointing at the same database share one
     * instance (the core reuses a running proxy per upstream), and each
     * holds it once, so each one's stop() releases its own hold.
     *
     * @var array<string, GoldLapel>
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

            // The `goldlapel` block takes the same options as
            // GoldLapel::start(), passed through as-is (unset keys are left
            // to the core's defaults — notably proxy_port, which the core
            // allocates so several connections don't collide).
            // Requests and commands share one proxy per machine (see the
            // core's attachShared()); a test run keeps its own, so nothing
            // lingers to hold its databases.
            $startOptions = array_filter(
                array_diff_key($glConfig, ['enabled' => true]),
                fn ($value) => $value !== null,
            ) + ['client' => 'laravel', 'shared' => !$this->app->runningUnitTests()];

            try {
                $upstream = buildUpstreamUrl($config);
                // The connection-less factory variant — Laravel opens its
                // own PDOs against the rewritten host/port. Connections
                // pointing at the same database get the same running proxy
                // back from the core; the first connection's options win.
                // Started from the app's root, whichever directory the request
                // began in (FPM: public/): that's where its .env and
                // goldlapel.toml are, for the proxy and for the shared key.
                $cwd = getcwd();
                @chdir($this->app->basePath());
                try {
                    $instance = GoldLapel::startProxyOnly($upstream, $startOptions);
                } finally {
                    if ($cwd !== false) {
                        @chdir($cwd);
                    }
                }
            } catch (\Throwable $e) {
                // \Throwable, not \Exception: a mistyped option value
                // raises \TypeError, which must not take the app down.
                logger()->warning("Gold Lapel failed to start for connection '{$name}': " . $e->getMessage());
                continue;
            }

            $this->glConnections[$name] = $instance;

            config([
                "database.connections.{$name}.host" => '127.0.0.1',
                "database.connections.{$name}.port" => $instance->getProxyPort(),
                "database.connections.{$name}.url" => null,
                "database.connections.{$name}.sslmode" => 'prefer',
            ]);
        }

        if (empty($this->glConnections)) {
            return;
        }

        $stop = function () {
            foreach ($this->glConnections as $instance) {
                try {
                    $instance->stop();
                } catch (\Throwable $e) {
                    // Never let one stop() error keep the rest running.
                }
            }
            $this->glConnections = [];
        };

        if (isset($_SERVER['LARAVEL_OCTANE'])) {
            // Octane boots this provider once per worker but runs the
            // terminating callbacks after every request, so stopping there
            // would leave every later request pointed at a dead proxy. The
            // proxies live as long as the worker and stop with it.
            $this->app['events']->listen(\Laravel\Octane\Events\WorkerStopping::class, $stop);
        } else {
            // One request (PHP-FPM) or one console command: stop when the
            // app terminates (for a shared proxy, let go of it). The
            // process-exit hook in the core would too; this just makes it
            // deterministic.
            $this->app->terminating($stop);
        }
    }
}
