<?php

namespace GoldLapel\Laravel;

use GoldLapel\GoldLapel;
use Illuminate\Database\Connection;
use Illuminate\Support\ServiceProvider;

class GoldLapelServiceProvider extends ServiceProvider
{
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

            $port = $glConfig['port'] ?? GoldLapel::DEFAULT_PORT;
            $glOptions = $glConfig['config'] ?? [];
            $extraArgs = $glConfig['extra_args'] ?? [];
            $invalidationPort = $glConfig['invalidation_port'] ?? null;

            try {
                $upstream = buildUpstreamUrl($config);
                putenv('GOLDLAPEL_CLIENT=laravel');
                GoldLapel::start($upstream, $port, $glOptions, $extraArgs);
            } catch (\Exception $e) {
                logger()->warning("Gold Lapel failed to start for connection '{$name}': " . $e->getMessage());
                continue;
            }

            $this->glConnections[$name] = $invalidationPort;

            config([
                "database.connections.{$name}.host" => '127.0.0.1',
                "database.connections.{$name}.port" => $port,
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
                    $conn->setInvalidationPort($glConnections[$connName]);
                    return $conn;
                }

                return new \Illuminate\Database\PostgresConnection($connection, $database, $prefix, $config);
            });
        }
    }
}
