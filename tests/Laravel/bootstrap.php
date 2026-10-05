<?php

// Define spy GoldLapel classes before the autoloader loads the real ones.
// This lets us test the service provider without needing the actual binary.

namespace GoldLapel {
    class GoldLapel
    {
        const DEFAULT_PROXY_PORT = 7932;

        public static array $calls = [];

        /** @var array<int, self> */
        public static array $liveInstances = [];

        public int $stopCalls = 0;
        private ?string $url = null;

        public static function reset(): void
        {
            self::$calls = [];
            self::$liveInstances = [];
        }

        public static function start(string $upstream, array $options = []): self
        {
            $port = $options['proxy_port'] ?? self::DEFAULT_PROXY_PORT;
            self::$calls[] = [
                'upstream' => $upstream,
                'port' => $port,
                'config' => $options['config'] ?? [],
                'extraArgs' => $options['extra_args'] ?? [],
                'logLevel' => $options['log_level'] ?? null,
                'mode' => $options['mode'] ?? null,
                'client' => $options['client'] ?? null,
            ];
            $instance = new self();
            $instance->url = "postgresql://localhost:{$port}/db";
            self::$liveInstances[spl_object_id($instance)] = $instance;
            return $instance;
        }

        public static function startProxyOnly(string $upstream, array $options = []): self
        {
            $port = $options['proxy_port'] ?? self::DEFAULT_PROXY_PORT;
            self::$calls[] = [
                'upstream' => $upstream,
                'port' => $port,
                'config' => $options['config'] ?? [],
                'extraArgs' => $options['extra_args'] ?? [],
                'logLevel' => $options['log_level'] ?? null,
                'mode' => $options['mode'] ?? null,
                'client' => $options['client'] ?? null,
            ];
            $instance = new self();
            $instance->url = "postgresql://localhost:{$port}/db";
            self::$liveInstances[spl_object_id($instance)] = $instance;
            return $instance;
        }

        public function stop(): void
        {
            $this->stopCalls++;
            unset(self::$liveInstances[spl_object_id($this)]);
        }

        public function url(): ?string { return $this->url; }
        public static function cleanupAll(): void {}
    }
}

namespace {
    require __DIR__ . '/../../vendor/autoload.php';
}
