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
        private int $proxyPort = self::DEFAULT_PROXY_PORT;

        public static function reset(): void
        {
            self::$calls = [];
            self::$liveInstances = [];
        }

        public static function start(string $upstream, array $options = []): self
        {
            return self::spawn($upstream, $options);
        }

        public static function startProxyOnly(string $upstream, array $options = []): self
        {
            return self::spawn($upstream, $options);
        }

        /**
         * Records the call and stands in for the core's port allocation:
         * an explicit proxy_port is used as given, otherwise the smallest
         * P >= 7932 whose P and P+1 aren't held by a live spy instance.
         */
        private static function spawn(string $upstream, array $options): self
        {
            self::$calls[] = [
                'upstream' => $upstream,
                'port' => $options['proxy_port'] ?? null,
                'config' => $options['config'] ?? [],
                'extraArgs' => $options['extra_args'] ?? [],
                'logLevel' => $options['log_level'] ?? null,
                'mode' => $options['mode'] ?? null,
                'client' => $options['client'] ?? null,
                'options' => $options,
            ];
            $held = [];
            foreach (self::$liveInstances as $live) {
                $held[] = $live->proxyPort;
                $held[] = $live->proxyPort + 1;
            }
            $port = $options['proxy_port'] ?? self::DEFAULT_PROXY_PORT;
            if (!isset($options['proxy_port'])) {
                while (in_array($port, $held, true) || in_array($port + 1, $held, true)) {
                    $port++;
                }
            }
            $instance = new self();
            $instance->proxyPort = $port;
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
        public function getProxyPort(): int { return $this->proxyPort; }
        public static function cleanupAll(): void {}
    }
}

namespace {
    require __DIR__ . '/../../vendor/autoload.php';
}
