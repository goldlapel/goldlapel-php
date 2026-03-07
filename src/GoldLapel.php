<?php

namespace GoldLapel;

use RuntimeException;

class GoldLapel
{
    const DEFAULT_PORT = 7932;
    const STARTUP_TIMEOUT = 10.0;
    const STARTUP_POLL_INTERVAL = 0.05;

    private string $upstream;
    private int $port;
    private array $extraArgs;
    /** @var resource|null */
    private $process = null;
    private ?string $url = null;

    private static ?self $instance = null;
    private static bool $cleanupRegistered = false;

    public function __construct(string $upstream, ?int $port = null, array $extraArgs = [])
    {
        $this->upstream = $upstream;
        $this->port = $port ?? self::DEFAULT_PORT;
        $this->extraArgs = $extraArgs;
    }

    public function startProxy(): string
    {
        if ($this->process !== null && $this->isRunning()) {
            return $this->url;
        }

        $binary = self::findBinary();
        $cmd = array_merge(
            [$binary, '--upstream', $this->upstream, '--port', (string) $this->port],
            $this->extraArgs
        );

        $nullDevice = PHP_OS_FAMILY === 'Windows' ? 'NUL' : '/dev/null';
        $descriptors = [
            0 => ['file', $nullDevice, 'r'],
            1 => ['file', $nullDevice, 'w'],
            2 => ['pipe', 'w'],
        ];

        $pipes = [];
        $this->process = proc_open($cmd, $descriptors, $pipes);

        if (!is_resource($this->process)) {
            throw new RuntimeException("Failed to start Gold Lapel process");
        }

        $stderr = $pipes[2];
        stream_set_blocking($stderr, false);
        $stderrOutput = '';

        $deadline = hrtime(true) + (int) (self::STARTUP_TIMEOUT * 1e9);

        while (hrtime(true) < $deadline) {
            $chunk = @fread($stderr, 65536);
            if ($chunk !== false && $chunk !== '') {
                $stderrOutput .= $chunk;
            }

            $status = proc_get_status($this->process);
            if (!$status['running']) {
                break;
            }

            if (self::waitForPort('127.0.0.1', $this->port, 0.5)) {
                fclose($stderr);
                $this->url = self::makeProxyUrl($this->upstream, $this->port);
                return $this->url;
            }
        }

        // Failure path — drain remaining stderr, terminate, throw
        $chunk = @fread($stderr, 65536);
        if ($chunk !== false && $chunk !== '') {
            $stderrOutput .= $chunk;
        }
        fclose($stderr);

        $this->terminate();

        throw new RuntimeException(
            "Gold Lapel failed to start on port {$this->port} within " . self::STARTUP_TIMEOUT . "s.\nstderr: {$stderrOutput}"
        );
    }

    public function stopProxy(): void
    {
        if ($this->process === null) {
            return;
        }

        $this->terminate();
        $this->url = null;
    }

    public function getUrl(): ?string
    {
        return $this->url;
    }

    public function getPort(): int
    {
        return $this->port;
    }

    public function isRunning(): bool
    {
        if ($this->process === null) {
            return false;
        }

        $status = proc_get_status($this->process);
        return $status['running'];
    }

    // -- Static singleton --

    public static function start(string $upstream, ?int $port = null, array $extraArgs = []): string
    {
        if (self::$instance !== null && self::$instance->isRunning()) {
            if (self::$instance->upstream !== $upstream) {
                throw new RuntimeException(
                    'Gold Lapel is already running for a different upstream. ' .
                    'Call GoldLapel::stop() before starting with a new upstream.'
                );
            }
            return self::$instance->url;
        }

        self::$instance = new self($upstream, $port, $extraArgs);

        if (!self::$cleanupRegistered) {
            register_shutdown_function([self::class, 'cleanup']);
            self::$cleanupRegistered = true;
        }

        return self::$instance->startProxy();
    }

    public static function stop(): void
    {
        if (self::$instance !== null) {
            self::$instance->stopProxy();
            self::$instance = null;
        }
    }

    public static function proxyUrl(): ?string
    {
        return self::$instance?->url;
    }

    public static function cleanup(): void
    {
        self::stop();
    }

    // -- Static helpers --

    public static function findBinary(): string
    {
        $envPath = getenv('GOLDLAPEL_BINARY');
        if ($envPath !== false && $envPath !== '') {
            if (!is_file($envPath)) {
                throw new RuntimeException(
                    "GOLDLAPEL_BINARY points to {$envPath} but file not found"
                );
            }
            return $envPath;
        }

        $os = match (PHP_OS_FAMILY) {
            'Darwin' => 'darwin',
            'Linux' => 'linux',
            'Windows' => 'windows',
            default => PHP_OS_FAMILY,
        };

        $arch = match (php_uname('m')) {
            'x86_64', 'amd64' => 'x86_64',
            'aarch64', 'arm64' => 'aarch64',
            default => php_uname('m'),
        };

        $binaryName = "goldlapel-{$os}-{$arch}";
        if ($os === 'windows') {
            $binaryName .= '.exe';
        }
        $bundled = __DIR__ . '/../bin/' . $binaryName;
        if (is_file($bundled)) {
            return realpath($bundled);
        }

        $pathBinary = self::which('goldlapel');
        if ($pathBinary !== null) {
            return $pathBinary;
        }

        throw new RuntimeException(
            "Gold Lapel binary not found. Set GOLDLAPEL_BINARY env var, " .
            "install the platform-specific package, or ensure 'goldlapel' is on PATH."
        );
    }

    public static function makeProxyUrl(string $upstream, int $port): string
    {
        $withPort = '/^(postgres(?:ql)?:\/\/(?:.*@)?)([^\/:?#]+):(\d+)(.*)$/';
        $noPort = '/^(postgres(?:ql)?:\/\/(?:.*@)?)([^\/:?#]+)(.*)$/';

        if (preg_match($withPort, $upstream, $m)) {
            return $m[1] . 'localhost:' . $port . $m[4];
        }

        if (preg_match($noPort, $upstream, $m)) {
            return $m[1] . 'localhost:' . $port . $m[3];
        }

        return 'localhost:' . $port;
    }

    public static function waitForPort(string $host, int $port, float $timeout): bool
    {
        $deadline = hrtime(true) + (int) ($timeout * 1e9);

        while (hrtime(true) < $deadline) {
            $fp = @fsockopen($host, $port, $errno, $errstr, 0.5);
            if ($fp !== false) {
                fclose($fp);
                return true;
            }
            usleep((int) (self::STARTUP_POLL_INTERVAL * 1e6));
        }

        return false;
    }

    // -- Private helpers --

    private function terminate(): void
    {
        if ($this->process === null) {
            return;
        }

        $status = proc_get_status($this->process);
        if ($status['running']) {
            proc_terminate($this->process, 15); // SIGTERM

            $deadline = hrtime(true) + (int) (5 * 1e9);
            while (hrtime(true) < $deadline) {
                $status = proc_get_status($this->process);
                if (!$status['running']) {
                    break;
                }
                usleep(50000);
            }

            $status = proc_get_status($this->process);
            if ($status['running']) {
                proc_terminate($this->process, 9); // SIGKILL
            }
        }

        proc_close($this->process);
        $this->process = null;
    }

    private static function which(string $name): ?string
    {
        $path = getenv('PATH');
        if ($path === false) {
            return null;
        }

        $pathext = getenv('PATHEXT');
        $extensions = $pathext ? explode(';', $pathext) : [''];

        $dirs = explode(PATH_SEPARATOR, $path);
        foreach ($dirs as $dir) {
            foreach ($extensions as $ext) {
                $file = $dir . DIRECTORY_SEPARATOR . $name . $ext;
                if (is_file($file) && is_executable($file)) {
                    return $file;
                }
            }
        }

        return null;
    }
}
