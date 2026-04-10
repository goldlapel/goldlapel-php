<?php

namespace GoldLapel\Laravel;

use InvalidArgumentException;

function parseUrlIntoConfig(array $config): array
{
    $url = $config['url'] ?? null;
    if ($url === null || $url === '') {
        return $config;
    }

    $parts = parse_url($url);
    if ($parts === false) {
        return $config;
    }

    if (isset($parts['host'])) {
        $config['host'] = $parts['host'];
    }
    if (isset($parts['port'])) {
        $config['port'] = (string) $parts['port'];
    }
    if (isset($parts['user'])) {
        $config['username'] = rawurldecode($parts['user']);
    }
    if (isset($parts['pass'])) {
        $config['password'] = rawurldecode($parts['pass']);
    }
    if (isset($parts['path']) && $parts['path'] !== '/') {
        $config['database'] = rawurldecode(ltrim($parts['path'], '/'));
    }

    return $config;
}

function buildUpstreamUrl(array $config): string
{
    $config = parseUrlIntoConfig($config);

    $host = $config['host'] ?? '';
    if ($host === '') {
        $host = 'localhost';
    }

    if (str_starts_with($host, '/')) {
        throw new InvalidArgumentException(
            "Unix socket connections are not supported by Gold Lapel (host: {$host}). Use a TCP host instead."
        );
    }

    $port = $config['port'] ?? '';
    if ($port === '') {
        $port = '5432';
    }

    $database = $config['database'] ?? '';

    $user = $config['username'] ?? '';
    $password = $config['password'] ?? '';

    $userinfo = '';
    if ($user !== '') {
        $userinfo = rawurlencode($user);
        if ($password !== '') {
            $userinfo .= ':' . rawurlencode($password);
        }
        $userinfo .= '@';
    }

    $encodedDb = rawurlencode($database);

    return "postgresql://{$userinfo}{$host}:{$port}/{$encodedDb}";
}
