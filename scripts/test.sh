#!/usr/bin/env bash
# Run the PHP test suite and surface a skip-count summary at the end.
#
# Integration tests in tests/Amp/IntegrationTest.php (and a couple of
# siblings) skip cleanly when Postgres isn't reachable. That's valid
# behavior — we don't want to fail tests just because a developer doesn't
# have Postgres running locally — but PHPUnit's default summary line is
# easy to miss, so a developer might reasonably assume "all tests passed"
# when integration coverage never ran.
#
# This wrapper runs PHPUnit (both the default and Laravel suites, to mirror
# CI), captures the combined output (while still streaming it to the
# terminal in real time), then parses the trailing PHPUnit summary for the
# skip count and prints a highlighted reminder if any tests skipped.
#
# Exit code preserves the first non-zero status from either suite.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# Extra args forwarded to every phpunit invocation.
extra_args=("$@")

run_suite() {
    local config=$1
    shift
    # Tee to stderr for live streaming, capture for post-processing.
    # PIPESTATUS[0] preserves the phpunit exit code across the tee pipeline.
    local out
    out=$(vendor/bin/phpunit --configuration "$config" "$@" 2>&1 | tee /dev/stderr; exit "${PIPESTATUS[0]}")
    local rc=$?
    printf '%s' "$out"
    return "$rc"
}

# Run default suite first.
default_out=$(run_suite phpunit.xml "${extra_args[@]}")
default_rc=$?

# Then the Laravel suite (matches CI).
laravel_out=$(run_suite phpunit-laravel.xml "${extra_args[@]}")
laravel_rc=$?

# Combined output for skip parsing.
combined_out="${default_out}
${laravel_out}"

# Propagate the first failing exit code; 0 only if both passed.
if [ "$default_rc" -ne 0 ]; then
    rc=$default_rc
elif [ "$laravel_rc" -ne 0 ]; then
    rc=$laravel_rc
else
    rc=0
fi

# Parse skip count from PHPUnit summary line. Two formats to handle:
#   "Tests: 607, Assertions: 1234, Skipped: 8."
#   "OK, but incomplete, skipped, or risky tests! ... Skipped: 8."
# grep -oE isolates the "Skipped: N" token; a second grep extracts the int.
# Sum across both suites (default + Laravel) so every skip is counted.
skipped=0
while read -r n; do
    [ -n "$n" ] && skipped=$((skipped + n))
done < <(printf '%s\n' "$combined_out" | grep -oE 'Skipped: [0-9]+' | grep -oE '[0-9]+' || true)

# Only show the summary banner for successful runs with skips. On failure,
# PHPUnit's own output is what the developer needs to see.
if [ "$rc" -eq 0 ] && [ "$skipped" -gt 0 ]; then
    printf '\n\033[33m==========================================\033[0m\n'
    printf '\033[33m⚠  %d tests skipped\033[0m — integration tests require a local Postgres.\n' "$skipped"
    printf '   Set PGHOST / PGUSER / PGPASSWORD and re-run, or rely on the CI workflow\n'
    printf '   (.github/workflows/test.yml) which provisions postgres:16 automatically.\n'
    printf '\033[33m==========================================\033[0m\n\n'
fi

exit "$rc"
