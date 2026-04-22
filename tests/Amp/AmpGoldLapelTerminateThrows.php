<?php

namespace GoldLapel\Amp\Tests;

use GoldLapel\Amp\GoldLapel;

/**
 * Subclass of the Amp factory that throws from terminate(). The factory's
 * start() catch block must swallow this and re-throw the original
 * connect exception — otherwise the user would see the wrong error.
 *
 * Requires `new static(...)` in startProxyInstance() and `protected`
 * visibility on terminate() for the override to take effect. See the
 * test-only comments in src/Amp/GoldLapel.php for why those shapes
 * are tolerated.
 */
class AmpGoldLapelTerminateThrows extends GoldLapel
{
    protected function terminate(): void
    {
        throw new TerminateSabotageException(
            'Synthetic teardown failure — must be swallowed, not propagated.'
        );
    }
}
