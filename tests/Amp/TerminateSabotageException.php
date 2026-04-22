<?php

namespace GoldLapel\Amp\Tests;

/**
 * Distinctive exception thrown by the test's terminate() override
 * (AmpGoldLapelTerminateThrows), so the test can tell it apart from
 * any real connect-failure type.
 *
 * Lives in its own file for PSR-4 autoloading.
 */
class TerminateSabotageException extends \RuntimeException
{
}
