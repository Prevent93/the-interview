<?php

$path = '/path/to/root/of/laravel';
$config = require $path . '/app/config/database.php';

require $path . '/vendor/autoload.php';

// Make sure that eloquent is in our namespace.
use Illuminate\Database\Eloquent;

// Bootstrap Laravel
$app = require_once $path . '/bootstrap/start.php';
