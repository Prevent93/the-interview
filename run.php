<?php

/**
 * Here is some basic code that will launch the Laravel job item...
 *
 * Nothing here will need to be changed, but it does show how the class
 * gets launched.
 *
 */

require 'class_docsv.php';
require 'load_laravel.php';

$project = new doCSV();

$project->runJob(
    null, /* we don't have a real job, there is nothing to delete...*/
    array (
        'url'   => 'http://path.to.our/third_party/download.file',
        'client'=> 123
    )
);
