<?php

/**
* Sortbalancer.
* 
* @desc This script should run on the webserver, but can also be ran on any other
* server with database access.
* 
* The script runs through all newsletter recipients and reserves them to all available
* sortservers in batches of a predefined size to get an even distribution.
* 
* It required PDO and PHP5 and should be called by a cron job every 5 minutes.
*/

define('SORT_BALANCER_ROOT', realpath(dirname(__file__)));

require_once 'config.php';
require_once 'Logging.class.php';
require_once 'SortBalancer.class.php';

// Check the mutex.
$mutex = SORT_BALANCER_ROOT . '/sortbalancer.lck';
if(is_readable($mutex))
{
  $mutexValue = trim(file_get_contents($mutex));
  if($mutexValue == 'locked')
  {
    exit;
  }
}

// Create the file.
file_put_contents($mutex, 'locked');

$sortBalancer = new SortBalancer();
$sortBalancer->start();

// Clear the mutex.
file_put_contents($mutex, 'unlocked');