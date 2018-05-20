<?php

/**
* SendBalancer
* 
* @desc The SendBalancer reads emails from the secondary database, and database connection
* settings for the mailservers from the primary database. It then establishes a connection
* to each mailserver and divides the emails to them, using an even distribution method.
* 
* Email status is kept in a small, separate table on all servers to avoid waiting times when
* updating the status.
*/

define('SEND_BALANCER_ROOT', realpath(dirname(__file__)));

require_once 'config.php';
require_once 'Logging.class.php';
require_once 'MailServerConnection.class.php';
require_once 'SendBalancer.class.php';

// Check the mutex.
$mutex = SEND_BALANCER_ROOT . '/sendbalancer.lck';
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

$sendBalancer = new SendBalancer();
$sendBalancer->start();

// Clear the mutex.
file_put_contents($mutex, 'unlocked');