<?php

/**
* Main send system file.
* 
* @desc This is the main file for sending emails from an email server.
* It uses pcntl_fork() to start a number of processes that will each
* reserve and send a number of emails. This works only on UNIX-like
* systems.
* 
* The sendsystem fetches emails from a local database and sends them using
* the local Postfix installation through PHPMailer (a PHP class).
* 
* It currently works as follows:
* - Upon startup, it checks its status in the primary database. If an entry
* for this host does not exist, it is created with the database connection
* settings for this host. That information is needed by the sendbalancer.
* - Status for this host is set to busy. If it is already busy, the script terminates
*   since it should not run in parallel.
* - All emails in the local database are divided equally between the processes.
* - PROCESS_COUNT processes are started.
* - Each process sends its mails and updates the newsletter status if necessary.
* - The emails table and email status table are both emptied if no emails remain.
*   The tables are also emptied if only emails that failed to send remain.
* - The script changes host status back to 'available' and terminates.
* 
* If this script is run in development mode, it does not run PROCESS_COUNT processes.
* Instead, if runs one process with id 1, which works on Windows. This process will
* pretend to send emails, but actually, it does not.
*/

define('SEND_SYSTEM_ROOT', realpath(dirname(__file__)));
set_include_path(get_include_path() . PATH_SEPARATOR . SEND_SYSTEM_ROOT . PATH_SEPARATOR . SEND_SYSTEM_ROOT . '/phpmailer');

$env = (((count($argv) >= 2) && ($argv[1] == 'dev')) ? 'dev' : 'prod');

require_once 'config.php';
require_once 'Logging.class.php';
require_once 'SendConfig.class.php';
require_once 'SendSystem.class.php';
require_once 'phpmailer/class.phpmailer.php';
require_once 'phpmailer/class.smtp.php';

$timeStart = microtime(true);

// List of children processes.
$children = array();
$status = '';

$sendConfig = new SendConfig();
$sendConfig->getLogFile()->write('Initializing parent process.');

// Register the mail server with the host, if it isn't already.
$isAvailable = $sendConfig->checkStatus('busy');
if($isAvailable)
{
  $myId = $sendConfig->getId();
}
else
{
  // Not allowed to run. Document it and exit.
  $sendConfig->getLogFile()->write('Not allowed to run because status is \'disabled\', \'failed\' or \'busy\', quitting.');
  exit;
}

// Initial check to see if there is any work.
if(!$sendConfig->hasEmails())
{
  $sendConfig->getLogFile()->write('There are no emails to send, quitting.');
  $sendConfig->checkStatus('available', false);
  exit;
}

// Divide the emails between the processes.
$sendConfig->reserveEmails();

if($env == 'dev')
{
  $sendSystem = new SendSystem($myId, 1);
  $sendSystem->start(true);
}
else
{
  // Create the defined number of processes.
  $sendConfig->getLogFile()->write('Creating ' . PROCESS_COUNT . ' working processes.');
  for($processId = 1; $processId <= PROCESS_COUNT; $processId++)
  {
    $pid = pcntl_fork();
    if($pid == 0)
    {
      // This is the child process. Do the work.
      $sendSystem = new SendSystem($myId, $processId);
      $sendSystem->start();
      unset($sendSystem);
      exit;
    }
    else
    {
      // This is the parent process. Add the child to the list.
      $children[] = $pid;
    }
  }

  // This is the parent process. Wait for the children to finish their work.
  // Then exit in an orderly fashion.
  foreach($children as $pid)
  {
    $pid = pcntl_wait($status);
    if(pcntl_wifexited($status))
    {
      // The process is finished.
      $code = pcntl_wexitstatus($status);
    }
    else
    {
      // Something went wrong.
      $sendConfig->getLogFile()->write('Process child finished with errors.');
    }
  }
}

// Reconnect.
$sendConfig = new SendConfig();
$sendConfig->connect();

// All sent emails are deleted.
$sendConfig->deleteEmails();

// Mark the mailserver as available.
$sendConfig->checkStatus('available', false);

$timeEnd = microtime(true);
$sendConfig->getLogFile()->write('Finished parent process. Total time spent: ' . number_format($timeEnd - $timeStart, 2) . ' seconds.');