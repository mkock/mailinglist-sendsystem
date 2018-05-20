<?php

/**
* SendSystem.
* 
* @desc This is the main email sendsystem.
* PDO is used for the database connection.
* It uses a log file for basic errors that cannot be written to the database.
* Usage is simple - just call method start().
*/

require_once 'config.php';
require_once 'Logging.class.php';

class SendSystem
{
  /**
  * Mailserver Id.
  */
  protected $id;

  /**
  * Unique Id for this process.
  */
  protected $sendId;
  
  /**
  * No emails are sent in development mode.
  */
  protected $developmentMode;
  
  /**
  * File resource.
  */
  protected $logFile;
  
  /**
  * Connection to the local database.
  */
  protected $localDbConnection;
  
  /**
  * Connection to the primary database.
  */
  protected $primaryDbConnection;
  
  /**
  * Name of host.
  */
  protected $host;
  
  /**
  * PHPMailer object, used for sending emails through SMTP.
  */
  protected $mail;
  
  /**
  * Array that contains an entry for each newsletter id with the number of sent emails.
  */
  protected $nrSent;
  
  /**
  * Array that contains an entry for each newsletter id with the number of failed emails.
  */
  protected $nrFailed;
  
  protected $isDisabledStatement;
  
  protected $hasEmailsStatement;
  
  protected $updateStatsStatement;
  
  protected $selectEmailIdsStatement;
  
  protected $selectEmailStatement;
  
  protected $updateStatusStatement;
  
  protected $updateLetterStatusStatement;
  
  protected $createStatsStatement;
  
  protected $checkQueueSizeStatement;
  
  protected $updateMailServerStatement;
  
  /**
  * Initializes the send procedure for one process.
  * 
  * @desc Connects to the STMP server and sets basic settings.
  * 
  * @param Integer Mailserver ID (must match that given in the database).
  * @param Integer Process ID (can be any number).
  * $param Integer The number of available mailservers (used for calculations).
  */
  public function __construct($anId, $aSendId)
  {
    $this->id = $anId;
    $this->sendId = $aSendId;
    $this->connect();
    $this->logFile = new Logging('sendsystem.log');
    $this->mail = new PHPMailer();
    $this->mail->CharSet = 'UTF-8';
    $this->mail->SMTPKeepAlive = true;
    $this->mail->IsSMTP();
    $this->mail->Host = "localhost";
    $this->mail->SMTPAuth = false;
    $this->nrSent = array();
    $this->nrFailed = array();
    $host = php_uname('n');
    $this->host = substr($host, 0, (strpos($host, '.') ? strpos($host, '.') : strlen($host)));
    $this->prepareStatements();
  }
  
  public function __destruct()
  {
    $this->disconnect();
  }
  
  public function getPaddedId()
  {
    return str_pad($this->sendId, strlen(PROCESS_COUNT), '0', STR_PAD_LEFT);
  }
  
  /**
  * Main method.
  * 
  * @desc Main method for sending emails. It should only be necessary to call this.
  * 
  * @param Boolean Set to TRUE to run this script in development mode (no emails are actually sent).
  */
  public function start($aDevelopmentMode = false)
  {
    $this->developmentMode = $aDevelopmentMode;
    $this->logFile->write('Process ' . $this->getPaddedId() . ': Initializing.');

    // Initial update of queue size.
    $this->updateQueueSize();
    
    try
    {
      while($this->hasReservedEmails())
      {
        // Initialization.
        $this->nrSent = array();

        // Send the emails.
        $this->sendEmails();
        $this->registerAmountSent();
        
        // Check mailserver status.
        if($this->isDisabled())
        {
          $this->logFile->write('Process ' . $this->getPaddedId() . ': Asked to quit by host.');
          break;
        }
        
        // Tell the sendbalancer how many emails this server has in the queue.
        $this->updateQueueSize();
      }
    }
    catch(PDOException $someException)
    {
      $this->logFile->write('Process ' . $this->getPaddedId() . ': Database error: ' . $someException->getMessage());
      return;
    }
    
    $this->logFile->write('Process ' .$this->getPaddedId() .  ': Nothing more to send, quitting.');
  }

  /**
  * @desc Returns if any emails were reserved by this mail server.
  */
  private function hasReservedEmails()
  {
    $this->hasEmailsStatement->execute();
    $hasEmails = $this->hasEmailsStatement->fetchAll(PDO::FETCH_ASSOC);
    return (count($hasEmails) > 0);
  }
  
  /**
  * Prepare statements.
  * 
  * @desc Prepares the SQL commands needed by this script.
  */
  private function prepareStatements()
  {
    // Prepare the SQL command to check if this mailserver has been disabled.
    $sql = 'SELECT status FROM mailservers WHERE host = :host';
    $this->isDisabledStatement = $this->primaryDbConnection->prepare($sql);
    $this->isDisabledStatement->bindValue(':host', $this->host, PDO::PARAM_STR);
    
    // Prepare the SQL command to check if this process has any reserved emails.
    $sql = 'SELECT id FROM local_email_status WHERE reserved_by = :processId AND status = :status LIMIT 1';
    $this->hasEmailsStatement = $this->localDbConnection->prepare($sql);
    $this->hasEmailsStatement->bindValue(':processId', $this->sendId, PDO::PARAM_INT);
    $this->hasEmailsStatement->bindValue(':status', 'reserved', PDO::PARAM_STR);
    
    // Prepare the SQL command to update letterstats.
    $sql = 'UPDATE newsletters SET nr_sent = nr_sent + :nrSent, nr_failed = nr_failed + :nrFailed WHERE id = :letterId';
    $this->updateStatsStatement = $this->primaryDbConnection->prepare($sql);

    // Prepare the SQL command to update letter status.
    $sql = 'UPDATE newsletters SET status = :status WHERE id = :letterId';
    $this->updateLetterStatusStatement = $this->primaryDbConnection->prepare($sql);
    $this->updateLetterStatusStatement->bindValue(':status', 'sending', PDO::PARAM_STR);
    
    // Prepare the SQL command to fetch reserved email ids.
    $sql = 'SELECT id, email_ref_id, failure_count FROM local_email_status WHERE reserved_by = :processId ORDER BY time_to_send LIMIT ' . MAX_BATCH_SIZE;
    $this->selectEmailIdsStatement = $this->localDbConnection->prepare($sql);
    $this->selectEmailIdsStatement->bindParam(':processId', $this->sendId, PDO::PARAM_INT);
    
    // Prepare the SQL command to select emails.
    $sql = 'SELECT newsletter_ref_id, envelope_sender, recipient, header, body FROM local_emails WHERE id = :emailId';
    $this->selectEmailStatement = $this->localDbConnection->prepare($sql);
    
    // Prepare the SQL command to update emails in case of failure.
    $sql = 'UPDATE local_email_status SET status = :status, reserved_by = NULL, failure_count = failure_count + :aFailure WHERE id = :id';
    $this->updateStatusStatement = $this->localDbConnection->prepare($sql);
    
    // Prepare the SQL command to update statistics.
    $sql = 'INSERT INTO letterstats (id, letter_ref_id, type, data, created_at) VALUES (NULL, :letterId, :type, NULL, NOW())';
    $this->createStatsStatement = $this->primaryDbConnection->prepare($sql);
    
    // Prepare the SQL command to update queue count.
    $sql = 'SELECT COUNT(*) AS queue_size FROM local_email_status WHERE status != :failed AND status != :sent AND time_to_send <= ADDTIME(NOW(), \'00:05:00\')';
    $this->checkQueueSizeStatement = $this->localDbConnection->prepare($sql);
    $this->checkQueueSizeStatement->bindValue(':failed', 'failed', PDO::PARAM_STR);
    $this->checkQueueSizeStatement->bindValue(':sent', 'sent', PDO::PARAM_STR);
    
    // Prepare the SQL command to update the mailserver status.
    $sql = 'UPDATE mailservers SET nr_to_send = :nrToSend WHERE id = :id';
    $this->updateMailServerStatement = $this->primaryDbConnection->prepare($sql);
    $this->updateMailServerStatement->bindValue(':id', $this->id, PDO::PARAM_INT);
  }
  
  /**
  * Send emails.
  * 
  * @desc This method will send emails as long as there are some which are reserved by this process.
  */
  private function sendEmails()
  {
    $nrSent = 0;
    $timeStart = microtime(true);

    // Bind some variables to the prepared statements.
    $this->selectEmailStatement->bindParam(':emailId', $emailId, PDO::PARAM_INT);
    $this->updateStatusStatement->bindParam(':status', $status, PDO::PARAM_STR);
    $this->updateStatusStatement->bindParam(':id', $id, PDO::PARAM_INT);
    $this->updateStatusStatement->bindParam(':aFailure', $isFailed, PDO::PARAM_INT);
    $this->createStatsStatement->bindParam(':letterId', $letterId, PDO::PARAM_INT);
    $this->createStatsStatement->bindParam(':type', $type, PDO::PARAM_STR);
    
    // Get the emails.
    $this->selectEmailIdsStatement->execute();
    $emailKeys = $this->selectEmailIdsStatement->fetchAll(PDO::FETCH_ASSOC);
    
    // Iterate over each email key.
    for($index = 0; $index < count($emailKeys); $index++)
    {
      // Put the data into some variables.
      $emailId = $emailKeys[$index]['email_ref_id'];
      $this->selectEmailStatement->execute();
      $email = $this->selectEmailStatement->fetchAll(PDO::FETCH_ASSOC);
      if(count($email) == 0)
      {
        continue;
      }
      $id = $emailKeys[$index]['id'];
      $letterId = $email[0]['newsletter_ref_id'];
      $envelopeSender = $email[0]['envelope_sender'];
      $recipient = $email[0]['recipient'];
      $header = $email[0]['header'];
      $body = $email[0]['body'];
      $failureCount = $emailKeys[$index]['failure_count'];
      
      // Send it.
      $result = $this->doSend($envelopeSender, $recipient, $header, $body);
      if($result)
      {
        // Mark the email as sent.
        $status = 'sent';
        $isFailed = 0;
        $this->updateStatusStatement->execute();
        $nrSent++;
        
        // Register the newsletter id as sent.
        if(array_key_exists($letterId, $this->nrSent))
        {
          $this->nrSent[$letterId]++;
        }
        else
        {
          $this->nrSent[$letterId] = 1;
        }
        
        // Create stats.
        $type = 'email_sent';
        $this->createStatsStatement->execute();
      }
      else if($failureCount < (MAX_FAILURE_COUNT - 1))
      {
        // Cancel the email reservation and increase the failure count in the hope that
        // another process may be able to send it later. Also reset the SMTP connection.
        $status = 'send';
        $isFailed = 1;
        $this->updateStatusStatement->execute();
        $this->mail->SmtpClose();
        $this->logFile->write('Process ' . $this->getPaddedId() . ': Could not send email. Server said: \'' . $this->mail->ErrorInfo . '\'. The email has failed ' . ($failureCount + $isFailed) . ' time(s). Resetting SMTP connection.');
      }
      else
      {
        // Failed to send too many times. Register the newsletter id as sent.
        if(array_key_exists($letterId, $this->nrFailed))
        {
          $this->nrFailed[$letterId]++;
        }
        else
        {
          $this->nrFailed[$letterId] = 1;
        }
        
        // Update stats and set status to 'failed'.
        $status = 'failed';
        $isFailed = 1;
        $this->updateStatusStatement->execute();
        $type = 'email_not_sent';
        $this->createStatsStatement->execute();
        $this->logFile->write('Process ' . $this->getPaddedId() . ': Failed to send an email after trying ' . MAX_FAILURE_COUNT . ' times. It will therefore not be sent at all.');
      }
    }
    
    // Calculate time spent.
    $timeEnd = microtime(true);
    
    // Log the results.
    if($nrSent > 0)
    {
      $this->logFile->write('Process ' . $this->getPaddedId() . ': Sent ' . $nrSent . ' emails in ' . number_format(($timeEnd - $timeStart), 2) . ' seconds.');
    }
  }

  /**
  * Sends the actual email.
  * 
  * @desc This method uses phpmailer to send the email. It uses localhost as SMTP server.
  * An error is written to the log file if the email could not be sent.
  * 
  * @param String Envelope sender, in the format 'name <email@host.domain>'.
  * @param String Recipient, in the format 'name <email@host.domain>'.
  * @param String HTML body.
  * @param String Plaintext body.
  * @return Boolean TRUE if the mail was successfully sent. Otherwise FALSE.
  */
  private function doSend($anEnvelopeSender, $aRecipient, $aHeader, $aBody)
  {
    if($this->developmentMode)
    {
      return true;
    }
    
    // Set mail data.
    $this->mail->From = $this->stringToEmail($aSender);
    $this->mail->FromName = $this->stringToName($aSender);
    $this->mail->AddAddress($this->stringToEmail($aRecipient), $this->stringToName($aRecipient));
    $this->mail->Sender = $anEnvelopeSender; // Envelope sender.
    
    // Send it!
    $result = $this->mail->SmtpSend($aHeader, mb_convert_encoding($aBody, 'UTF-8', 'auto'));
    
    // Clear all addresses and attachments for next send operation.
    $this->mail->ClearAddresses();
    $this->mail->ClearReplyTos();
    $this->mail->Sender = '';
    
    return $result;
  }
  
  /**
  * Returns the email part of a string.
  * 
  * @desc Takes a string formatted as 'name <email@host.domain', and returns the email part
  * with symbols stripped. If the string only contains the email address, an empty name is returned.
  * 
  * @param String The sender string.
  * @return String The email part.
  */
  private function stringToEmail($aString)
  {
    if(strpos($aString, '<') === false)
    {
      return $aString;
    }
    else
    {
      return trim(substr($aString, strpos($aString, '<') + 1), '> ');
    }
  }
  
  /**
  * Takes a string formatted as 'name <email@host.domain', and returns the name part.
  * 
  * @desc If the string only contains the email address, an empty name is returned.
  * 
  * @param String The sender string.
  * @return String The name part.
  */
  private function stringToName($aString)
  {
    if(strpos($aString, '<') === false)
    {
      return '';
    }
    else
    {
      return trim(substr($aString, 0, strpos($aString, '<')));
    }
  }
  
  /**
  * Register amount sent.
  * 
  * @desc Updates each newsletter on the list $this->nrSent with the amount sent for that newsletter.
  * Also changes the newsletter status to 'sending' if it isn't already.
  * Afterwards, the list is cleared.
  */
  private function registerAmountSent()
  {
    $this->updateStatsStatement->bindParam(':nrSent', $nr, PDO::PARAM_INT);
    $this->updateStatsStatement->bindParam(':nrFailed', $nrFailed, PDO::PARAM_INT);
    $this->updateStatsStatement->bindParam(':letterId', $id, PDO::PARAM_INT);
    $this->updateLetterStatusStatement->bindParam(':letterId', $id, PDO::PARAM_INT);
    
    // Register nr. sent and failed.
    foreach($this->nrSent as $id => $nr)
    {
      if(array_key_exists($id, $this->nrFailed))
      {
        $nrFailed = $this->nrFailed[$id];
      }
      else
      {
        $nrFailed = 0;
      }
      $this->updateStatsStatement->execute();
      $this->updateLetterStatusStatement->execute();
    }
  }
  
  /**
  * Connects to the database.
  * 
  * @desc If a connection cannot be established, an error message is written
  * to the log file, and an exception is thrown.
  */
  private function connect()
  {
    try
    {
      $this->primaryDbConnection = new PDO(PRIMARYDBTYPE.':host='.PRIMARYDBSERVER.';dbname='.PRIMARYDBNAME, PRIMARYDBUSER, PRIMARYDBPASSWORD);
      $this->primaryDbConnection->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
      $this->localDbConnection = new PDO(LOCALDBTYPE.':host='.LOCALDBSERVER.';dbname='.LOCALDBNAME, LOCALDBUSER, LOCALDBPASSWORD);
      $this->localDbConnection->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    }
    catch(PDOException $someException)
    {
      $this->logFile->write('Error: ' . $someException->getMessage());
      throw $someException;
    }
  }

  /**
  * Closes the connection to the database.
  * 
  * @desc On failure, nothing is done.
  */
  private function disconnect()
  {
    $this->localDbConnection = null;
  }
  
  /**
  * Is disabled.
  * 
  * @desc Checks the status of this mailserver. If it has been disabled in the database,
  * FALSE is returned and causes the script to shut down nicely.
  * 
  * @param String New status to set for the sortserver if status is ok.
  * @param Boolean Whether or not to check if the server is busy. If TRUE and it is busy, FALSE is returned.
  * @return Boolean TRUE if status is ok, otherwise FALSE.
  */
  public function isDisabled()
  {
    $this->isDisabledStatement->execute();
    $isDisabled = $this->isDisabledStatement->fetchAll(PDO::FETCH_ASSOC);
    return ($isDisabled[0]['status'] == 'disabled');
  }
  
  /**
  * Update queue size.
  * 
  * @desc After a batch of emails has been sent, the queue size for the mailserver is updated
  * to help the sendbalancer do its work properly.
  */
  private function updateQueueSize()
  {
    $this->updateMailServerStatement->bindParam(':nrToSend', $nrToSend, PDO::PARAM_INT);
    
    $this->checkQueueSizeStatement->execute();
    $queueSize = $this->checkQueueSizeStatement->fetchAll(PDO::FETCH_ASSOC);
    $nrToSend = $queueSize[0]['queue_size'];
    $this->updateMailServerStatement->execute();
  }
}