<?php
/**
* @desc This is the main email sendsystem.
* 
* Creole is used for the database connection.
* It uses a log file for basic errors that cannot be written to the database.
*/

class SendConfig
{
  protected $id;
  
  protected $host;
  
  protected $logFile;
  
  protected $primaryDbConnection;
  
  protected $localDbConnection;
  
  protected $mailServerStatusStatement;
  
  protected $changeMailServerStatusStatement;

  protected $createMailServerStatement;
  
  protected $countEmailsStatement;
  
  protected $reserveEmailsStatement;
  
  protected $checkEmailsStatement;
  
  protected $getSentMailsStatement;
  
  protected $deleteFromEmailsTableStatement;
  
  /**
  * Construct.
  * 
  * @desc Initializes the database connections and prepares the SQL statements.
  */
  public function __construct()
  {
    $host = php_uname('n');
    $this->host = substr($host, 0, (strpos($host, '.') ? strpos($host, '.') : strlen($host)));
    $this->logFile = new Logging('sendsystem.log');
    $this->connect();
    $this->prepareStatements();
  }
  
  public function __destruct()
  {
    $this->disconnect();
  }
  
  public function getLogFile()
  {
    return $this->logFile;
  }
  
  /**
  * Connect.
  * 
  * @desc If a connection to the databases cannot be established, an error message is written
  * to the log file, and an exception is thrown.
  */
  public function connect()
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
  * Disconnect.
  * 
  * @desc Closes the connection to the database.
  */
  public function disconnect()
  {
    $this->primaryDbConnection = null;
    $this->localDbConnection = null;
  }

  /**
  * Prepare statements.
  * 
  * @desc Prepares the SQL commands that will be used in this script.
  */
  private function prepareStatements()
  {
    // Check if the host exists and is not disabled.
    $sql = 'SELECT id, status FROM mailservers WHERE host = :host';
    $this->mailServerStatusStatement = $this->primaryDbConnection->prepare($sql);
    $this->mailServerStatusStatement->bindParam(':host', $this->host, PDO::PARAM_INT);

    // Prepare the SQL command to update sortserver status.
    $sql = 'UPDATE mailservers SET status = :newStatus, updated_at = NOW() WHERE id = :id AND status != :oldStatus';
    $this->changeMailServerStatusStatement = $this->primaryDbConnection->prepare($sql);
    $this->changeMailServerStatusStatement->bindParam(':id', $this->id, PDO::PARAM_INT);
    $this->changeMailServerStatusStatement->bindValue(':oldStatus', 'disabled', PDO::PARAM_STR);

    // Prepare the SQL command to create the host mailserver.
    $sql = 'INSERT INTO mailservers (id, host, db_type, db_user, db_password, db_name, status, nr_to_send, updated_at, created_at) VALUES (NULL, :host, :dbType, :dbUser, AES_ENCRYPT(:dbPassword, :aesKey), :dbName, :status, :nrToSend, NOW(), NOW())';
    $this->createMailServerStatement = $this->primaryDbConnection->prepare($sql);
    $this->createMailServerStatement->bindValue(':host', $this->host, PDO::PARAM_STR);
    $this->createMailServerStatement->bindValue(':dbType', LOCALDBTYPE, PDO::PARAM_STR);
    $this->createMailServerStatement->bindValue(':dbUser', LOCALDBUSER, PDO::PARAM_STR);
    $this->createMailServerStatement->bindValue(':dbPassword', LOCALDBPASSWORD, PDO::PARAM_STR);
    $this->createMailServerStatement->bindValue(':aesKey', AES_KEY, PDO::PARAM_STR);
    $this->createMailServerStatement->bindValue(':dbName', LOCALDBNAME, PDO::PARAM_STR);
    $this->createMailServerStatement->bindValue(':nrToSend', 0, PDO::PARAM_INT);
    
    // Prepare the SQL command to see how many emails should be sent within the next five minutes.
    $sql = 'SELECT COUNT(*) AS count FROM local_email_status WHERE time_to_send <= ADDTIME(NOW(), \'00:05:00\') AND status LIKE :status';
    $this->countEmailsStatement = $this->localDbConnection->prepare($sql);
    $this->countEmailsStatement->bindValue(':status', 'send', PDO::PARAM_STR);
    
    // Prepare the SQL command to check if there is any work to do at all.
    $sql = 'SELECT id FROM local_email_status WHERE status NOT LIKE :status LIMIT 1';
    $this->checkEmailsStatement = $this->localDbConnection->prepare($sql);
    $this->checkEmailsStatement->bindValue(':status', 'sent', PDO::PARAM_STR);
    
    // Prepare the SQL command to select sent emails for deletion.
    $sql = 'SELECT email_ref_id FROM local_email_status WHERE status LIKE :sent LIMIT ' . MAX_BATCH_SIZE;
    $this->getSentMailsStatement = $this->localDbConnection->prepare($sql);
    $this->getSentMailsStatement->bindValue(':sent', 'sent', PDO::PARAM_STR);
       
    // Prepare the SQL command to empty the local_emails table.
    $sql = 'DELETE FROM local_emails WHERE id = :emailId';
    $this->deleteFromEmailsTableStatement = $this->localDbConnection->prepare($sql);
  }
  
  /**
  * Check status.
  * 
  * @desc Checks the status of this mailserver. If it has been disabled in the database,
  * or if it is busy or has failed, FALSE is returned and causes the script to shut down nicely,
  * making sure that no emails have been reserved. If the mailserver does not exist,
  * it's created, and status is set to the given. Afterwards, this object's id has been set.
  * 
  * @param String New status to set for the sortserver if status is ok.
  * @param Boolean Whether or not to check if the server is busy. If TRUE and it is busy, FALSE is returned.
  * @return Boolean TRUE if status is ok, otherwise FALSE.
  */
  public function checkStatus($aStatus, $aBusyCheck = true)
  {
    $this->mailServerStatusStatement->execute();
    $resultSet = $this->mailServerStatusStatement->fetchAll(PDO::FETCH_ASSOC);

    // Check host status.
    if(count($resultSet) == 0)
    {
      // Create the host.
      $this->createMailServerStatement->bindParam(':status', $aStatus, PDO::PARAM_STR);
      $this->createMailServerStatement->execute();
      $this->id = $this->primaryDbConnection->lastInsertId();
    }
    else if(($resultSet[0]['status'] == 'disabled') || (($aBusyCheck) && ($resultSet[0]['status'] == 'busy')) || ($resultSet[0]['status'] == 'failed'))
    {
      return false;
    }
    else
    {
      $this->id = $resultSet[0]['id'];
      $this->changeStatus($aStatus);
    }
    
    return true;
  }
 
  /**
  * Get id.
  * 
  * @desc Returns the id of this mailserver as given in the database table 'mailservers'.
  */
  public function getId()
  {
    return $this->id;
  }
 
   /**
  * Change status.
  * 
  * @desc Changes the status of the mailserver.
  * @param String The status can be 'available', 'busy', 'failed', 'disabled' or 'unavailable'.
  */
  public function changeStatus($aStatus)
  {
    $this->changeMailServerStatusStatement->bindParam(':newStatus', $aStatus, PDO::PARAM_STR);
    $this->changeMailServerStatusStatement->execute();
  }

  /**
  * Reserve emails.
  * 
  * @desc Sets status to 'reserved' for all emails that are ready to be sent within the next 5 minutes
  * (or earlier), and sets reserved_by to one of the processes.
  */
  public function reserveEmails()
  {
    // Find an equal distribution of emails between the processes.
    $this->countEmailsStatement->execute();
    $availableEmails = $this->countEmailsStatement->fetchAll(PDO::FETCH_ASSOC);
    $nrOfEmails = $availableEmails[0]['count'];
    $limit = (int) ceil($nrOfEmails / PROCESS_COUNT);

    // Prepare the SQL command to reserve emails.
    $sql = 'UPDATE local_email_status SET status = :newStatus, reserved_by = :processId WHERE time_to_send <= ADDTIME(NOW(), \'00:05:00\') AND status = :oldStatus LIMIT ' . $limit;
    $this->reserveEmailsStatement = $this->localDbConnection->prepare($sql);
    $this->reserveEmailsStatement->bindValue(':newStatus', 'reserved', PDO::PARAM_STR);
    $this->reserveEmailsStatement->bindValue(':oldStatus', 'send', PDO::PARAM_STR);
    
    $this->reserveEmailsStatement->bindParam(':processId', $processId, PDO::PARAM_INT);
    
    // Divide the emails between the processes.
    for($processId = 1; $processId <= PROCESS_COUNT; $processId++)
    {
      $this->reserveEmailsStatement->execute();
      $this->logFile->write('Distributed ' . number_format($this->reserveEmailsStatement->rowCount()) . ' emails to process ' . $processId);
    }
  }
  
  /**
  * Delete emails.
  * 
  * @desc After an email has been sent, it is marked as 'sent'. Deletion is delayed because of
  * efficiency reasons. This method handles the delayed deletion. It will also delete emails
  * that has been marked as 'failed'. Notice that only rows from the local_emails table are
  * deleted. The script depends on the database constraints to delete the corresponding status
  * rows.
  */
  public function deleteEmails()
  {
    $this->deleteFromEmailsTableStatement->bindParam(':emailId', $emailId, PDO::PARAM_INT);
    $this->logFile->write('Deleting sent emails.');
    
    $timeStart = microtime(true);
    $hasSentMails = true;
    $nrDeleted = 0;
    while($hasSentMails)
    {
      $this->getSentMailsStatement->execute();
      $sentMailIds = $this->getSentMailsStatement->fetchAll(PDO::FETCH_ASSOC);
      $hasSentMails = (count($sentMailIds) > 0);
      foreach($sentMailIds as $someEmail)
      {
        $emailId = $someEmail['email_ref_id'];
        $this->deleteFromEmailsTableStatement->execute();
        $nrDeleted++;
      }
    }
    $timeEnd = microtime(true);
    $this->logFile->write('Deleted ' . number_format($nrDeleted) . ' sent emails in ' . number_format($timeEnd - $timeStart) . ' seconds.');
  }
  
  /**
  * Has emails.
  * 
  * @desc Checks whether there are any emails in the local database that haven't already been sent.
  * 
  * @return Boolean TRUE if there are any emails to send, otherwise FALSE.
  */
  public function hasEmails()
  {
    $this->checkEmailsStatement->execute();
    $hasEmails = $this->checkEmailsStatement->fetchAll(PDO::FETCH_ASSOC);
    return (count($hasEmails) > 0);
  }
}