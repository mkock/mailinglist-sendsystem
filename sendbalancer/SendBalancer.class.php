<?php

/**
* SendBalancer.
* 
* @desc The SendBalancer reads the number of mailservers from the primary database,
* and the amount of emails that each has. It does this continually, so that emails
* can be given to the mailservers that have the least amount of emails to send.
* 
* Each mailserver has its own database connection settings. These are kept in the
* primary database together with the mailserver status to keep it centralized.
*/
class SendBalancer
{
  protected $primaryDbConnection;
  
  protected $secondaryDbConnection;
  
  protected $getMailServersStatement;
  
  protected $getQueuesStatement;
  
  protected $checkEmailsStatement;
  
  protected $fetchEmailsStatement;
  
  protected $fetchByStatusStatement;
  
  protected $deleteEmailsStatement;
  
  protected $deleteStatusStatement;
  
  protected $updateMailserverStatement;
  
  protected $deleteZombieStatement;
  
  protected $mailServers;
  
  protected $logFile;

  /**
  * Start.
  * 
  * @desc This is the main method for starting the SendBalancer. It is only necessary to
  * call this method. All errors are caught and written to the log.
  * 
  * @return Boolean TRUE if the script finished without errors, otherwise FALSE.
  */
  public function start()
  {
    $this->logFile->write('Initializing.');
    $start = microtime(true);
    $nrOfEmails = 0;
    
    try
    {
      $this->prepareStatements();
      $this->fetchEmailsStatement->bindParam(':emailId', $emailId, PDO::PARAM_INT);
      $this->fetchByStatusStatement->bindParam(':distribution', $distribution, PDO::PARAM_INT);
      $this->deleteZombieStatement->bindParam(':emailId', $emailId, PDO::PARAM_INT);
      
      // Quick check to see if there is anything to send.
      if(!$this->hasEmails())
      {
        $this->logFile->write('Nothing to send, quitting.');
        return true;
      }
      
      // Connect to the mailservers.
      $hasConnections = $this->connectToMailServers();
      if(!$hasConnections)
      {
        return false;
      }
      
      $this->prepareMailServers();

      // This is the main loop.
      while($this->hasEmails())
      {
        // Calculate the distribution for each mailserver.
        $this->calculateDistribution();
        
        // Distribute emails.
        foreach($this->mailServers as $someServer)
        {
          $distribution = $someServer->getDistribution();
          $this->fetchByStatusStatement->execute();
          $emails = $this->fetchByStatusStatement->fetchAll(PDO::FETCH_ASSOC);
          if(count($emails) == 0)
          {
            continue;
          }
          
          foreach($emails as $someEmail)
          {
            $emailId = $someEmail['email_ref_id'];
            $this->fetchEmailsStatement->execute();
            $email = $this->fetchEmailsStatement->fetchAll(PDO::FETCH_ASSOC);
            if(empty($email))
            {
              // No email exists for this status entry, so delete it.
              $this->deleteZombieStatement->execute();
              $this->logFile->write('Recovering from possible error by removing zombie status entry.');
            }
            if(empty($email[0]['time_to_send']))
            {
              $this->logFile->write('ERROR: time_to_send is empty for email ' . $email[0]['id'] . ', newsletter ' . $email[0]['newsletter_ref_id'] . '.');
            }
            try
            {
              $someServer->insertEmail($email);
            }
            catch(Exception $someException)
            {
              // If emails cannot be delegated to the mailserver, change its status to 'failed' and ignore it in the next run.
              $this->updateMailserverStatement->execute(array(':status' => 'failed', ':id' => $someServer->getId()));
              $this->logFile->write('Mailserver \'' . $someServer->getHost() . '\' failed to accept emails and will be ignored in the future. This problem has to be fixed manually.');
              return;
            }
          }
          $nrOfBatchEmails = count($emails);
          $nrOfEmails += $nrOfBatchEmails;
          $this->logFile->write('Distributed ' . $nrOfBatchEmails . ' emails to \'' . $someServer->getHost() . '\'.');
        }
      }
      
      // Perform table cleanup.
      $this->cleanup();
      
      // Finished. Close connections.
      $this->disconnectFromMailServers();
    }
    catch(Exception $someException)
    {
      $this->logFile->write('Error: ' . $someException->getMessage());
      return false;
    }
    
    $end = microtime(true);
    $this->logFile->write('Finished distributing ' . number_format($nrOfEmails) . ' emails to ' . number_format(count($this->mailServers)) . ' mailserver(s) in ' . number_format($end - $start, 2) . ' seconds.');
    
    return true;
  }
  
  /**
  * Construct.
  * 
  * @desc Enables logging and establishes basic database connections.
  */
  public function __construct()
  {
    $this->logFile = new Logging('sendbalancer.log');
    $this->connect();
    $this->mailServers = array();
  }
  
  /**
  * Destruct.
  * 
  * @desc Closes the database connections.
  */
  public function __destruct()
  {
    $this->disconnect();
    $this->disconnectFromMailServers();
  }
  
  /**
  * Connect.
  * 
  * @desc Established database connections to the primary and secondary servers.
  */
  private function connect()
  {
    try
    {
      $this->primaryDbConnection = new PDO(PRIMARYDBTYPE.':host='.PRIMARYDBSERVER.';dbname='.PRIMARYDBNAME, PRIMARYDBUSER, PRIMARYDBPASSWORD);
      $this->secondaryDbConnection = new PDO(SECONDARYDBTYPE.':host='.SECONDARYDBSERVER.';dbname='.SECONDARYDBNAME, SECONDARYDBUSER, SECONDARYDBPASSWORD);
    }
    catch(PDOException $someException)
    {
      $this->logFile->write('Error: ' . $someException->getMessage());
      throw $someException;
    }
  }
  
  /**
  * Prepare statements.
  * 
  * @desc Prepares all SQL statements that are used within the script.
  */
  private function prepareStatements()
  {
    // Prepare the SQL command to fetch available mailservers and their connection settings.
    $sql = 'SELECT id, host, db_type, db_user, AES_DECRYPT(db_password, :aesKey) AS db_password, db_name FROM mailservers WHERE status NOT LIKE :disabled AND status NOT LIKE :unreachable';
    $this->getMailServersStatement = $this->primaryDbConnection->prepare($sql);
    $this->getMailServersStatement->bindValue(':disabled', 'disabled', PDO::PARAM_STR);
    $this->getMailServersStatement->bindValue(':unreachable', 'unreachable', PDO::PARAM_STR);
    $this->getMailServersStatement->bindValue(':aesKey', AES_KEY, PDO::PARAM_STR);
    
    // Prepare the SQL command to fetch mailserver queues.
    $sql = 'SELECT id, nr_to_send FROM mailservers WHERE id = :id';
    $this->getQueuesStatement = $this->primaryDbConnection->prepare($sql);
    
    // Prepare the SQL command to check for emails.
    $sql = 'SELECT id FROM secondary_email_status WHERE status = :status LIMIT 1';
    $this->checkEmailsStatement = $this->secondaryDbConnection->prepare($sql);
    $this->checkEmailsStatement->bindValue(':status', 'send', PDO::PARAM_STR);
    
    // Prepare the SQL command to fetch emails.
    $sql = 'SELECT id, manager_ref_id, email_template_ref_id, newsletter_ref_id, envelope_sender, recipient, is_hotmail, header, body, time_to_send FROM secondary_emails WHERE id = :emailId';
    $this->fetchEmailsStatement = $this->secondaryDbConnection->prepare($sql);
    
    // Prepare the SQL command to fetch email ids by status.
    $sql = 'SELECT email_ref_id FROM secondary_email_status WHERE status = :status LIMIT :distribution';
    $this->fetchByStatusStatement = $this->secondaryDbConnection->prepare($sql);
    $this->fetchByStatusStatement->bindValue(':status', 'send', PDO::PARAM_STR);
    
    // Prepare the SQL command to delete emails.
    $sql = 'TRUNCATE TABLE secondary_emails';
    $this->deleteEmailsStatement = $this->secondaryDbConnection->prepare($sql);
    
    // Prepare the SQL command to delete status messages.
    $sql = 'TRUNCATE TABLE secondary_email_status';
    $this->deleteStatusStatement = $this->secondaryDbConnection->prepare($sql);
    
    // Prepare the SQL command to update mailserver status.
    $sql = 'UPDATE mailservers SET status = :status WHERE id = :id';
    $this->updateMailserverStatement = $this->primaryDbConnection->prepare($sql);
    
    // Prepare the SQL command to remove zombie status entries.
    $sql = 'DELETE FROM secondary_email_status WHERE email_ref_id = :emailId';
    $this->deleteZombieStatement = $this->secondaryDbConnection->prepare($sql);
  }
  
  /**
  * Connect to mailservers.
  * 
  * @desc Looks up available mailservers and their connection settings in the primary database,
  * and establishes a connection to each one.
  * 
  * Assumes that prepareStatements has been called beforehand.
  * 
  * @return Boolean TRUE if any connections were established, otherwise FALSE.
  */
  private function connectToMailServers()
  {
    $this->getMailServersStatement->execute();
    $resultSet = $this->getMailServersStatement->fetchAll(PDO::FETCH_ASSOC);
    
    $countServers = count($resultSet);
    if($countServers == 0)
    {
      $this->logFile->write('No mailservers available, quitting.');
      return false;
    }
    else
    {
      $this->logFile->write('Found ' . $countServers . ' mailserver(s).');
    }
    
    for($index = 0; $index < count($resultSet); $index++)
    {
      // Create a mailserver connection object.
      $mailServer = new MailServerConnection($this->secondaryDbConnection);
      $mailServer->setId($resultSet[$index]['id']);
      $mailServer->setDbType($resultSet[$index]['db_type']);
      $mailServer->setHost($resultSet[$index]['host']);
      $mailServer->setDbName($resultSet[$index]['db_name']);
      $mailServer->setDbUser($resultSet[$index]['db_user']);
      $mailServer->setDbPassword($resultSet[$index]['db_password']);
      if($mailServer->connect())
      {
        $this->mailServers[] = $mailServer;
      }
      else
      {
        // In case of a connection error, the error is logged and the server is skipped.
        $this->logFile->write('Unable to connect to mailserver \'' . $mailServer->getHost() . '\', skipping it.');
        continue;
      }
    }

    $countConnections = count($this->mailServers);
    if($countConnections == 0)
    {
      $this->logFile->write('No connections could be established to any mailservers, quitting.');
    }
    else
    {
      $this->logFile->write('Successfully connected to ' . $countConnections . ' of ' . $countServers . ' mailservers.');
    }
    return ($countConnections > 0);
  }
  
  /**
  * Disconnect from mailservers.
  * 
  * @desc Called by the destructor.
  */
  private function disconnectFromMailServers()
  {
    foreach($this->mailServers as $someMailServer)
    {
      $someMailServer->disconnect();
    }
  }
  
  private function disconnect()
  {
    $this->primaryDbConnection = null;
    $this->secondaryDbConnection = null;
  }
  
  /**
  * Calculate distribution.
  * 
  * @desc Calculates the number of emails to distribute to each mailserver. The algorithm works
  * as follows: For the mailserver with the largest amount of unsent emails, only MAX_BATCH_SIZE
  * emails are added. For the rest, they are given the amount of emails required to level the
  * queue lengths. Example:
  * 
  * mail01 has 100 emails.
  * mail02 has 250 emails.
  * MAX_BATCH_SIZE is 1000.
  * 
  * mail02 has the largest queue size, so 1000 emails is added to it.
  * mail01 is now 250+1000-100=1150 emails behind, so 1150 emails are added to it.
  * Both mailservers now have 1250 emails, and the queue size is leveled.
  * 
  * The distribution is saved within each MailServerConnection object.
  */
  private function calculateDistribution()
  {
    $this->getQueuesStatement->bindParam(':id', $id, PDO::PARAM_INT);
    
    $max = 0;
    foreach($this->mailServers as $someMailServer)
    {
      $id = $someMailServer->getId();
      $this->getQueuesStatement->execute();
      $resultSet = $this->getQueuesStatement->fetchAll(PDO::FETCH_ASSOC);
      $someMailServer->setNrToSend($resultSet[0]['nr_to_send']);
      $max = max($max, $resultSet[0]['nr_to_send']);
    }
    
    // Calculate distributions.
    $maxSize = $max + MAX_BATCH_SIZE;
    foreach($this->mailServers as $someMailServer)
    {
      $someMailServer->setDistribution($maxSize - $someMailServer->getNrToSend());
    }
  }
  
  /**
  * Has emails.
  * 
  * @desc Checks if there are any emails that haven't been distributed to a mailserver.
  * 
  * @return Boolean TRUE if there are emails left, otherwise FALSE.
  */
  private function hasEmails()
  {
    $this->checkEmailsStatement->execute();
    $hasEmails = $this->checkEmailsStatement->fetchAll(PDO::FETCH_ASSOC);
    return (count($hasEmails) > 0);
  }
  
  /**
  * Prepare mailservers.
  * 
  * @desc Prepares each mailserver to receive emails.
  */
  private function prepareMailServers()
  {
    foreach($this->mailServers as $someServer)
    {
      $someServer->prepareStatements();
    }
  }
  
  /**
  * Cleanup.
  * 
  * @desc Deletes all emails and their status messages from the secondary database
  * if, and only if, all emails has been distributed.
  */
  private function cleanup()
  {
    // Return without doing anything if there are still unreserved emails.
    if($this->hasEmails())
    {
      return;
    }
    
    // Empty the tables.
    $this->secondaryDbConnection->beginTransaction();
    $this->deleteEmailsStatement->execute();
    $this->deleteStatusStatement->execute();
    $this->secondaryDbConnection->commit();
  }
}