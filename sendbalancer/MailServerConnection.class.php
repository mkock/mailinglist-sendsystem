<?php

/**
* MailServerConnection.
* 
* @desc Representation of a database connection to a mailserver.
* This object is used to manage the connection and insert emails.
* 
* A MailServerConnection only manages the connection to the mailserver,
* however it also has access to the secondary database.
*/
class MailServerConnection
{
  protected $id;
  
  protected $dbConnection;

  protected $secondaryDbConnection;
  
  protected $host;
  
  protected $dbType;
  
  protected $dbUser;
  
  protected $dbPassword;
  
  protected $dbName;
  
  protected $insertEmailStatement;
  
  protected $insertStatusStatement;

  protected $updateStatusStatement;
  
  protected $nrToSend;
  
  protected $distribution;
  
  /**
  * Constructor.
  * 
  * @desc Takes a connection to the secondary database as an argument,
  * which is used to update email status.
  */
  public function __construct($aDbConnection)
  {
    $this->secondaryDbConnection = $aDbConnection;
  }
  
  public function getNrToSend()
  {
    return $this->nrToSend;
  }
  
  public function setNrToSend($aNrToSend)
  {
    $this->nrToSend = $aNrToSend;
  }
  
  public function getDistribution()
  {
    return $this->distribution;
  }
  
  public function setDistribution($aDistribution)
  {
    $this->distribution = $aDistribution;
  }

  public function getId()
  {
    return $this->id;
  }
  
  public function setId($anId)
  {
    $this->id = $anId;
  }
  
  public function getHost()
  {
    return $this->host;
  }
  
  public function setHost($aHost)
  {
    $this->host = $aHost;
  }
  
  public function getDbType()
  {
    return $this->dbType;
  }
  
  public function setDbType($aDbType)
  {
    $this->dbType = $aDbType;
  }
  
  public function getDbUser()
  {
    return $this->dbUser;
  }
  
  public function setDbUser($aDbUser)
  {
    $this->dbUser = $aDbUser;
  }
  
  public function getDbPassword()
  {
    return $this->dbPassword;
  }
  
  public function setDbPassword($aPassword)
  {
    $this->dbPassword = $aPassword;
  }
  
  public function getDbName()
  {
    return $this->dbName;
  }
  
  public function setDbName($aDbName)
  {
    $this->dbName = $aDbName;
  }
  
  public function prepareStatements()
  {
    // Prepare the SQL command to insert emails.
    $sql = 'INSERT INTO local_emails (id, manager_ref_id, email_template_ref_id, newsletter_ref_id, envelope_sender, recipient, is_hotmail, header, body) VALUES (NULL, :manager, :template, :newsletter, :envelopeSender, :recipient, :isHotmail, :header, :body)';
    $this->insertEmailStatement = $this->dbConnection->prepare($sql);
    
    // Prepare the SQL command to insert status messages.
    $sql = 'INSERT INTO local_email_status (id, email_ref_id, status, time_to_send, reserved_by, failure_count) VALUES (NULL, :emailId, :status, :timeToSend, NULL, 0)';
    $this->insertStatusStatement = $this->dbConnection->prepare($sql);
    $this->insertStatusStatement->bindValue(':status', 'send', PDO::PARAM_STR);
    
    // Prepare the SQL command to update status.
    $sql = 'UPDATE secondary_email_status SET status = :status WHERE email_ref_id = :emailId';
    $this->updateStatusStatement = $this->secondaryDbConnection->prepare($sql);
    $this->updateStatusStatement->bindValue(':status', 'reserved', PDO::PARAM_STR);
  }
  
  /**
  * Insert emails.
  * 
  * @desc Inserts an email into the database.
  * 
  * @param Array A PDO resultset as an associative array.
  */
  public function insertEmail($aResultSet)
  {
    // Initial binding of parameters.
    $this->insertEmailStatement->bindParam(':manager', $managerId, PDO::PARAM_INT);
    $this->insertEmailStatement->bindParam(':template', $templateId, PDO::PARAM_INT);
    $this->insertEmailStatement->bindParam(':newsletter', $newsletterId, PDO::PARAM_INT);
    $this->insertEmailStatement->bindParam(':envelopeSender', $envelopeSender, PDO::PARAM_STR);
    $this->insertEmailStatement->bindParam(':recipient', $recipient, PDO::PARAM_STR);
    $this->insertEmailStatement->bindParam(':isHotmail', $isHotmail, PDO::PARAM_INT);
    $this->insertEmailStatement->bindParam(':header', $header, PDO::PARAM_STR);
    $this->insertEmailStatement->bindParam(':body', $body, PDO::PARAM_STR);
    $this->insertStatusStatement->bindParam(':emailId', $emailId, PDO::PARAM_INT);
    $this->insertStatusStatement->bindParam(':timeToSend', $timeToSend, PDO::PARAM_STR);
    $this->updateStatusStatement->bindParam(':emailId', $secondaryEmailsId, PDO::PARAM_INT);

    // Insert emails and update their status messages in the secondary database.
    $secondaryEmailsId = $aResultSet[0]['id'];
    $managerId = $aResultSet[0]['manager_ref_id'];
    $templateId = $aResultSet[0]['email_template_ref_id'];
    $newsletterId = $aResultSet[0]['newsletter_ref_id'];
    $envelopeSender = $aResultSet[0]['envelope_sender'];
    $recipient = $aResultSet[0]['recipient'];
    $isHotmail = $aResultSet[0]['is_hotmail'];
    $header = $aResultSet[0]['header'];
    $body = $aResultSet[0]['body'];
    $timeToSend = $aResultSet[0]['time_to_send'];
    $this->insertEmailStatement->execute();
    $emailId = $this->dbConnection->lastInsertId();
    $this->insertStatusStatement->execute();
    $this->updateStatusStatement->execute();
  }
  
  /**
  * Connect.
  * 
  * @desc Connects to the mailserver identified by the connection parameters given to this object.
  * 
  * @return Boolean TRUE if the connection was established, otherwise FALSE.
  */
  public function connect()
  {
    try
    {
      $this->dbConnection = new PDO($this->getDbType().':host='.$this->getHost().';dbname='.$this->getDbName(), $this->getDbUser(), $this->getDbPassword());
      $this->dbConnection->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    }
    catch(PDOException $someException)
    {
      return false;
    }
    return true;
  }
  
  /**
  * Disconnect.
  * 
  * @desc Closes the database connection to the mailserver.
  */
  public function disconnect()
  {
    $this->dbConnection = null;
  }
}
?>
