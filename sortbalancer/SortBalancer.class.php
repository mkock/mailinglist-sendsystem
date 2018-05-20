<?php
/**
* SortBalancer
* 
* @desc Checks how many sortservers are available and fetches their ids.
* Then divides the newsletter recipients equally between the sortservers
* by setting the reserved_by column to the sortserver id.
*/
class SortBalancer
{
  protected $dbConnection;
  
  protected $logFile;
  
  protected $hasAvailableRecipientsStatement;
  
  protected $availableSortServersStatement;
  
  protected $reserveRecipientsStatement;
  
  public function __construct()
  {
    $this->logFile = new Logging('sortbalancer.log');
    $this->connect();
    $this->logFile->write('Initializing.');
  }

  public function __destruct()
  {
    $this->disconnect();
  }

  private function connect()
  {
    try
    {
      $this->dbConnection = new PDO(DBTYPE.':host='.DBSERVER.';dbname='.DBNAME, DBUSER, DBPASSWORD);
    }
    catch(PDOException $someException)
    {
      $this->logFile->write($this->sortId, $someException->getMessage());
      throw $someException;
    }
  }
  
  private function disconnect()
  {
    $this->dbConnection = null;
  }
  
  /**
  * Get available sortservers.
  * 
  * @desc Returns a list of all sortservers that are not disabled or unreachable.
  */
  private function getAvailableSortServers()
  {
    $ids = array();
    
    $this->availableSortServersStatement->execute();
    while($resultSet = $this->availableSortServersStatement->fetch(PDO::FETCH_ASSOC))
    {
      $ids[] = $resultSet['id'];
    }
    
    $this->logFile->write('Found ' . count($ids) . ' sortservers.');
    return $ids;
  }
  
  /**
  * Has available recipients.
  * 
  * @desc Checks whether or not there are any recipients available.
  */
  private function hasAvailableRecipients()
  {
    $this->hasAvailableRecipientsStatement->execute();
    $resultSet = $this->hasAvailableRecipientsStatement->fetchAll();
    return (count($resultSet) > 0);
  }
  
  /**
  * Prepare statements.
  * 
  * @desc Prepares all SQL statements that are used in this script.
  */
  private function prepareStatements()
  {
    // For getting available sortservers.
    $sql = 'SELECT id FROM sortservers WHERE status != \'disabled\' AND status != \'unreachable\'';
    $this->availableSortServersStatement = $this->dbConnection->prepare($sql);

    // For checking for available recipients.
    $sql = 'SELECT id FROM newsletter_recipients WHERE status = \'delegate\' LIMIT 1';
    $this->hasAvailableRecipientsStatement = $this->dbConnection->prepare($sql);
    
    // For reserving a batch of recipients.
    $sql = 'UPDATE newsletter_recipients SET status = \'reserved\', reserved_by = :sortServer WHERE status = \'delegate\' LIMIT ' . MAX_BATCH_SIZE;
    $this->reserveRecipientsStatement = $this->dbConnection->prepare($sql);
  }
  
  /**
  * Start.
  * 
  * @desc Starts distributing recipients to the sortservers.
  * 
  * @return Boolean TRUE if distribution completed without problems, otherwise FALSE.
  */
  public function start()
  {
    $this->logFile->write('Initializing.');
    $nrDelegated = 0;
    $timeStart = microtime(true);
    
    // Prepare and bind statements.
    $this->prepareStatements();
    $this->reserveRecipientsStatement->bindParam(':sortServer', $someSortServer, PDO::PARAM_INT);
    
    // Get sortservers and quit if none are available.
    $sortServers = $this->getAvailableSortServers();
    if(count($sortServers) == 0)
    {
      $this->logFile->write('No sortservers available, quitting.');
      return false;
    }
    
    // Initial check to see if there are any recipients available at all.
    if(!$this->hasAvailableRecipients())
    {
      $this->logFile->write('No available recipients, quitting.');
      return true;
    }
    
    // Distribute recipients.
    while($this->hasAvailableRecipients())
    {
      foreach($sortServers as $someSortServer)
      {
        $this->reserveRecipientsStatement->execute();
        $nrDelegated = $nrDelegated + $this->reserveRecipientsStatement->rowCount();
      }
    }
    
    $timeEnd = microtime(true);
    $this->logFile->write('Finished delegating ' . number_format($nrDelegated) . ' recipients to ' . number_format(count($sortServers)) . ' sortservers in ' . number_format($timeEnd - $timeStart, 2) . ' seconds.');
    return true;
  }
}
?>
