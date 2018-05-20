<?php

/**
* SortSystem.
* 
* @desc This is the main email sortsystem.
* PDO is used for the database connection.
* It uses a log file for basic errors that cannot be written to the database.
* Usage is simple - just call method start().
*/

require_once 'config.php';
require_once 'Logging.class.php';

class SortSystem
{
  // Sortserver host name.
  protected $host;
  
  // Sortserver id.
  protected $id;
  
  // File resource.
  protected $logFile;
  
  // Connection resource file from PDO for the primary database.
  protected $primaryDbConnection;
  
  // Connection resource file from PDO for the secondary database.
  protected $secondaryDbConnection;
  
  protected $changeSortServerStatusStatement;
  
  protected $nrOfAvailableSortingServers;
  
  protected $hasRecipientsStatement;
  
  protected $recipientStatement;
  
  protected $templateStatement;
  
  protected $letterStatement;
  
  protected $interestStatement;
  
  protected $userInterestStatement;
  
  protected $deleteRecipientStatement;
  
  protected $listStatement;
  
  protected $datafieldStatement;
  
  protected $checkStatement;
  
  protected $valueStatement;
  
  protected $emailStatement;
  
  protected $emailStatusStatement;
  
  protected $statusStatement;
  
  protected $isFinishedStatement;

  protected $newsletterStatement;
  
  protected $emailTemplateStatement;

  protected $sortServerStatusStatement;
  
  protected $earliestFirstStatement;
  
  protected $letterHasRecipientsStatement;
  
  protected $checkNewsletterStatement;
  
  protected $timestamp;
  
  protected $dateInternational;
  
  protected $dateDanish;
  
  protected $identifierPrefix;
  
  protected $identifierPostfix;
  
  protected $adminLinkStyleBegin;
  
  protected $adminLinkStyleEnd;
  
  /**
  * Initializes the sorting procedure.
  * 
  * @desc Connects to the sorting server and sets basic settings.
  */
  public function __construct()
  {
    $this->logFile = new Logging('sortsystem.log');
    $host = php_uname('n');
    $this->host = substr($host, 0, (strpos($host, '.') ? strpos($host, '.') : strlen($host)));
    $this->connect();
    $this->timestamp = time();
    $this->dateDanish = date('d/m-Y', $this->timestamp);
    $this->dateInternational = date('Y-m-d', $this->timestamp);
  }
  
  public function __destruct()
  {
    $this->disconnect();
  }
  
  /**
  * Main method.
  * 
  * @desc Main method for delegating recipients. It is only be necessary to call this.
  * 
  * @return Boolean TRUE if delegation completed without problems, otherwise FALSE.
  */
  public function start()
  {
    $this->logFile->write('Initializing.');
    
    try
    {
      // Prepare SQL statements.
      $this->prepareStatements();

      if(!$this->checkStatus('busy') && !DEV_VERSION)
      {
        $this->logFile->write('Not allowed to run because status is \'busy\' or \'disabled\', quitting.');
        return false;
      }
      
      //Update version
      $this->updateVersion();
      
      // Quick check to see if there is anything to delegate.
      if(!$this->hasReservedRecipients())
      {
        $this->logFile->write('Nothing to delegate, quitting.');
        $this->changeStatus('available');
        return true;
      }
      
      // Reserve and delegate recipients.
      $this->delegateRecipients();
    }
    catch(Exception $someException)
    {
      $this->logFile->write('Error: ' . $someException->getMessage());
      return false;
    }

    $this->changeStatus('available');
    $this->logFile->write('Nothing more to delegate, quitting.');
    return true;
  }

  /**
  * Prepare statements.
  * 
  * @desc Prepares all statements that are used in this script.
  */
  private function prepareStatements()
  {
    // Check if the host exists and is not disabled.
    $sql = 'SELECT id, status FROM sortservers WHERE host = :host';
    $this->sortServerStatusStatement = $this->primaryDbConnection->prepare($sql);
    $this->sortServerStatusStatement->bindParam(':host', $this->host, PDO::PARAM_INT);

    // Prepare the SQL command to update sortserver status.
    $sql = 'UPDATE sortservers SET status = :newStatus, updated_at = NOW() WHERE id = :id AND status != :oldStatus';
    $this->changeSortServerStatusStatement = $this->primaryDbConnection->prepare($sql);
    $this->changeSortServerStatusStatement->bindParam(':id', $this->id, PDO::PARAM_INT);
    $this->changeSortServerStatusStatement->bindValue(':oldStatus', 'disabled', PDO::PARAM_STR);

    // Prepare the SQL command to update sortserver version.
    $sql = 'UPDATE sortservers SET version = :newVersion WHERE id = :id';
    $this->updateSortServerVersionStatement = $this->primaryDbConnection->prepare($sql);
    $this->updateSortServerVersionStatement->bindParam(':id', $this->id, PDO::PARAM_INT);

    // Prepare the SQL command to check if any recipients has been reserved.
    $sql = 'SELECT id FROM newsletter_recipients WHERE status = :status AND reserved_by = :serverId LIMIT 1';
    $this->hasRecipientsStatement = $this->primaryDbConnection->prepare($sql);
    $this->hasRecipientsStatement->bindValue(':status', 'reserved', PDO::PARAM_STR);
    $this->hasRecipientsStatement->bindParam(':serverId', $this->id, PDO::PARAM_INT);
    
    // Prepare the SQL command to check if any recipients are reserved by this sortserver for a given newsletter.
    $sql = 'SELECT id FROM newsletter_recipients WHERE newsletter_ref_id = :letterId AND status = :status AND reserved_by = :serverId LIMIT 1';
    $this->letterHasRecipientsStatement = $this->primaryDbConnection->prepare($sql);
    $this->letterHasRecipientsStatement->bindValue(':status', 'reserved', PDO::PARAM_STR);
    $this->letterHasRecipientsStatement->bindParam(':serverId', $this->id, PDO::PARAM_INT);
    
    // Prepare the SQL command to select newsletters by time to send.
    $sql = 'SELECT id, current_state FROM newsletters WHERE status = :delegate OR status = :delegating ORDER BY time_to_send';
    $this->earliestFirstStatement = $this->primaryDbConnection->prepare($sql);
    $this->earliestFirstStatement->bindValue(':delegate', 'delegate', PDO::PARAM_STR);
    $this->earliestFirstStatement->bindValue(':delegating', 'delegating', PDO::PARAM_STR);
    
    // Prepare the SQL command to select recipients.
    $sql = 'SELECT id, newsletter_ref_id, template_ref_id, user_ref_id, name, email, is_hotmail, culture, external_pass FROM newsletter_recipients WHERE newsletter_ref_id = :newsletterId AND status = :status AND reserved_by = :serverId LIMIT ' . MAX_BATCH_SIZE . ' FOR UPDATE';
    $this->recipientStatement = $this->primaryDbConnection->prepare($sql);
    $this->recipientStatement->bindValue(':status', 'reserved', PDO::PARAM_STR);
    $this->recipientStatement->bindParam(':serverId', $this->id, PDO::PARAM_INT);
  
    // Prepare the SQL command to get the template.
    $sql = 'SELECT id, sender, header, body FROM email_templates WHERE newsletter_ref_id = :letterId AND type = :type';
    $this->templateStatement = $this->primaryDbConnection->prepare($sql);

    // Prepare the SQL command to get the newsletter data.
    $sql = 'SELECT list_ref_id, manager_ref_id, mail_format, subject, current_state, subject_for_first_resend, subject_for_second_resend, time_to_send, use_limits, from_zipcode, to_zipcode, include_empty_zipcodes, from_year_of_birth, to_year_of_birth, include_empty_year_of_births, gender, include_empty_genders FROM newsletters WHERE id = :id';
    $this->letterStatement = $this->primaryDbConnection->prepare($sql);

    // Prepare the SQL command to get newsletter interests.
    $sql = 'SELECT interest_ref_id FROM newsletter_interests WHERE newsletter_ref_id = :newsletter';
    $this->interestStatement = $this->primaryDbConnection->prepare($sql);

    // Prepare the SQL command to check user interests.
    $sql = 'SELECT id FROM user_interests WHERE user_ref_id = :user AND interest_ref_id = :interest';
    $this->userInterestStatement = $this->primaryDbConnection->prepare($sql);

    // Prepare the SQL command to delete a recipient.
    $sql = 'DELETE FROM newsletter_recipients WHERE id = :id';
    $this->deleteRecipientStatement = $this->primaryDbConnection->prepare($sql);

    // Prepare the SQL command to get the list data.
    $sql = 'SELECT name, footer_link_type, footer_link_text, id FROM mailinglists WHERE id = :listId';
    $this->listStatement = $this->primaryDbConnection->prepare($sql);

    // Prepare the SQL command to get the datafields.
    $sql = 'SELECT id, name, token, default_value, is_gender, is_birth_year FROM datafields WHERE mailinglist_ref_id = :listId';
    $this->datafieldStatement = $this->primaryDbConnection->prepare($sql);

    // Prepare the SQL command to check for datafield values.
    $sql = 'SELECT id FROM datafield_values WHERE user_ref_id = :userId LIMIT 1';
    $this->checkStatement = $this->primaryDbConnection->prepare($sql);

    // Prepare the SQL command to get the datafield values.
    $sql = 'SELECT value FROM datafield_values WHERE datafield_ref_id = :datafieldId AND user_ref_id = :userId';
    $this->valueStatement = $this->primaryDbConnection->prepare($sql);

    // Prepare the SQL command to create the emails.
    $sql = 'INSERT INTO secondary_emails VALUES (NULL, :managerId, :templateId, :newsletterId, :envelopeSender, :recipient, :isHotmail, :header, :body, :timeToSend)';
    $this->emailStatement = $this->secondaryDbConnection->prepare($sql);

    // Prepare the SQL command to create email status.
    $sql = 'INSERT INTO secondary_email_status VALUES (NULL, :emailId, :status)';
    $this->emailStatusStatement = $this->secondaryDbConnection->prepare($sql);
    $this->emailStatusStatement->bindValue(':status', 'send', PDO::PARAM_STR);
    
    // Prepare the SQL command to update newsletter recipient status.
    $sql = 'UPDATE newsletter_recipients SET status = :recipientStatus WHERE id = :recipientId';
    $this->statusStatement = $this->primaryDbConnection->prepare($sql);
    $this->statusStatement->bindValue(':recipientStatus', 'sent', PDO::PARAM_STR);
    
    // Prepare the SQL command to examine if delegation has finished.
    $sql = 'SELECT DISTINCT status FROM newsletter_recipients WHERE newsletter_ref_id = :letterId';
    $this->isFinishedStatement = $this->primaryDbConnection->prepare($sql);

    // Prepare the SQL command to update newsletter status.
    $sql = 'UPDATE newsletters SET status = :newStatus WHERE id = :letterId';
    $this->newsletterStatement = $this->primaryDbConnection->prepare($sql);

    // Prepare the SQL command to update template status.
    $sql = 'UPDATE email_templates SET status = \'send\' WHERE id = :templateId';
    $this->emailTemplateStatement = $this->primaryDbConnection->prepare($sql);
    
    $sql = 'SELECT nr_sent FROM newsletters WHERE id = :letterId';
    $this->checkNewsletterStatement = $this->primaryDbConnection->prepare($sql);
}
  
  /**
  * Has reserved recipients.
  * 
  * @desc Returns if any recipients were reserved by this sorting server.
  * 
  * @return Boolean TRUE if there are available recipients, otherwise FALSE.
  */
  private function hasReservedRecipients()
  {
    $this->hasRecipientsStatement->execute();
    $resultSet = $this->hasRecipientsStatement->fetchAll(PDO::FETCH_ASSOC);
    return (count($resultSet) > 0);
  }
  
  /**
  * Delegate recipients.
  * 
  * @desc This method delegates the recipients.
  * It only delegates the amount defined by MAX_BATCH_SIZE.
  */
  private function delegateRecipients()
  {
    $nrDelegated = 0;
    $nrDelegatedTotal = 0;
    $timeStart = microtime(true);

    // Bind variables to the prepared statements.
    $this->recipientStatement->bindParam(':newsletterId', $letterId, PDO::PARAM_INT);
    $this->templateStatement->bindParam(':letterId', $letterId, PDO::PARAM_INT);
    $this->templateStatement->bindParam(':type', $currentState, PDO::PARAM_INT);
    $this->letterStatement->bindParam(':id', $letterId, PDO::PARAM_INT);
    $this->interestStatement->bindParam(':newsletter', $letterId, PDO::PARAM_INT);
    $this->userInterestStatement->bindParam(':user', $userId, PDO::PARAM_INT);
    $this->userInterestStatement->bindParam(':interest', $interestId, PDO::PARAM_INT);
    $this->deleteRecipientStatement->bindParam(':id', $recipientId, PDO::PARAM_INT);
    $this->listStatement->bindParam(':listId', $listId, PDO::PARAM_INT);
    $this->datafieldStatement->bindParam(':listId', $listId, PDO::PARAM_INT);
    $this->checkStatement->bindParam(':userId', $userId, PDO::PARAM_INT);
    $this->letterHasRecipientsStatement->bindParam(':letterId', $letterId, PDO::PARAM_INT);
    $this->emailStatement->bindParam(':managerId', $managerId, PDO::PARAM_INT);
    $this->emailStatement->bindParam(':templateId', $templateId, PDO::PARAM_INT);
    $this->emailStatement->bindParam(':newsletterId', $letterId, PDO::PARAM_INT);
    $this->emailStatement->bindParam(':envelopeSender', $envelopeSender, PDO::PARAM_STR);
    $this->emailStatement->bindParam(':recipient', $recipient, PDO::PARAM_STR);
    $this->emailStatement->bindParam(':isHotmail', $isHotmail, PDO::PARAM_INT);
    $this->emailStatement->bindParam(':header', $header, PDO::PARAM_STR);
    $this->emailStatement->bindParam(':body', $body, PDO::PARAM_STR);
    $this->emailStatement->bindParam(':timeToSend', $timeToSend, PDO::PARAM_STR);
    $this->emailStatusStatement->bindParam(':emailId', $emailId, PDO::PARAM_INT);
    $this->statusStatement->bindParam(':recipientId', $recipientId, PDO::PARAM_INT);
    $this->isFinishedStatement->bindParam(':letterId', $letterId, PDO::PARAM_INT);
    $this->newsletterStatement->bindParam(':letterId', $letterId, PDO::PARAM_INT);
    $this->newsletterStatement->bindParam(':newStatus', $status, PDO::PARAM_STR);
    $this->emailTemplateStatement->bindParam(':templateId', $templateId, PDO::PARAM_INT);
    $this->checkNewsletterStatement->bindParam(':letterId', $letterId, PDO::PARAM_INT);
    
    // Global variables.
    $templateRow = null;
    $failureCount = 0;

    // Find the newsletters to send, ordered by sending time.
    $this->earliestFirstStatement->execute();
    $letterIds = $this->earliestFirstStatement->fetchAll(PDO::FETCH_ASSOC);
    if(count($letterIds) == 0)
    {
      $this->logFile->write('Could not find any newsletters to delegate, quitting.');
      return;
    }
  
    // Iterate over newsletters with status 'delegate' or 'delegating'.
    for($newsletterIndex = 0; $newsletterIndex < count($letterIds); $newsletterIndex++)
    {
      $letterId = $letterIds[$newsletterIndex]['id'];

      // Choose the next newsletter if this one has no more recipients to delegate.
      $this->letterHasRecipientsStatement->execute();
      $letterHasRecipients = $this->letterHasRecipientsStatement->fetchAll(PDO::FETCH_ASSOC);

      if(count($letterHasRecipients) > 0)
      {
        // Update newsletter status.
        $status = 'delegating';
        $this->newsletterStatement->execute();

        // Get all related letter data.
        $currentState = $letterIds[$newsletterIndex]['current_state'];
        
        // Prepare pre- and postfix strings for the identifier.
        $this->identifierPrefix = $letterId . '_';
        $this->identifierPostfix = '_' . $this->timestamp;
        
        // Get letter interests, if any.
        $this->interestStatement->execute();
        $interests = $this->interestStatement->fetchAll(PDO::FETCH_ASSOC);
        $nrOfInterests = count($interests);

        // Get the template.
        $this->templateStatement->execute();
        $templateRow = $this->templateStatement->fetchAll(PDO::FETCH_ASSOC);
        $sender = $templateRow[0]['sender'];
        $templateId = $templateRow[0]['id'];

        // Find tags.
        $headerTags = $this->findTags($templateRow[0]['header']);
        $bodyTags = $this->findTags($templateRow[0]['body']);
        $tags = array_unique(array_merge($headerTags, $bodyTags));

        // Get newsletter data.
        $this->letterStatement->execute();
        $letterRow = $this->letterStatement->fetchAll(PDO::FETCH_ASSOC);
        $listId = $letterRow[0]['list_ref_id'];
        $managerId = $letterRow[0]['manager_ref_id'];
        $mailFormat = $letterRow[0]['mail_format'];
        $useLimits = ((bool) $letterRow[0]['use_limits']);
        if($useLimits)
        {
          // Limits are used to delete recipients that do not meet them.
          $fromZipcode = $letterRow[0]['from_zipcode'];
          $toZipcode = $letterRow[0]['to_zipcode'];
          $emptyZipcodes = $letterRow[0]['include_empty_zipcodes'];
          $fromBirthYear = $letterRow[0]['from_year_of_birth'];
          $toBirthYear = $letterRow[0]['to_year_of_birth'];
          $emptyBirthYears = $letterRow[0]['include_empty_year_of_births'];
          $gender = $letterRow[0]['gender'];
          $emptyGenders = $letterRow[0]['include_empty_genders'];
        }
        if($letterRow[0]['current_state'] == 'send')
        {
          $subject = $letterRow[0]['subject'];
        }
        else if($letterRow[0]['current_state'] == 'first_resend')
        {
          $subject = $letterRow[0]['subject_for_first_resend'];
        }
        else
        {
          $subject = $letterRow[0]['subject_for_second_resend'];
        }
        $timeToSend = $letterRow[0]['time_to_send'];
        
        // Get list data.
        $this->listStatement->execute();
        $listRow = $this->listStatement->fetchAll(PDO::FETCH_ASSOC);
        
        // Get datafields.
        $this->datafieldStatement->execute();
        $datafields = $this->datafieldStatement->fetchAll(PDO::FETCH_ASSOC);
        $tags = $this->matchTags($tags, $datafields);
        
        // Find indexes for datafields that are used to check limits.
        foreach($datafields as $index => $someDatafield)
        {
          if($someDatafield['is_gender'])
          {
            $indexForGender = $index;
          }
          else if($someDatafield['is_birth_year'])
          {
            $indexForBirthYear = $index;
          }
          else if($someDatafield['token'] == 'zipcode')
          {
            $indexForZipcode = $index;
          }
        }
      }
      
      while(count($letterHasRecipients) > 0)
      {
        // Batch statistics.
        $batchStart = microtime(true);
        $nrDelegated = 0;
        
        // Find the corresponding recipients that have been reserved by this sortserver.
        $this->recipientStatement->execute();
        $recipientRows = $this->recipientStatement->fetchAll(PDO::FETCH_ASSOC);
        
        // Iterate over each recipient.
        for($recipientNr = 0; $recipientNr < count($recipientRows); $recipientNr++)
        {
          // valueStatement has to be rebound here, because it is also rebounded in method mergeDatafields.
          $this->valueStatement->bindParam(':datafieldId', $datafieldId, PDO::PARAM_INT);
          $this->valueStatement->bindParam(':userId', $userId, PDO::PARAM_INT);

          $recipientRow = $recipientRows[$recipientNr];
          $recipientId = $recipientRow['id'];
          $userId = $recipientRow['user_ref_id'];
          
          // Check for user interests if there are any for the newsletter.
          if($nrOfInterests > 0)
          {
            for($index = 0; $index < count($interests); $index++)
            {
              $interestId = $interests[$index]['interest_ref_id'];
              $this->userInterestStatement->execute();
              if(count($this->userInterestStatement->fetchAll()) > 0)
              {
                // Remove the recipient if he or she does not have any of the required interests.
                $this->deleteRecipientStatement->execute();
                continue 2;
              }
            }
          }
          
          // Check for limits if there are any for the newsletter.
          if($useLimits)
          {
            $deleteRecipient = false;
            
            // Check each limit.
            if(($fromZipcode) && ($toZipcode) && ($indexForZipcode))
            {
              // Check zipcodes.
              $datafieldId = $datafields[$indexForZipcode]['id'];
              $this->valueStatement->execute();
              $value = $this->valueStatement->fetchAll();
              if(count($value) == 0)
              {
                if(!$emptyZipcodes)
                {
                  // No value, so the recipient should be deleted if zip codes are not allowed to be empty.
                  $deleteRecipient = true;
                }
              }
              else if(($value[0]['value'] < $fromZipcode) || ($value[0]['value'] > $toZipcode))
              {
                // A value exists. Delete the recipient only if the value is not within the limit.
                $deleteRecipient = true;
              }
            }
            else if(($fromBirthYear) && ($toBirthYear) && ($indexForBirthYear) && (!$deleteRecipient))
            {
              // Check year of births.
              $datafieldId = $datafields[$indexForBirthYear]['id'];
              $this->valueStatement->execute();
              $value = $this->valueStatement->fetchAll();
              if(count($value) == 0)
              {
                if(!$emptyBirthYears)
                {
                  // No value, so the recipient should be deleted if birth years are not allowed to be empty.
                  $deleteRecipient = true;
                }
              }
              else if(($value[0]['value'] < $fromBirthYear) || ($value[0]['value'] > $toBirthYear))
              {
                // A value exists. Delete the recipient only if the value is not within the limit.
                $deleteRecipient = true;
              }
            }
            else if(($gender) && ($indexForGender) && (!$deleteRecipient))
            {
              // Check genders.
              $datafieldId = $datafields[$indexForGender]['id'];
              $this->valueStatement->execute();
              $value = $this->valueStatement->fetchAll();
              if(count($value) == 0)
              {
                if(!$emptyGenders)
                {
                  // No value, so the recipient should be deleted if genders are not allowed to be empty.
                  $deleteRecipient = true;
                }
              }
              else if($value[0]['value'] != $gender)
              {
                // A value exists. Delete the recipient only if the value is not within the limit.
                $deleteRecipient = true;
              }
            }
            
            if($deleteRecipient)
            {
              // Recipient is not included in a limit. Delete him or her and continue.
              $this->deleteRecipientStatement->execute();
              continue;
            }
          }

          $envelopeSender = 'Mailinglist System <' . $listId . '_' . $recipientRow['user_ref_id'] . '@' . MAILBOX . '>';
          
          $recipient = ($recipientRow['name'] ? $recipientRow['name'] . ' <' : '') . $recipientRow['email'] . ($recipientRow['name'] ? '>' : '');
          $isHotmail = $recipientRow['is_hotmail'];

          // General check for datafield values.
          $this->checkStatement->execute();
          $checkRow = $this->checkStatement->fetchAll(PDO::FETCH_ASSOC);
          $hasDatafields = (count($checkRow) > 0);
          
          // Merge tags with datafield values.
          $tags = $this->mergeDatafields($userId, $hasDatafields, $tags, $envelopeSender, $recipient, $recipientRow, $listRow);

          // Replace tags.
          $header = str_ireplace($tags['tag'], $tags['value'], $templateRow[0]['header']);
          $body = str_ireplace($tags['tag'], $tags['value'], $templateRow[0]['body']);
          
          // Some extra debugging.
          if(empty($timeToSend))
          {
            $this->logFile->write('ERROR: time_to_send is empty! Nr. of recipients: ' . count($letterHasRecipients) . '. It came from this letter: ' . serialize($letterRow) . '.');
          }
          
          // Create the email and corresponding status.
          $this->emailStatement->execute();
          $emailId = $this->secondaryDbConnection->lastInsertId();
          $this->emailStatusStatement->execute();
          $this->statusStatement->execute();
          
          $nrDelegated++;
        }

        // Write a batch summary to the log.
        $batchEnd = microtime(true);
        $time = number_format(($batchEnd - $batchStart), 2);
        $this->logFile->write('Delegated ' . $nrDelegated . ' recipients in ' . $time . ' seconds.');
        $nrDelegatedTotal += $nrDelegated;
        $nrDelegated = 0;
        
        // Check sortserver status before continuing.
        if(!$this->checkStatus('busy', false) && !DEV_VERSION)
        {
          $this->logFile->write('Asked to quit by host.');
          return;
        }

        // Check if this newsletter has any more recipients that are reserved by this sortserver.
        $this->letterHasRecipientsStatement->execute();
        $letterHasRecipients = $this->letterHasRecipientsStatement->fetchAll(PDO::FETCH_ASSOC);
      }

      // Check if the whole newsletter has been delegated.
      $isFinished = true;
      $this->isFinishedStatement->execute();
      $isFinishedRow = $this->isFinishedStatement->fetchAll(PDO::FETCH_ASSOC);
      for($isFinishedIndex = 0; $isFinishedIndex < count($isFinishedRow); $isFinishedIndex++)
      {
        // Delegation is finished when status for all recipients is 'sent'.
        if($isFinishedRow[$isFinishedIndex]['status'] != 'sent')
        {
          $isFinished = false;
          break;
        }
      }
      
      if($isFinished)
      {
        // Check if the newsletter has begun sending. If so, set status to 'sending', or else set it to 'send'.
        $this->checkNewsletterStatement->execute();
        $checkNewsletterStatus = $this->checkNewsletterStatement->fetchAll(PDO::FETCH_ASSOC);
        $status = ($checkNewsletterStatus[0]['nr_sent'] > 0 ? 'sending' : 'send');
        $this->newsletterStatement->execute();
        $this->emailTemplateStatement->execute();
      }
    }
    
    // Calculate time spent.
    $timeEnd = microtime(true);
    
    // Write a final summary to the log.
    if($nrDelegated > 0)
    {
      $this->logFile->write('Finished. Delegated a total of ' . number_format($nrDelegatedTotal) . ' recipients in ' . number_format(($timeEnd - $timeStart), 2) . ' seconds.');
    }
  }

  /**
  * Merge datafields.
  * 
  * @desc Takes an associative array of tags and finds the datafield values
  * for each tag, if one exists. If not, an empty value is used instead.
  * 
  * The tag list is expected to contain the following keys: tag, type, name, id,
  * default_value, is_gender and value where id is the id of the datafield.
  * The returned tag list has the value filled out correctly. The rest of the tag list
  * is unmodified.
  * 
  * @param Integer User id of the user to send the newsletter to.
  * @param Boolean Whether or not to check datafields.
  * @param Array List of tags.
  * @param String Envelope sender, correctly formatted as "name <email@domain.ext>".
  * @param String Recipient, correctly formatted as "name <email@domain.ext>" or just "email@domain.ext".
  * @param Array List of recipient info.
  * @param Array List of mailinglist info.
  * @return Array List of modified tags.
  */
  private function mergeDatafields($aUserId, $aDatafieldsChoice, $anArrayOfTags, $anEnvelopeSender, $aRecipient, $aRecipientInfoArray, $aListInfoArray)
  {
    if($aDatafieldsChoice)
    {
      // Bind parametes to the SQL command to fetch datafield values.
      $this->valueStatement->bindParam(':datafieldId', $datafieldId, PDO::PARAM_INT);
      $this->valueStatement->bindParam(':userId', $aUserId, PDO::PARAM_INT);
    }
    
    // Merge tags with datafield and other values.
    for($index = 0; $index < count($anArrayOfTags['id']); $index++)
    {
      $matches = array();
      
      if(($aDatafieldsChoice) && ($anArrayOfTags['type'][$index] == 'datafield'))
      {
        $datafieldId = $anArrayOfTags['id'][$index];
        $this->valueStatement->execute();
        $valueRow = $this->valueStatement->fetchAll(PDO::FETCH_ASSOC);
        $anArrayOfTags['value'][$index] = ($valueRow ? $valueRow[0]['value'] : $anArrayOfTags['default_value'][$index]);
        if(($anArrayOfTags['is_gender'][$index]) && ($aRecipientInfoArray['culture'] == 'da') && ($anArrayOfTags['value'][$index]))
        {
          // Danish words for gender.
          if($anArrayOfTags['value'][$index] == 'male')
          {
            $anArrayOfTags['value'][$index] = 'mand';
          }
          else if($anArrayOfTags['value'][$index] == 'female')
          {
            $anArrayOfTags['value'][$index] = 'kvinde';
          }
        }
      }
      else if(preg_match('/^name\|?(.*)?$/i', $anArrayOfTags['name'][$index], $matches))
      {
        if(empty($aRecipientInfoArray['name']))
        {
          $anArrayOfTags['value'][$index] = (empty($matches[1]) ? '' : $matches[1]);
        }
        else
        {
          $anArrayOfTags['value'][$index] = $aRecipientInfoArray['name'];
        }
      }
      else if($anArrayOfTags['name'][$index] == 'email')
      {
        $anArrayOfTags['value'][$index] = $aRecipientInfoArray['email'];
      }
      else if($anArrayOfTags['name'][$index] == 'listname')
      {
        $anArrayOfTags['value'][$index] = $aListInfoArray[0]['name'];
      }
      else if($anArrayOfTags['name'][$index] == 'adminurl')
      {
        if($aListInfoArray[0]['footer_link_type'] == 'unsubscribe')
        {
          $anArrayOfTags['value'][$index] = ($aRecipientInfoArray['culture'] == 'da' ? USERSERVICE_DA : USERSERVICE_OTHER) . '/mailinglist/unsubscribeExternal/id/' . $aListInfoArray[0]['id'] . '/userId/' . $aRecipientInfoArray['user_ref_id'].'/externalPass/'. $aRecipientInfoArray['external_pass'];
        }
        else if($aListInfoArray[0]['footer_link_type'] == 'admin')
        {
          $anArrayOfTags['value'][$index] = ($aRecipientInfoArray['culture'] == 'da' ? USERSERVICE_DA : USERSERVICE_OTHER) . '/mailinglist/adminExternal/id/' . $aListInfoArray[0]['id'] . '/userId/' . $aRecipientInfoArray['user_ref_id'].'/externalPass/'. $aRecipientInfoArray['external_pass'];
        }
      }
      else if($anArrayOfTags['name'][$index] == 'adminlink')
      {
        if($aListInfoArray[0]['footer_link_type'] == 'unsubscribe')
        {
          $anArrayOfTags['value'][$index] = '<a href="' . ($aRecipientInfoArray['culture'] == 'da' ? USERSERVICE_DA : USERSERVICE_OTHER) . '/mailinglist/unsubscribeExternal/id/' . $aListInfoArray[0]['id'] . '/userId/' . $aRecipientInfoArray['user_ref_id'].'/externalPass/'. $aRecipientInfoArray['external_pass'] .'">' . $aListInfoArray[0]['footer_link_text'] . '</a>';
        }                                                                           
        else if($aListInfoArray[0]['footer_link_type'] == 'admin')
        {
          $anArrayOfTags['value'][$index] = '<a href="' . ($aRecipientInfoArray['culture'] == 'da' ? USERSERVICE_DA : USERSERVICE_OTHER) . '/mailinglist/adminExternal/id/' . $aListInfoArray[0]['id'] . '/userId/' . $aRecipientInfoArray['user_ref_id'].'/externalPass/'. $aRecipientInfoArray['external_pass'] . '">' . $aListInfoArray[0]['footer_link_text'] . '</a>';
        }
      }
      else if($anArrayOfTags['name'][$index] == 'date')
      {
        $anArrayOfTags['value'][$index] = ($aRecipientInfoArray['culture'] == 'da' ? $this->dateDanish : $this->dateInternational);
      }
      else if($anArrayOfTags['name'][$index] == 'userid')
      {
        $anArrayOfTags['value'][$index] = $aRecipientInfoArray['user_ref_id'];
      }
      else if($anArrayOfTags['name'][$index] == 'externalpass')
      {
        $anArrayOfTags['value'][$index] = $aRecipientInfoArray['external_pass'];
      }
      else if($anArrayOfTags['name'][$index] == 'recipient')
      {
        $anArrayOfTags['value'][$index] = $aRecipient;
      }
      else if($anArrayOfTags['name'][$index] == 'envelopesender')
      {
        $anArrayOfTags['value'][$index] = $anEnvelopeSender;
      }
      else if($anArrayOfTags['name'][$index] == 'identifier')
      {
        $anArrayOfTags['value'][$index] = $this->identifierPrefix . $aRecipientInfoArray['user_ref_id'] . $this->identifierPostfix . $aRecipientInfoArray['user_ref_id'];
      }
      else if($anArrayOfTags['name'][$index] == 'letterurl')
      {
        $anArrayOfTags['value'][$index] = ($aRecipientInfoArray['culture'] == 'da' ? URL_DA : URL_INT) . '/newsletter/show/id/' . $aRecipientInfoArray['newsletter_ref_id'];
      }
      else if($anArrayOfTags['name'][$index] == 'letterlink')
      {
        $link = ($aRecipientInfoArray['culture'] == 'da' ? URL_DA : URL_INT) . '/newsletter/show/id/' . $aRecipientInfoArray['newsletter_ref_id'];
        $anArrayOfTags['value'][$index] = '<a href="' . $link . '">' . $link . '</a>';
      }
      else
      {
        $anArrayOfTags['value'][$index] = '';
      }
    }
    return $anArrayOfTags;
  }
  
  /**
  * Connects to the databases.
  * 
  * @desc If a connection cannot be established, an error message is written
  * to the log file, and an exception is thrown.
  */
  public function connect()
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
  * Closes the connection to the database.
  * 
  * @desc On failure, nothing is done.
  */
  public function disconnect()
  {
    $this->primaryDbConnection = null;
    $this->secondaryDbConnection = null;
  }
  
  /**
  * Find tags.
  * 
  * @desc Locates and returns a list of all tags [x] that was found in the text.
  * All returned tags are converted to lowercase letters.
  * 
  * @return Array List of tags as strings.
  */
  private function findTags($aText)
  {
    $matches = array();
    preg_match_all('/\[[^\]]*\]/i', $aText, $matches);
    return array_map('strtolower', $matches[0]);
  }
  
  /**
  * Match tags.
  * 
  * @desc From two arrays, this method matches the tags to the datafields.
  * If a datafield token matches the tag, the datafield data is put together with the
  * tag name in the resulting array, and an extra space per tag for the final value.
  * If not, the tag name is still put into the array, but with another type and no extra
  * data.
  */
  private function matchTags($aListOfTags, $aListOfDatafields)
  {
    $tags = array();
    foreach($aListOfTags as $someTag)
    {
      $isDatafield = false;
      $tag = trim($someTag, '[ ]');
      for($index = 0; $index < count($aListOfDatafields); $index++)
      {
        if($aListOfDatafields[$index]['token'] == $tag)
        {
          // This is a datafield tag.
          $tags['tag'][] = $someTag;
          $tags['type'][] = 'datafield';
          $tags['name'][] = $aListOfDatafields[$index]['token'];
          $tags['id'][] = $aListOfDatafields[$index]['id'];
          $tags['default_value'][] = $aListOfDatafields[$index]['default_value'];
          $tags['is_gender'][] = $aListOfDatafields[$index]['is_gender'];
          $tags['value'][] = '';
          $isDatafield = true;
          break;
        }
      }
      if(!$isDatafield)
      {
        // This is a normal tag.
        $tags['tag'][] = $someTag;
        $tags['type'][] = 'general';
        $tags['name'][] = $tag;
        $tags['id'][] = null;
        $tags['default_value'][] = null;
        $tags['is_gender'][] = null;
        $tags['value'][] = '';
      }
    }
    
    return $tags;
  }

  /**
  * Check status.
  * 
  * @desc Checks the status of this sortserver. If it has been disabled in the database,
  * or if it is busy, FALSE is returned and causes the script to shut down nicely,
  * making sure that no recipients have been reserved. If the sortserver does not exist,
  * it's created, and status is set to the given. Afterwards, this object's id has been set.
  * 
  * @param String New status to set for the sortserver if status is ok.
  * @param Boolean Whether or not to check if the server is busy. If TRUE and it is busy, FALSE is returned.
  * @return Boolean TRUE if status is ok, otherwise FALSE.
  */
  private function checkStatus($aStatus, $aBusyCheck = true)
  {
    $this->sortServerStatusStatement->execute();
    $resultSet = $this->sortServerStatusStatement->fetchAll(PDO::FETCH_ASSOC);

    // Check host status.
    if(count($resultSet) == 0)
    {
      // Create the host.
      $insertSql = 'INSERT INTO sortservers (host, status, version, updated_at, created_at) VALUES (:host, :status, :version, NOW(), NOW())';
      $insertStatement = $this->primaryDbConnection->prepare($insertSql);
      $insertStatement->bindValue(':host', $this->host, PDO::PARAM_STR);
      $insertStatement->bindValue(':status', $aStatus);
      $insertStatement->bindValue(':version', SORT_VERSION);
      $insertStatement->execute();
      $this->id = $this->primaryDbConnection->lastInsertId();
    }
    else if(($resultSet[0]['status'] == 'disabled') || (($aBusyCheck) && ($resultSet[0]['status'] == 'busy')))
    {
      $this->id = $resultSet[0]['id'];
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
  * Change status.
  * 
  * @desc Changes the status of the sortserver.
  * @param String The status can be 'available', 'busy', 'unreachable', 'disabled' or 'unavailable'.
  */
  private function changeStatus($aStatus)
  {
    if(!DEV_VERSION)
    {
      $this->changeSortServerStatusStatement->bindParam(':newStatus', $aStatus, PDO::PARAM_STR);
	    $this->changeSortServerStatusStatement->execute();
    }
  }
  
  /**
  * Update sort version
  * 
  * @desc Updates the version of the sort server
  */
  private function updateVersion()
  {
    $this->updateSortServerVersionStatement->bindValue(':newVersion', SORT_VERSION, PDO::PARAM_STR);
	  $this->updateSortServerVersionStatement->execute();
  }
}