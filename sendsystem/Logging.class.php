<?php

/**
* @desc Provides basic logging functionality for independent scripts.
*/
class Logging
{
  protected $logFile;
  
  protected $fileName;
  
  public function __construct($aFilename)
  {
    $folder = dirname(__file__) . '/logs';

    // Create the folder if it does not exist.
    if(!is_dir($folder))
    {
      mkdir($folder, 0700);
    }
    $this->fileName = $folder . '/' . date('Y-m-d') . ' ' . $aFilename;
    $this->open();
  }
  
  public function __destruct()
  {
    $this->close();
  }
  
  /**
  * @desc Opens a log file that can be used to monitor the send system.
  */
  private function open()
  {
    if(!is_writable($this->fileName))
    {
      touch($this->fileName);
    }
    $this->logFile = @fopen($this->fileName, 'a');
    if(!$this->logFile)
      throw new Exception('Log file could not be opened.');
  }

  /**
  * @desc Closes the log file.
  */
  private function close()
  {
    fclose($this->logFile);
  }
  
  /**
  * @desc Writes a timestamped message to the log file.
  */
  public function write($aMessage)
  {
    fwrite($this->logFile, date('Y-m-d H:i:s') . ' ' . $aMessage . "\n");
  }
}