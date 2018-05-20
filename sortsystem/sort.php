$<?php
/**
* Main sort system file.
* 
* @desc This is the main file for sorting emails from a sorting server.
* It sorts a number of newsletter recipients that have already been reserved by
* the sortbalancer script.
* 
* Recipients are taken from the primary database and the resulting emails are
* inserted int the secondary database. These are defined in the config.php file.
* Two databases are used for efficiency, but there is no reason that the primary
* and the secondary database can't be the same one.
*/

define('SORT_SYSTEM_ROOT', realpath(dirname(__file__)));
set_include_path(get_include_path() . PATH_SEPARATOR . SORT_SYSTEM_ROOT);

require_once 'config.php';
require_once 'SortSystem.class.php';

$sortSystem = new SortSystem();
$sortSystem->start();