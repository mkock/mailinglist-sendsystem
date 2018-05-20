
# This is a fix for InnoDB in MySQL >= 4.1.x
# It "suspends judgement" for fkey relationships until are tables are set.
SET FOREIGN_KEY_CHECKS = 0;

#-----------------------------------------------------------------------------
#-- emails on mailserver.
#-----------------------------------------------------------------------------

DROP TABLE IF EXISTS `local_emails`;

CREATE TABLE `local_emails`
(
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `manager_ref_id` INTEGER,
  `email_template_ref_id` INTEGER,
  `newsletter_ref_id` INTEGER,
  `envelope_sender` VARCHAR(100),
  `recipient` VARCHAR(100),
  `is_hotmail` INTEGER,
  `header` LONGBLOB,
  `body` LONGBLOB,
  PRIMARY KEY (`id`),
  INDEX `local_emails_FI_1` (`newsletter_ref_id`)
) ENGINE = InnoDB;

#-----------------------------------------------------------------------------
#-- email_status on mailserver.
#-----------------------------------------------------------------------------

DROP TABLE IF EXISTS `local_email_status`;

CREATE TABLE `local_email_status`
(
  `id` INTEGER NOT NULL AUTO_INCREMENT,
  `email_ref_id` INTEGER,
  `status` VARCHAR(100),
  `time_to_send` DATETIME,
  `reserved_by` INTEGER,
  `failure_count` INTEGER,
  PRIMARY KEY (`id`),
  INDEX `local_email_status_FI_1` (`email_ref_id`),
  CONSTRAINT `local_email_status_FK_1`
    FOREIGN KEY (`email_ref_id`)
    REFERENCES `local_emails` (`id`)
    ON DELETE CASCADE,
  INDEX `local_email_status_FI_2` (`status`),
  INDEX `local_email_status_FI_3` (`time_to_send`),
  INDEX `local_email_status_FI_4` (`reserved_by`)
) ENGINE = InnoDB;

SET FOREIGN_KEY_CHECKS = 1;