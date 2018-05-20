# Send System

This repository is part of a project developed for a Danish mailinglist provider in 2007. The project included an advanced control panel for admins and users alike, which allowed users to setup and manage mailinglists, and a distributed load-balanced mailing system written in PHP.

## About

The control panel was written in Symfony/PHP, using MySQL for data storage. The features that I developed for this system, include:

- Mailinglist statistics
- User management
- Dynamic signup forms
- Handling of bounce mails
- Design templates for emails, with mail merging and file attachments
- A WYSIWYG editor for template editing
- Scheduled newsletter sending
- Delegation of mailinglist administration to other users
- Recipient service with email blocking capabilities
- A distributed mail sending system

## The SendSystem in a Nutshell

The SendSystem needed to support a goal of being able to send one million emails per hour, something the previous system was incapable of.

I designed a distributed system, which could be scaled up in order to meet the demand. This was before AWS and GAE was a thing, so servers were being set up physically, and code deployed manually.

The SendSystem is divided into two parts:

- A SortingSystem, which has the sole responsibility of merging user data and other variables with the email templates.
- A SendingSystem, which is responsible for sending the emails, using SMTP.

Both of these parts consist of two sub-parts:

- A "Balancer", which uses a home-made algorithm for determining how many servers are available, and how to distribute the load among those servers.

- A "System", which is the sub-part that actually carries out the work.

The algorithm behind the Balancer has some basic functionality for distributing the work according to load: A server which is slow, for example, will not be allowed to fall further behind. Instead, the work is distributed while accounting for existing load, which also means that faster servers will receive more load than slow ones.

Each System is meant to be started by cron, and uses a simple lock file on the file system, to prevent it from being started multiple times when it's already running.

For reasons of efficiency, each System initializes itself by pre-preparing all the SQL statements that it needs to run, thereby saving time during execution. Furthermore, once a System has been started from cron, it will register itself as available for receiving work, in the database. The database can also be used to disable one or more Systems, which it will discover on its own, and subsequently exit gracefully.

## The Result

The Send System was able to meet the demand of sending one million emails per hour, simply by adding more servers to it without changing the source code. Also, it was able to grow beyond this requirement, with only a slight overhead in manual intervention and maintenance.

## Lessons Learned

- The basic architecture behind the SendSystem worked very well. Particularly dividing the implementation into a SortingSystem and a SendSystem was a good idea.
- Having each server automatically report itself as available for receiving work, was a really good idea, as it reduced the amount of manual labor that went into keeping the system running.
- I decided to implement the SendSystem without any dependencies to Symfony. This was a good choice, as it kept the SendSystem small and maintainable, and easy to deploy. However, the choice of PHP for this kind of heavy backend work was a mistake that I will not repeat. Back in 2007, I could have written it in Java instead. Looking at it now, I could implement it in Go. PHP is good at generating HTML, not for running server jobs.
- The SendSystem uses a centralized database for distributing work between servers. Although this worked fine, a contemporary implementation would probably use a pub/sub service for this purpose.
- Having cron start the service every 10 minutes, and then using a lock file to prevent it from running multiple processes on the same machine could have been considered a stroke of genius. Today, cron is a poor choice for keeping such a service running. Instead, it could keep itself alive inside a Docker container, and Kubernetes or a Docker restart policy could be embraced in order to keep it alive during unexpected crashes and errors.
