CREATE TABLE IF NOT EXISTS `aapl` (
  `nls_time` datetime NOT NULL,
  `nls_price` decimal(10,4) NOT NULL,
  `nls_volume` int(11) NOT NULL,
  `id` int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`),
  KEY `id` (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 AUTO_INCREMENT=1 ;

CREATE TABLE IF NOT EXISTS `aapl_second` (
  `nls_time` datetime NOT NULL,
  `nls_price` decimal(30,4) NOT NULL,
  `nls_volume` int(13) NOT NULL,
  `id` int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`),
  KEY `id` (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 AUTO_INCREMENT=1 ;

CREATE TABLE IF NOT EXISTS `aapl_log` (
  `type` VARCHAR(6) NOT NULL,
  `time_last_started` datetime NOT NULL,
  `time_last_finished` datetime NOT NULL,
  `time_execution` datetime NOT NULL,
  `records_created` int(11) NOT NULL,
  `status` VARCHAR(7) NOT NULL,
  `error_message` TEXT NOT NULL,
  `id` int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`),
  KEY `id` (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 AUTO_INCREMENT=1 ;

CREATE TABLE IF NOT EXISTS `aapl_progress` (
  `sale_time` int(11) NOT NULL,
  `pageno` int(11) NOT NULL,
  `records_partial` int(11) NOT NULL,
  `last_time` datetime NOT NULL,
  `id` int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`),
  KEY `id` (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 AUTO_INCREMENT=1 ;
