-- --------------------------------------------------------
-- Host:                         127.0.0.1
-- Versione server:              10.5.2-MariaDB - mariadb.org binary distribution
-- S.O. server:                  Win64
-- HeidiSQL Versione:            10.2.0.5599
-- --------------------------------------------------------

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8 */;
/*!50503 SET NAMES utf8mb4 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;


-- Dump della struttura del database twitteremotions
CREATE DATABASE IF NOT EXISTS `twitteremotions` /*!40100 DEFAULT CHARACTER SET utf16 COLLATE utf16_unicode_ci */;
USE `twitteremotions`;

-- Dump della struttura di tabella twitteremotions.emoticon
CREATE TABLE IF NOT EXISTS `emoticon` (
  `ID` int(11) NOT NULL AUTO_INCREMENT,
  `Code` varchar(30) COLLATE utf16_unicode_ci NOT NULL DEFAULT '0',
  `Polarity` smallint(6) NOT NULL DEFAULT 0,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB AUTO_INCREMENT=12869 DEFAULT CHARSET=utf16 COLLATE=utf16_unicode_ci;

-- L’esportazione dei dati non era selezionata.

-- Dump della struttura di tabella twitteremotions.emoticoncount
CREATE TABLE IF NOT EXISTS `emoticoncount` (
  `Emotion` varchar(15) COLLATE latin7_general_cs NOT NULL,
  `IDEmoticon` int(11) NOT NULL,
  `Count` int(11) NOT NULL,
  PRIMARY KEY (`Emotion`,`IDEmoticon`),
  KEY `FK__emoticon` (`IDEmoticon`),
  CONSTRAINT `FK__emoticon` FOREIGN KEY (`IDEmoticon`) REFERENCES `emoticon` (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin7 COLLATE=latin7_general_cs;

-- L’esportazione dei dati non era selezionata.

-- Dump della struttura di tabella twitteremotions.hashtagcount
CREATE TABLE IF NOT EXISTS `hashtagcount` (
  `Emotion` varchar(15) COLLATE latin7_general_cs NOT NULL,
  `Hashtag` varchar(100) CHARACTER SET utf16 COLLATE utf16_unicode_ci NOT NULL,
  `Count` int(11) NOT NULL DEFAULT 0,
  PRIMARY KEY (`Emotion`,`Hashtag`)
) ENGINE=InnoDB DEFAULT CHARSET=latin7 COLLATE=latin7_general_cs;

-- L’esportazione dei dati non era selezionata.

-- Dump della struttura di tabella twitteremotions.negativeword
CREATE TABLE IF NOT EXISTS `negativeword` (
  `Word` varchar(15) COLLATE latin7_general_cs NOT NULL,
  PRIMARY KEY (`Word`)
) ENGINE=InnoDB DEFAULT CHARSET=latin7 COLLATE=latin7_general_cs;

-- L’esportazione dei dati non era selezionata.

-- Dump della struttura di tabella twitteremotions.slang
CREATE TABLE IF NOT EXISTS `slang` (
  `Slang` varchar(15) COLLATE latin7_general_cs NOT NULL,
  `Traduction` varchar(50) COLLATE latin7_general_cs NOT NULL,
  PRIMARY KEY (`Slang`)
) ENGINE=InnoDB DEFAULT CHARSET=latin7 COLLATE=latin7_general_cs;

-- L’esportazione dei dati non era selezionata.

-- Dump della struttura di tabella twitteremotions.stopword
CREATE TABLE IF NOT EXISTS `stopword` (
  `Word` varchar(50) COLLATE latin7_general_cs NOT NULL DEFAULT '0',
  PRIMARY KEY (`Word`)
) ENGINE=InnoDB DEFAULT CHARSET=latin7 COLLATE=latin7_general_cs;

-- L’esportazione dei dati non era selezionata.

-- Dump della struttura di tabella twitteremotions.tweet
CREATE TABLE IF NOT EXISTS `tweet` (
  `ID` int(11) NOT NULL AUTO_INCREMENT,
  `Emotion` varchar(15) COLLATE utf16_unicode_ci NOT NULL DEFAULT '0',
  `Text` varchar(1000) COLLATE utf16_unicode_ci NOT NULL DEFAULT '0',
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB AUTO_INCREMENT=3789893 DEFAULT CHARSET=utf16 COLLATE=utf16_unicode_ci;

-- L’esportazione dei dati non era selezionata.

-- Dump della struttura di tabella twitteremotions.wordcount
CREATE TABLE IF NOT EXISTS `wordcount` (
  `Emotion` varchar(15) CHARACTER SET utf8mb4 NOT NULL,
  `Word` varchar(200) CHARACTER SET utf16 COLLATE utf16_unicode_ci NOT NULL,
  `Count` int(11) NOT NULL DEFAULT 0,
  `FlagSentisense` tinyint(4) NOT NULL DEFAULT 0,
  `FlagNRC` tinyint(4) NOT NULL DEFAULT 0,
  `FlagEmoSN` tinyint(4) NOT NULL DEFAULT 0,
  PRIMARY KEY (`Emotion`,`Word`)
) ENGINE=InnoDB DEFAULT CHARSET=latin7 COLLATE=latin7_general_cs;

-- L’esportazione dei dati non era selezionata.

/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IF(@OLD_FOREIGN_KEY_CHECKS IS NULL, 1, @OLD_FOREIGN_KEY_CHECKS) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
