CREATE TABLE referral_mapping (
    ID int NOT NULL AUTO_INCREMENT COMMENT 'id of user',
    UUID VARCHAR(50) NOT NULL COMMENT 'UUID of the user resistered for referral service',
    CODE VARCHAR(10) NOT NULL COMMENT 'Referral code generated for user ',
    REFERRAR_UUID VARCHAR(50) NULL COMMENT 'UUID for the user who referred the user',
    MOBILE_VERIFIED BIT(1) DEFAULT 0 COMMENT 'if the mobile no of the user is verified',
    JOINING_BONUS BIT(1) DEFAULT 0 COMMENT 'does the user recived his joining bonus',
    FIRST_CHECKOUT BIT(1) DEFAULT 0 COMMENT 'have the user checked out from his first booking',
    REFERRAL_BONUS BIT(1) DEFAULT 0 COMMENT 'If the user, who referred the currunt user, have recived the referral bonus',
    CREATED TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'To be used in rare debug cases',
    LAST_UPDATED TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'To be used in rare debug cases',
    PRIMARY KEY (`ID`),
    UNIQUE (`UUID`),
    INDEX `CODE` (`CODE`),
    INDEX `REFERRAR_UUID` (`REFERRAR_UUID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE transactions (
    ID int NOT NULL AUTO_INCREMENT COMMENT 'id of transaction',
    TRANSACTION_ID VARCHAR(50) NOT NULL COMMENT 'UUID of the user resistered for referral service',
    TRANSACTION_TYPE TINYINT NOT NULL COMMENT 'weather it is a realised transaction or not 1 for realised and 0 for unrealised',
    REFERRAL_MAPPING_ID INT COMMENT 'Will hold the mapping with referral_mapping table and will be used to know which transaction is linked to which action',
    AFFECTED_USER_UUID VARCHAR(50) NOT NULL COMMENT 'UUID of the user afected by the transaction',
    DISCOUNT_TYPE TINYINT NOT NULL COMMENT 'The type of discount provided to user 0 for instant discount while 1 for referral bonus',
    EXTRA_META_DATA VARCHAR (20) DEFAULT NULL COMMENT 'Can be used to store any meta data',
    CREATED TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'To be used in rare debug cases',
    LAST_UPDATED TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'To be used in rare debug cases',
    PRIMARY KEY (`ID`),
    UNIQUE (`TRANSACTION_ID`),
    INDEX `REFERRAL_MAPPING_ID` (`REFERRAL_MAPPING_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;