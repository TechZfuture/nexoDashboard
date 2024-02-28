create table internalIncomingBills(
	entryId varchar(40),
    bankBalanceDateIsGreaterThanEntryDate boolean,
    isVirtual boolean,
    accountId varchar(40),
    accountName varchar(100),
    accountIsDeleted boolean,
    date datetime,
    identifier varchar(100),
    value decimal(12, 2),
    checkNumber varchar(200),
    isReconciliated boolean,
    isTransfer boolean,
    isFlagged boolean
);