create table internalPayments(
	entryId varchar(40),
    bankBalanceDateIsGreaterThanEntryDate boolean,
    isVirtual boolean,
    accountId varchar(40),
    accountName varchar(50),
    accountIsDeleted boolean,
    date datetime,
    identifier varchar(240),
    value decimal(12, 2),
    isReconciliated boolean,
    isTransfer boolean,
    isFlagged boolean
);
