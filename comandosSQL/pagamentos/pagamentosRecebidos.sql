create table paymentsReceivable(
     id varchar(40),
     categoryId varchar(40),
     categoryName varchar(50),
     value decimal(12, 2),
     type char(3),
     parent varchar(50),
     parentId varchar(40),
     scheduleId varchar(40),
     typeOperation char(10),
     isEntry boolean,
	 isBill boolean,
     isDebiteNote boolean,
     isFlagged boolean,
     isDued boolean,
     dueDate datetime,
     accrualDate datetime,
     scheduleDate datetime,
     createDate datetime,
     isPaid boolean,
     costCenterValueType int,
     paidValue decimal(12,2),
     openValue decimal(12,2),
     stakeholderId varchar(40),
     stakeholderType char(15),
     stakeholderName varchar(250),
     stakeholderIsDeleted boolean,
     description varchar(250),
     reference varchar(40),
     hasInstallment boolean,
     installmentId varchar(40),
     hasRecurrence boolean,
     hasOpenEntryPromise boolean,
     hasEntryPromise boolean,
     autoGenerateEntryPromise boolean,
     hasInvoice boolean,
     hasPendingInvoice boolean,
     hasScheduleInvoice boolean,
     autoGenerateNfseType int,
     isPaymentScheduled boolean,
     negativo boolean,
     status varchar(30),
     valorCorreto decimal(12,2)
 )	