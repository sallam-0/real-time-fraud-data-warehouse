"""
SQL Queries for extracting MSSQL Source Tables
"""

SOURCE_TABLES = [
    {
        "table": "Customer",
        "s3_folder": "customer",
        "query": """
            SELECT CustomerID, FirstName, LastName, Email, DateOfBirth,
                   Gender, MaritalStatus, Occupation, StreetAddress,
                   City, State, Country, PostalCode, CustomerSince,
                   CustomerSegment, IDType, IDNumber,TaxID
            FROM dbo.Customer
        """,
    },
    {
        "table": "CustomerPhone",
        "s3_folder": "customer_phone",
        "query": """
            SELECT PhoneID, CustomerID, PhoneNumber, PhoneType,
                   IsPrimary
            FROM dbo.CustomerPhone
        """,
    },
    {
        "table": "Account",
        "s3_folder": "account",
        "query": """
            SELECT AccountNumber, CustomerID, AccountType, BranchID,
                   Balance, AvailableBalance, Currency, OpenDate,
                   LastActivityDate, InterestRate, MinimumBalance,
                   OverdraftLimit, AccountStatus, CreatedDate, ModifiedDate
            FROM dbo.Account
        """,
    },
    {
        "table": "Card",
        "s3_folder": "card",
        "query": """
            SELECT CardNumber, AccountNumber, CardType, CardNetwork,
                   IssueDate, ExpiryDate, CVV, PIN, CardHolderName,
                   DailyWithdrawalLimit, DailyPurchaseLimit,
                   SingleTransactionLimit, CardStatus
            FROM dbo.Card
        """,
    },
    {
        "table": "Branch",
        "s3_folder": "branch",
        "query": """
            SELECT BranchID, BranchName, BranchLocation, City,
                   State, Country, PostalCode, PhoneNumber, ManagerID, OpenDate, BranchType
            FROM dbo.Branch
        """,
    },
    {
        "table": "Department",
        "s3_folder": "department",
        "query": """
            SELECT DepartmentID, DepartmentName, ManagerID,
                   BranchID
            FROM dbo.Department
        """,
    },
    {
        "table": "Employee",
        "s3_folder": "employee",
        "query": """
            SELECT EmployeeID, FirstName, LastName, Email, PhoneNumber,
                   DateOfBirth, HireDate, Position, Salary,
                   DepartmentID, BranchID, SupervisorID
            FROM dbo.Employee
        """,
    },
    {
        "table": "TransactionType",
        "s3_folder": "transaction_type",
        "query": """
            SELECT TransactionTypeID, TransactionTypeName,
                   Category, Description
            FROM dbo.TransactionType
        """,
    },
    {
        "table": "Merchant",
        "s3_folder": "merchant",
        "query": """
            SELECT MerchantID, MerchantName, MerchantCategory, MCC,
                    StreetAddress, City, State, Country,
                   PostalCode, PhoneNumber, 
                   BusinessType, WebsiteURL
            FROM dbo.Merchant
        """,
    },
    {
        "table": "ATM",
        "s3_folder": "atm",
        "query": """
            SELECT ATMID, Location, StreetAddress, City, State,
                   Country, PostalCode, Latitude, Longitude,
                   BranchID, ATMType, Manufacturer, Model,
                   MaxWithdrawalAmount, CurrentCashLevel, ATMStatus, InstallationDate, 
                   LastMaintenanceDate
            FROM dbo.ATM
        """,
    },
    {
        "table": "Loan",
        "s3_folder": "loan",
        "query": """
            SELECT LoanID, CustomerID, AccountNumber, BranchID,
                   LoanType, LoanAmount, OutstandingAmount, InterestRate,
                   LoanTermMonths, StartDate, EndDate, NextPaymentDate,
                   MonthlyPayment,TotalPaid
            FROM dbo.Loan
        """,
    },
    {
        "table": "Transaction",
        "s3_folder": "transaction",
        "query": """
            SELECT TransactionID, AccountNumber, TransactionDate,
                   Amount, Currency, TransactionTypeID,
                   TransactionMethod, Channel, TransactionStatus,
                   MerchantID, ATMID, City, State, Country,
                   Latitude, Longitude, IsInternational,
                   DeviceID, DeviceType, IPAddress, CardNumber
            FROM dbo.[Transaction] WITH (NOLOCK)
        """,
    },
]
