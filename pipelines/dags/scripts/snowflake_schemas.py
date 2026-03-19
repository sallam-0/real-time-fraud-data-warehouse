"""
Snowflake schema definitions and copy configurations.
"""

BATCH_COPY_CONFIGS = [
    {
        "target_table": "STAGING.RAW_CUSTOMER",
        "stage": "@STAGING.S3_BATCH_STAGE/customer/",
    },
    {
        "target_table": "STAGING.RAW_CUSTOMER_PHONE",
        "stage": "@STAGING.S3_BATCH_STAGE/customer_phone/",
    },
    {
        "target_table": "STAGING.RAW_ACCOUNT",
        "stage": "@STAGING.S3_BATCH_STAGE/account/",
    },
    {
        "target_table": "STAGING.RAW_CARD",
        "stage": "@STAGING.S3_BATCH_STAGE/card/",
    },
    {
        "target_table": "STAGING.RAW_BRANCH",
        "stage": "@STAGING.S3_BATCH_STAGE/branch/",
    },
    {
        "target_table": "STAGING.RAW_DEPARTMENT",
        "stage": "@STAGING.S3_BATCH_STAGE/department/",
    },
    {
        "target_table": "STAGING.RAW_EMPLOYEE",
        "stage": "@STAGING.S3_BATCH_STAGE/employee/",
    },
    {
        "target_table": "STAGING.RAW_TRANSACTION_TYPE",
        "stage": "@STAGING.S3_BATCH_STAGE/transaction_type/",
    },
    {
        "target_table": "STAGING.RAW_MERCHANT",
        "stage": "@STAGING.S3_BATCH_STAGE/merchant/",
    },
    {
        "target_table": "STAGING.RAW_ATM",
        "stage": "@STAGING.S3_BATCH_STAGE/atm/",
    },
    {
        "target_table": "STAGING.RAW_LOAN",
        "stage": "@STAGING.S3_BATCH_STAGE/loan/",
    },
    {
        "target_table": "STAGING.RAW_TRANSACTION",
        "stage": "@STAGING.S3_BATCH_STAGE/transaction/",
    },
]

REALTIME_COPY_CONFIGS = [
    {
        "target_table": "STAGING.RAW_REALTIME_TRANSACTION",
        "stage": "@STAGING.S3_REALTIME_TXN_STAGE",
    },
    {
        "target_table": "STAGING.RAW_REALTIME_FRAUD_DETECTION",
        "stage": "@STAGING.S3_REALTIME_DET_STAGE",
    },
]

# ── Batch RAW table DDL (auto-created before COPY INTO) ─────────────────
# All columns VARCHAR to accept any Parquet data; typing happens in dbt staging.
BATCH_RAW_TABLE_DDL = {
    "STAGING.RAW_CUSTOMER": [
        "CustomerID", "FirstName", "LastName", "Email", "DateOfBirth",
        "Gender", "MaritalStatus", "Occupation", "StreetAddress",
        "City", "State", "Country", "PostalCode", "CustomerSince",
        "CustomerSegment", "IDType", "IDNumber", "TaxID",
    ],
    "STAGING.RAW_CUSTOMER_PHONE": [
        "PhoneID", "CustomerID", "PhoneNumber", "PhoneType", "IsPrimary",
    ],
    "STAGING.RAW_ACCOUNT": [
        "AccountNumber", "CustomerID", "AccountType", "BranchID",
        "Balance", "AvailableBalance", "Currency", "OpenDate",
        "LastActivityDate", "InterestRate", "MinimumBalance",
        "OverdraftLimit", "AccountStatus", "CreatedDate", "ModifiedDate",
    ],
    "STAGING.RAW_CARD": [
        "CardNumber", "AccountNumber", "CardType", "CardNetwork",
        "IssueDate", "ExpiryDate", "CVV", "PIN", "CardHolderName",
        "DailyWithdrawalLimit", "DailyPurchaseLimit",
        "SingleTransactionLimit", "CardStatus",
    ],
    "STAGING.RAW_BRANCH": [
        "BranchID", "BranchName", "BranchLocation", "City",
        "State", "Country", "PostalCode", "PhoneNumber", "ManagerID",
        "OpenDate", "BranchType",
    ],
    "STAGING.RAW_DEPARTMENT": [
        "DepartmentID", "DepartmentName", "ManagerID", "BranchID",
    ],
    "STAGING.RAW_EMPLOYEE": [
        "EmployeeID", "FirstName", "LastName", "Email", "PhoneNumber",
        "DateOfBirth", "HireDate", "Position", "Salary",
        "DepartmentID", "BranchID", "SupervisorID",
    ],
    "STAGING.RAW_TRANSACTION_TYPE": [
        "TransactionTypeID", "TransactionTypeName", "Category", "Description",
    ],
    "STAGING.RAW_MERCHANT": [
        "MerchantID", "MerchantName", "MerchantCategory", "MCC",
        "StreetAddress", "City", "State", "Country",
        "PostalCode", "PhoneNumber", "BusinessType", "WebsiteURL",
    ],
    "STAGING.RAW_ATM": [
        "ATMID", "Location", "StreetAddress", "City", "State",
        "Country", "PostalCode", "Latitude", "Longitude",
        "BranchID", "ATMType", "Manufacturer", "Model",
        "MaxWithdrawalAmount", "CurrentCashLevel", "ATMStatus",
        "InstallationDate", "LastMaintenanceDate",
    ],
    "STAGING.RAW_LOAN": [
        "LoanID", "CustomerID", "AccountNumber", "BranchID",
        "LoanType", "LoanAmount", "OutstandingAmount", "InterestRate",
        "LoanTermMonths", "StartDate", "EndDate", "NextPaymentDate",
        "MonthlyPayment", "TotalPaid",
    ],
    "STAGING.RAW_TRANSACTION": [
        "TransactionID", "AccountNumber", "TransactionDate",
        "Amount", "Currency", "TransactionTypeID",
        "TransactionMethod", "Channel", "TransactionStatus",
        "MerchantID", "ATMID", "City", "State", "Country",
        "Latitude", "Longitude", "IsInternational",
        "DeviceID", "DeviceType", "IPAddress", "CardNumber",
    ],
}
