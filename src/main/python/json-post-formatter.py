import json

# Your JSON schema
schema = {
    "type": "record",
    "name": "TransactionRecord",
    "fields": [
        {"name": "accountNumber", "type": "string"},
        {"name": "customerId", "type": "string"},
        {"name": "creditLimit", "type": "float"},
        {"name": "availableMoney", "type": "float"},
        {"name": "transactionDateTime", "type": "string"},
        {"name": "transactionAmount", "type": "float"},
        {"name": "merchantName", "type": "string"},
        {"name": "acqCountry", "type": "string"},
        {"name": "merchantCountryCode", "type": "string"},
        {"name": "posEntryMode", "type": "string"},
        {"name": "posConditionCode", "type": "string"},
        {"name": "merchantCategoryCode", "type": "string"},
        {"name": "currentExpDate", "type": "string"},
        {"name": "accountOpenDate", "type": "string"},
        {"name": "dateOfLastAddressChange", "type": "string"},
        {"name": "cardCVV", "type": "string"},
        {"name": "enteredCVV", "type": "string"},
        {"name": "cardLast4Digits", "type": "string"},
        {"name": "transactionType", "type": "string"},
        {"name": "echoBuffer", "type": "string"},
        {"name": "currentBalance", "type": "float"},
        {"name": "merchantCity", "type": "string"},
        {"name": "merchantState", "type": "string"},
        {"name": "merchantZip", "type": "string"},
        {"name": "cardPresent", "type": "boolean"},
        {"name": "posOnPremises", "type": "string"},
        {"name": "recurringAuthInd", "type": "string"},
        {"name": "expirationDateKeyInMatch", "type": "boolean"},
        {"name": "isFraud", "type": "boolean"},
        {"name": "somenewRecord", "type": "string"}
    ]
}

# Convert schema to JSON string and escape double quotes
escaped_schema = json.dumps({"schema": json.dumps(schema)})

print(escaped_schema)