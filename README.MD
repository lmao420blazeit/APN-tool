# Annual Price Negotiation Tool

This APN tool was developed using Python and PL SQL to access SAP Oracle database for the purpose of preparing the Annual Price Negotiation master data.

### How it works

-> create master_data.xlsx
-> format
| Material  |
|-----------|
| XXXXXXXXX |
| YYYYYYYYY |
| ...       |

-> fill the appropriate data

USER -> str: username of the oracle database
PASSWORD -> str: password
DSN -> str: server connection string (data source name)
org_view -> str: database view
sap_machine_list -> list(str): list of strings with SAP machines

-> run the script
| LookupKey | Plant | Material Number | Quote | Payment Terms | N10A | SAP Supplier Plant | Supplier | Incoterms | Consignment | Current Price | Currency | SAPSYS |
|-----------|-------|-----------------|-------|---------------|------|--------------------|----------|-----------|-------------|---------------|----------|--------|
|           |       |                 |       |               |      |                    |          |           |             |               |          |        |
|           |       |                 |       |               |      |                    |          |           |             |               |          |        |
|           |       |                 |       |               |      |                    |          |           |             |               |          |        |
