# Annual Price Negotiation Tool

This APN tool was developed using Python and PL SQL to access SAP Oracle database for the purpose of preparing the Annual Price Negotiation master data.

## Tool description

The program loops through every SAP System (currently only suports systems with this scheme) and gets the quotas and scheduling agreements available for each material number and plant combination. It makes an evaluation about the quota and SA validity by checking if the start dates < today and end date > today to filter only active documents. 

~~~~sql
-- EQUK: Quota File: Header
-- EKKO: Purhcasing Document: Header
WHERE EQUK.VDATU < TO_CHAR (sysdate, 'YYYYMMDD') -- Quota start data < current date
AND EQUK.BDATU > TO_CHAR (sysdate, 'YYYYMMDD') -- Quote end date > current date
AND EKKO.KDATB < TO_CHAR (sysdate, 'YYYYMMDD') -- SA start date < current date
AND EKKO.KDATE > TO_CHAR (sysdate, 'YYYYMMDD') -- SA end date > current date
~~~~

For each (MATERIAL NUMBER && PLANT NUMBER) find the active SA NUMBER from the Purchasing Document Header. Merge the Purchasing Document Header with the Purchasing Document Item with the SA NUMBER (EBELN key).

- It also checks for documents blocked in the system or marked for deletion. 
~~~~sql
AND EKPO.LOEKZ = ' '
/*
EKPO-LOEKZ = 'X' when deleted,
EKPO-LOEKZ = 'L' when blocked.
*/
~~~~
- The plant supplier plant, incoterms, payment terms and currency are retrieved from each active SA and the name of the supplier from the vendor master table. The N10A and consignment from the quota files.
- It finally wraps up everything into a single file.


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
-> output file: /download/APN(...).xlsx

| LookupKey | Plant | Material Number | Quote | Payment Terms | N10A | SAP Supplier Plant | Supplier | Incoterms | Consignment | Current Price | Currency | SAPSYS |
|-----------|-------|-----------------|-------|---------------|------|--------------------|----------|-----------|-------------|---------------|----------|--------|
|           |       |                 |       |               |      |                    |          |           |             |               |          |        |
|           |       |                 |       |               |      |                    |          |           |             |               |          |        |
|           |       |                 |       |               |      |                    |          |           |             |               |          |        |

# Issues

- Quota item is not %, depending on how it's maintained on SAP, it can either have a Sum <> 100 (SUM QUOTA(for each (ACTIVE SA))) <> 100.
