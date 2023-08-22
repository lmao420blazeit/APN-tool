import cx_Oracle
import pandas as pd
import time
import pandas as pd

conn = cx_Oracle.connect(user='SII3BRG', password='TBS1DEG_Pcl8fxp', dsn='REDLake_ZeusP_Consumer_DALI.world')
data = pd.read_excel("master_data.xlsx")

data.update("'" + data["Material Bosch"].astype(str) + "'")
matnrs = data["Material Bosch"].astype(str).unique().tolist()
matnrs = (", ".join(matnrs))


cursor = conn.cursor()
 
             
def query_generator(matnrs): 
        data = pd.DataFrame()  
        sap_machine_list = ["p47", "p45", "p72", "p79", "p81", "p87", "p99", "POE", "PS0"]
        for _sys in sap_machine_list:
                # PROBLEMA 
                # CRIAR UM PROCEDIMENTO EM PL SQL QUE FAÇA LOOP POR TODAS AS MAQUINAS SAP                
                new_query2 = """
                        SELECT 
                                EQUK.WERKS as Plant,
                                EQUK.MATNR as "Material Number",
                                EQUP.QUOTE as Quote,
                                LFA1.LIFNR,
                                LFA1.NAME1 as Supplier,
                                MARC.EKGRP,
                                EQUP.QUMNG as "Allocated Quantity",
                                '{_sys}' as SAPSYS
                        FROM MARD_DALI_BBM.EQUK_{_sys} EQUK
                        RIGHT JOIN(
                            SELECT 
                                MATNR, 
                                WERKS,
                                EKGRP
                            FROM MARD_DALI_BBM.MARC_{_sys}
                        ) MARC
                        ON (EQUK.MATNR = MARC.MATNR AND EQUK.WERKS = MARC.WERKS)
                        RIGHT JOIN(
                                SELECT 
                                        QUNUM, 
                                        QUOTE,
                                        QUMNG,
                                        LIFNR
                                FROM MARD_DALI_BBM.EQUP_{_sys} 
                                
                        ) EQUP
                        ON (EQUK.QUNUM = EQUP.QUNUM)
                        RIGHT JOIN(
                                SELECT
                                        EBELN,
                                        WERKS, 
                                        MATNR,
                                        MATKL,
                                        LOEKZ
                                FROM MARD_DALI_BBM.EKPO_{_sys}
                        ) EKPO
                        ON (EQUK.WERKS = EKPO.WERKS AND EQUK.MATNR = EKPO.MATNR)
                        RIGHT JOIN(
                                SELECT 
                                        EBELN, 
                                        ZTERM,
                                        KDATB,
                                        KDATE,
                                        LIFNR
                                FROM MARD_DALI_BBM.EKKO_{_sys}
                        ) EKKO
                        ON (EKPO.EBELN = EKKO.EBELN AND EQUP.LIFNR = EKKO.LIFNR)
                        LEFT JOIN(
                                SELECT 
                                        LIFNR, 
                                        NAME1
                                FROM MARD_DALI_BBM.LFA1_{_sys}) LFA1  
                        ON (EKKO.LIFNR = LFA1.LIFNR)
                        WHERE EQUK.VDATU < TO_CHAR (sysdate, 'YYYYMMDD') 
                                AND EQUK.BDATU > TO_CHAR (sysdate, 'YYYYMMDD') 
                                AND EQUP.QUOTE <> '0'
                                AND EKKO.KDATB < TO_CHAR (sysdate, 'YYYYMMDD') 
                                AND EKKO.KDATE > TO_CHAR (sysdate, 'YYYYMMDD') 
                                AND EKPO.LOEKZ = ' '
                                AND (SUBSTR(MARC.EKGRP,1, 2) = '4B' OR (SUBSTR(MARC.EKGRP, 1, 2) = '4P'))
                """.format(_sys = _sys)

                _data = pd.read_sql(new_query2, conn)
                data = data.append(_data, ignore_index=True)
                time.sleep(1)

        data = data.drop_duplicates()
        data = data.replace({'CONSIGNMENT': {"0": "False", "1": "False", "2": "True"}})
        print(data)
        data = data.groupby(["EKGRP", "SAPSYS", "PLANT", "Material Number", "SUPPLIER"])["QUOTE"].sum().reset_index()
        data.to_excel("download//APN" + str(time.time()) + ".xlsx")
        return (data)

if __name__ == '__main__':
    query_generator(matnrs)

"""

app = Dash(__name__)

app.layout = dash_table.DataTable(df.to_dict('records'), [{"name": i, "id": i} for i in df.columns])

if __name__ == '__main__':
    app.run(debug=True)

#Purchase Requisition Number, Short Text, Material Number, WERKS, MENGE, Purchasing Document Number, Order Type (Purchasing), ZTERM, ZBD1T, Incoterms (Part 1), Price unit, Number of purchasing info record


        
DESCRIPTION:
Get active SA's for each part number and
        
Select A.MATNR as PN, to_number(C.LIFNR) as VENDOR, E.Name1 as Supplier, C.SOBES as SpecialProc, to_number(D.EBELN) as SA, G.BSART as Agreement_Type, A.BDATU as SourcingDateEnd, D.BDATU as SAEnd
FROM MARD_DALI_BBM.EQUP_P45 C -- QUOTA FILE ITEM (SOBES = SPECIAL PROCUREMENT TYPE, LIFNR = SUPPLIER CODE)
RIGHT JOIN (
        SELECT QUNUM, MATNR, BDATU, VDATU, WERKS -- QUOTA NUM, MAT NUM, VALIDITY END, VALIDITY START, PLANT
        FROM MARD_DALI_BBM.EQUK_P45) A -- QUOTA FILE HEADER
ON (C.QUNUM = A.QUNUM)
RIGHT JOIN (
        SELECT DISPO, MATNR, WERKS        
        FROM MARD_DALI_BBM.MARC_P45) B -- PLANT MATERIAL DATA      (MRP CONTROLLER, MATNUM, PLANT)    
ON (B.MATNR = A.MATNR AND A.WERKS=B.WERKS)
LEFT JOIN (
        SELECT LIFNR, NAME1
        FROM MARD_DALI_BBM.LFA1_P45) E      -- VENDOR MASTER GENERAL (SUPPLIER NUMBER, NAME)
ON (C.LIFNR = E.LIFNR)
RIGHT JOIN (
        select Matnr , VDATU , BDATU , LIFNR, EKORG , EBELN , AUTET, werks    
        FROM MARD_DALI_BBM.EORD_P45) D    -- PURCHASE SOURCE LIST (MAT NUM, VALIDITY START, VALIDITY END, PUR ORG, PUR DO NUMBER, MATERIAL USAGE?? AUTET, PLANT)
on (D.matnr=A.matnr and A.WERKS=D.WERKS and C.LIFNR=D.LIFNR)
RIGHT JOIN (
        select matnr, EBELN , LGORT, LOEKZ, KTMNG, PSTYP , LGBZO , BSTAE , RB04_YL1_LEBKZ, STPAC
        from MARD_DALI_BBM.EKPO_P45) F    -- PURCHASING DOC ITEM LIST ( MAT NUM,  PUR ORDER, STORAGE LOCATION, DELETION STATUS, CONFIRMATION KEY, CATEGORY PUR DOC, LEB FLAG
ON (F.EBELN =D.EBELN and A.matnr=f.matnr)
LEFT JOIN (
        SELECT BSART, EBELN, LIFNR, ZBD1T   
        FROM MARD_DALI_BBM.EKKO_P45) G    -- PURCHASING DOC HEADER (PUR DOC TYPE, PUR ORDER, SUPPLIER NUMBER, CASH DISCOUNT DAYS
ON (G.EBELN =D.EBELN and C.LIFNR=G.LIFNR)
WHERE A.VDATU<TO_CHAR (sysdate, 'YYYYMMDD')  -- RFQC VALIDITY STARTED; RFQC VALIDITY NOT ENDED; PUR DOC VALIDITY STARTED; PUR DOC VALIDITY NOT ENDED; SUPPLIER QUOTE <> 0; PUR DOC STATUS = ''
        and A.bdatu>TO_CHAR (sysdate, 'YYYYMMDD')
        AND D.VDATU<TO_CHAR (sysdate, 'YYYYMMDD')  
        and D.bdatu>TO_CHAR (sysdate, 'YYYYMMDD') 
        and C.QUOTE <>'0'
        AND F.LOEKZ = ' ' 
   
   
NEW QUERY     
        
-- create data parsing function
-- LOEKZ: L SA apagada, nao tem atividade; S bloquear SA por alguma razao, nao pode ser usado; se não tiver nada está OK
SELECT ekp.EMATN as PN, ekp.LOEKZ as Status, ekk.EBELN as SANR, ekp.MATKL as MatGrp, ekk.EKGRP as PurGrp, ekk.EKORG as PurOrg, ekk.KDATB as ValStart, ekk.LIFNR as Supplier, lfa.NAME1, ekk.BSART as AgrType, ekk.ZTERM as PTerm, ekk.ZBD1T as PDesc, ekk.WAERS as Currency, ekk.INCO1 as IncoT, ekk.INCO2 as IncoDesc
FROM MARD_DALI_BBM.EKKO_P45 ekk
LEFT JOIN (
        SELECT NAME1, LIFNR
        FROM MARD_DALI_BBM.LFA1_P45) lfa
ON lfa.LIFNR = ekk.LIFNR
LEFT JOIN (
        SELECT EMATN, MATKL, EBELN, LOEKZ
        FROM MARD_DALI_BBM.EKPO_P45) ekp
ON ekk.EBELN = ekp.EBELN
WHERE ekp.EMATN IN ('%s')
        
        
        
NEW QUERY

select A.matnr as PN, A.sobkz as Special_Stock, to_number(A.lifnr) as Supplier, A.bwart as Movement_Type, sum(A.menge) as quantity
From mard_dali_bbm.mseg_P45 A
right join (
        select mblnr, budat -- MATERIAL DOCUMENT HEADER (DOCUMENT KEY, DOCUMENT DATE)
        from mard_dali_bbm.MKPF_P45 ) B        
On (B.mblnr=A.mblnr)
where   bwart in('101','102')
        and b.budat>=" & validfrom & "
        and b.budat<=" & validto & "
        And a.lifnr in (" & vendor & ") 
        and A.sobkz in ('K')
group by A.matnr,A.sobkz, A.lifnr, A.bwart

NEW QUERY 

SELECT  to_number(C.EBELN) AS PurchDoc,A.MATNR as Material, TO_NUMBER(B.LIFNR) as Vendor,  SUM(C.menge-C.wemng) AS OPEN_QUANTITY
FROM MARD_DALI_BBM.EKET_P45 C
RIGHT JOIN (
        SELECT EBELN, BSART, EKORG, LOEKZ, LIFNR , BUKRS   
        FROM MARD_DALI_BBM.EKKO_P45) B 
ON (C.EBELN = B.EBELN)
right JOIN (
        SELECT EBELN, EBELP, WERKS, BSTAE, MATNR
        FROM MARD_DALI_BBM.EKPO_P45) A
ON (A.EBELN = C.EBELN AND A.EBELP = C.EBELP)
LEFT JOIN (SELECT MATNR, DISMM, WERKS, DISPO
FROM MARD_DALI_BBM.MARC_P45) D        
ON (A.MATNR = D.MATNR AND A.WERKS = D.WERKS  AND B.BUKRS=A.WERKS)
WHERE B.BSART  in ('LPA')
AND (B.EKORG = '4991' OR B.EKORG = 'ZEW3')  
AND B.LOEKZ = ' '  
AND b.BUKRS = '8150'  
And B.LIFNR in (" & vendor & ") 
AND C.EINDT <= " & validto & "
C.menge-C.wemng>0
GROUP BY C.EBELN, A.MATNR,B.LIFNR, D.WERKS

"let" & Chr(13) & "" & Chr(10) & "    
Source = Oracle.Database(""
        REDLake_ZeusP_Consumer_DALI.world"", 
        [Query=""SELECT  to_number(C.EBELN) AS PurchDoc,A.MATNR as Material, TO_NUMBER(B.LIFNR) as Vendor,  SUM(C.menge-C.wemng) AS OPEN_QUANTITY
        #(lf)FROM MARD_DALI_BBM.EKET_P45 C
        #(lf)RIGHT JOIN (
                # SELECT EBELN, BSART, EKORG, LOEKZ, LIFNR , BUKRS   
                #(lf)FROM MARD_DALI_BBM.EKK" & _ "O_P45) B
                #(lf)          ON (C.EBELN = B.EBELN)
        #(lf)right JOIN (
                SELECT EBELN, EBELP, WERKS, BSTAE, MATNR
        # #(lf) FROM MARD_DALI_BBM.EKPO_P45) A#(lf)          
        ON (A.EBELN = C.EBELN AND A.EBELP = C.EBELP)
        #(lf)LEFT JOIN (SELECT MATNR, DISMM, WERKS, DISPO#(lf)         FROM MARD_DALI_BBM.MARC_P45) D#(lf)          ON (A.MATNR = D.MATNR AND A.WERKS = D.WERKS  AND B.BUKR" & _
"S=A.WERKS)#(lf)WHERE B.BSART  in ('LPA') #(lf)  AND (B.EKORG = '4991' OR B.EKORG = 'ZEW3')  AND B.LOEKZ = ' '#(lf)  AND b.BUKRS = '8150'#(lf)  AND " & PN & "')#(lf) And B.LIFNR in (" & vendor & ") AND C.EINDT <= " & validto & "#(lf)  and C.menge-C.wemng>0#(lf) GROUP BY C.EBELN, A.MATNR,B.LIFNR, D.WERKS"",HierarchicalNavigation=true])" & Chr(13) & "" & Chr(10) & "in" & Chr(13) & "" & Chr(10) & "    Source" & _
""

                        SELECT  
                                EKKO.BSART, 
                                EKKO.EBELN, 
                                EKKO.LIFNR, 
                                EKKO.EKGRP, 
                                EKPO.WERKS, 
                                EKPO.MATNR, 
                                LFA1.NAME1,
                                EKET.MENGE,
                                EKET.EINDT
                        FROM MARD_DALI_BBM.EKKO_{_sys} EKKO    -- PURCHASING DOC HEADER (PUR DOC TYPE, PUR ORDER, SUPPLIER NUMBER, CASH DISCOUNT DAYS
                        RIGHT JOIN (
                                SELECT EBELN, LOEKZ, WERKS, MATNR, INFNR
                                FROM MARD_DALI_BBM.EKPO_{_sys}) EKPO   -- PURCHASING DOC ITEM
                        ON (EKKO.EBELN = EKPO.EBELN)
                        LEFT JOIN (
                                SELECT LIFNR, NAME1
                                FROM MARD_DALI_BBM.LFA1_{_sys}) LFA1  
                        ON (EKKO.LIFNR = LFA1.LIFNR) -- VENDOR MASTER GENERAL (SUPPLIER NUMBER, NAME)                                
                        RIGHT JOIN(
                                SELECT MENGE, EINDT, EBELN
                                FROM MARD_DALI_BBM.EKET_{_sys}
                        ) EKET
                        ON (EKKO.EBELN = EKET.EBELN)
                        WHERE  (SUBSTR(EKKO.EKGRP, 0, 2) = '4B' or SUBSTR(EKKO.EKGRP, 0, 2) = '4P')
                                AND EKPO.LOEKZ = ' ' 
                                AND EKET.MENGE <> '0'
                                AND EKET.EINDT > TO_CHAR (sysdate, 'YYYYMMDD') 
                                AND EKKO.BSART  in ('LPA')
                                AND " & PN & "
        """