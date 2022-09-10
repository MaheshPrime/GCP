MERGE <project_id>.<dataset>.Final_table T
USING (
  SELECT *,
case
when upper(FileName) like 'CHICOS%' then 'CHS'
when upper(FileName) like 'COTR%' then 'CHS'
when upper(FileName) like  'SOMA%' then 'SOMA'
when upper(FileName) like 'WHBM%' then 'WHBM'
END 
as Brand,
 case
when upper(FileName) like 'COTR%' then 'OTR'
else 'DCOM'
END
 as Channel
from  
  <project_id>.<dataset>.STAGING_TABLE) S
ON T.FileName = S.FileName

WHEN NOT MATCHED THEN
  INSERT(COL1, COL2, COL3,FileName,Brand,Channel)
  VALUES(COL1, COL2, COL3,FileName,Brand,Channel)