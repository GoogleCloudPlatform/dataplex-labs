SELECT *,   
to_date(dt,'yyyy-MM-dd') as ingest_date
FROM __table__