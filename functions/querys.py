## Query departamentos por Q
quarter_departments = '''
SELECT 
    department
    ,job
    ,CASE WHEN Q=1 THEN qty else 0 end as Q1
    ,CASE WHEN Q=2 THEN qty else 0 end as Q2
    ,CASE WHEN Q=3 THEN qty else 0 end as Q3
    ,CASE WHEN Q=4 THEN qty else 0 end as Q4
FROM (
	SELECT distinct
		d.department AS department
        ,j.job AS job
		,DATEPART(QUARTER, datetime) as Q
		,COUNT(*) AS qty
	FROM dbo.hired_employees e
	inner join dbo.jobs j
		ON e.job_id = j.id
	inner join dbo.departments d
		ON e.department_id = d.id
	WHERE year(e.datetime) = 2021
    GROUP BY job, department, DATEPART(QUARTER, datetime)
) AS basequery ORDER BY department
'''

## Query promedio mayor departamentos
mean_departments = '''
WITH base_query AS (

 SELECT distinct
        d.id as id
		,d.department AS department
		,COUNT(name) AS qty
	FROM dbo.hired_employees e
	inner join dbo.departments d
		ON e.department_id = d.id
    WHERE year(e.datetime) = 2021
    GROUP BY d.id, d.department
)
SELECT  *
from base_query
where qty > (select avg(qty) from base_query)
'''