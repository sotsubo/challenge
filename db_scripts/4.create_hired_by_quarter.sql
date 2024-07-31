create view hired_by_quarter or replace as 
select department, job, 
                SUM(CASE when quarter ='1' then hired end) as Q1, 
                SUM(CASE when quarter ='2' then hired end) as Q2, 
                SUM(CASE when quarter ='3' then hired end) as Q3, 
                SUM(CASE when quarter ='4' then hired end )as Q4 
        from hired_quarter group by department,job
        ;