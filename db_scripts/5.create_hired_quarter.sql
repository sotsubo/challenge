create or replace view hired_quarter as 
select 
department,c.job,QUARTER(dt_hired) as quarter, count(name) as hired from hired_employees a inner join departments b
on a.department_id =b.id 
inner join jobs c
on a.job_id =c.id 
where YEAR(a.dt_hired) =2021 and a.department_id is not null
group by b.department,c.job
;