select t.emp_no , t.title ,max(s.salary) as max_salary
from salaries s , titles t
where s.emp_no = t.emp_no 
group by t.emp_no, t.title
order by max(s.salary)
desc
limit 10;