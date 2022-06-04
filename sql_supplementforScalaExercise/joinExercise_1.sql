select first_name, last_name ,max(salary) as max_salary
from employees e, salaries s 
where e.emp_no = s.emp_no 
group by e.emp_no 
order by max_salary
desc;