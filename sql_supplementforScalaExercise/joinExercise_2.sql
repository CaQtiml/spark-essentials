select *
from titles t , employees e 
where t.emp_no = e.emp_no 
and t.title <> 'Manager';