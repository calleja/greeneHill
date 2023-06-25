show tables;

select * from mem_type limit 5;

show fields from mem_type;
show fields from mem_status;

select type from mem_type group by 1 order by 1;

select type_clean 
from mem_type 
group by 1 
order by 1;

drop table mem_type;
drop table mem_status;

-- historical trial starts