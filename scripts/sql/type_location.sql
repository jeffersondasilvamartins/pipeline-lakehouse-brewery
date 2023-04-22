select 
    brewery_type, 
    state,
    count(*) qtd  
 from tb_brewery 
group by 
    brewery_type, 
    state 
order by 
    brewery_type, 
    qtd desc