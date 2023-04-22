select 
    brewery_type, 
    state,
    count(*) qty
 from tb_brewery 
group by 
    brewery_type, 
    state 
order by 
    brewery_type, 
    qty desc