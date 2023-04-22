select 
    latitude, 
    longitude, 
    brewery_type, 
    count(*) qtd  
 from tb_brewery 
group by 
    latitude, 
    longitude, 
    brewery_type
order by 
    brewery_type, 
    qtd desc