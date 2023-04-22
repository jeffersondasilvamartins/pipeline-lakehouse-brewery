select 
    latitude, 
    longitude, 
    brewery_type, 
    count(*) qty
 from tb_brewery 
group by 
    latitude, 
    longitude, 
    brewery_type
order by 
    brewery_type, 
    qty desc