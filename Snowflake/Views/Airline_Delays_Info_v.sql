Create Or Replace view Public.Airline_Delays_Info_v As
with t_a
as
(select count(*) as departure_delay_count,airline
from flights
where nvl(departure_delay,-1) >= 0
group by airline),
t_b as
(select count(*) as arrival_delay_count,airline 
from flights
where nvl(try_cast(arrival_delay as integer),-1) >= 0
group by airline)
select t_a.airline,
       ar.airline as Airline_Name,
       t_a.departure_delay_count + t_b.arrival_delay_count  as Max_Delays 
from t_a,
     t_b,
     public.airlines ar
where t_a.airline = t_b.airline
and   t_a.airline = ar.iata_code
order by Max_Delays desc;


grant select on Airline_Delays_Info_v to public;