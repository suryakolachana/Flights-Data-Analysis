Create Or Replace view Public.On_Time_Airline_Info_v As
with t_a
as
(SELECT count(*) on_time_flight_count,airline
FROM public.FLIGHTS
WHERE YEAR = 2015
and nvl(departure_delay,1) <= 0
and nvl(try_cast(arrival_delay as integer),1) <= 0
group by airline),
t_b as
(SELECT count(*) total_flight_count,airline
FROM public.FLIGHTS
WHERE YEAR = 2015
group by airline)
select t_a.airline,
       ar.airline as airline_name,
       round(on_time_flight_count/total_flight_count*100,2)||'%' as Ontime_Flight_Percentage
from t_a,
     t_b,
     public.airlines ar
where t_a.airline = t_b.airline
and t_a.airline = ar.iata_code
order by Ontime_Flight_Percentage desc;

Grant Select on On_Time_Airline_Info_v To Public;