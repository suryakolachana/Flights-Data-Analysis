Create Or Replace View Public.Airlines_Unique_Route_Info_V As
with t_a
as
(select  distinct airline
               ,origin_airport
               ,listagg(distinct destination_airport, ', ') within group(order by destination_airport) as unique_routes
               ,count(distinct destination_airport) as unique_count
from flights
group by airline,origin_airport
order by airline)
select a.airline,
       ar.airline as Airline_Name,
       listagg(a.unique_routes_1, ' | ')within group(order by a.unique_routes_1) as Most_Unique_Routes,
       sum(a.unique_route_count) as Count_Of_Most_Unique_Routes
from
(select t_a.airline,
       t_a.origin_airport||' - '||listagg(distinct t_a.unique_routes, ', ') within group(order by t_a.unique_routes) as unique_routes_1,
       sum(t_a.unique_count) as unique_route_count
from t_a
group by t_a.airline, t_a.origin_airport) as a,
public.airlines ar
where a.airline = ar.iata_code
group by a.airline, ar.airline
order by Count_Of_Most_Unique_Routes desc;

Grant Select on Airlines_Unique_Route_Info_V To Public;