Create Or Replace View Public.Airport_Delay_Reasons_Info_V as
with t_a
as
(SELECT origin_airport,
       destination_airport,
       departure_delay,
       arrival_delay,
       air_system_delay,
       security_delay,
       airline_delay,
       late_aircraft_delay,
       weather_delay
FROM public.FLIGHTS
WHERE YEAR = 2015
and (nvl(departure_delay,1) > 0 or nvl(try_cast(arrival_delay as integer),1) > 0)
and air_system_delay is not null 
and security_delay is not null 
and airline_delay is not null 
and late_aircraft_delay is not null 
and weather_delay is not null)
select a.airport,
       ap.airport as airport_name,
       Ap.city||' - '||Ap.State||', '||Ap.Country as Airport_Location,
       a.delay_reasons
from
(select distinct airport,listagg(distinct Delay_Reasons, ' | ') within group(order by Delay_Reasons asc) as Delay_Reasons
from
(select case 
       when departure_delay > arrival_delay Then origin_airport 
       when arrival_delay > departure_delay Then destination_airport 
       when arrival_delay = departure_delay Then origin_airport 
       End as airport,
       case 
            when air_system_delay    <> 0      and security_delay   = 0 and airline_delay = 0 and late_aircraft_delay = 0 and weather_delay = 0  Then 'Air System Delay'
            when air_system_delay    <> 0      and security_delay   <> 0 and airline_delay = 0 and late_aircraft_delay = 0 and weather_delay = 0  Then 'Air System Delay, Security Delay'
            when air_system_delay    <> 0      and security_delay   <> 0 and airline_delay <> 0 and late_aircraft_delay = 0 and weather_delay = 0  Then 'Air System Delay, Security Delay, Airline Delay'
            when air_system_delay    <> 0      and security_delay   <> 0 and airline_delay <> 0 and late_aircraft_delay <> 0 and weather_delay = 0  Then 'Air System Delay, Security Delay, Airline Delay, Late Aircraft Delay,'
            when air_system_delay    <> 0      and security_delay   <> 0 and airline_delay <> 0 and late_aircraft_delay <> 0 and weather_delay <> 0  Then 'Air System Delay, Security Delay, Airline Delay, Late Aircraft Delay, Weather Delay'
            when security_delay      <> 0      and air_system_delay = 0 and airline_delay = 0 and late_aircraft_delay = 0 and weather_delay = 0  Then 'Security Delay'
            when security_delay      <> 0      and air_system_delay = 0 and airline_delay <> 0 and late_aircraft_delay = 0 and weather_delay = 0  Then 'Security Delay, Airline Delay'
            when security_delay      <> 0      and air_system_delay = 0 and airline_delay = 0 and late_aircraft_delay <> 0 and weather_delay = 0  Then 'Security Delay, Late Aircraft Delay'
            when security_delay      <> 0      and air_system_delay = 0 and airline_delay = 0 and late_aircraft_delay = 0 and weather_delay <> 0  Then 'Security Delay, Weather Delay'
            when security_delay      <> 0      and air_system_delay = 0 and airline_delay <> 0 and late_aircraft_delay <> 0 and weather_delay = 0  Then 'Security Delay, Airline Delay, Late Aircraft Delay'
            when security_delay      <> 0      and air_system_delay = 0 and airline_delay <> 0 and late_aircraft_delay <> 0 and weather_delay <> 0  Then 'Security Delay, Airline Delay, Late Aircraft Delay, Weather Delay'
            when airline_delay       <> 0      and air_system_delay = 0 and security_delay = 0 and late_aircraft_delay = 0 and weather_delay = 0 Then 'Airline Delay'
            when airline_delay       <> 0      and air_system_delay <> 0 and security_delay = 0 and late_aircraft_delay = 0 and weather_delay = 0 Then 'Air System Delay, Airline Delay'
            when airline_delay       <> 0      and air_system_delay <> 0 and security_delay = 0 and late_aircraft_delay = 0 and weather_delay <> 0 Then 'Air System Delay, Airline Delay, Weather Delay'
            when airline_delay       <> 0      and air_system_delay = 0 and security_delay = 0 and late_aircraft_delay <> 0 and weather_delay = 0 Then 'Airline Delay, Late Aircraft Delay'
            when airline_delay       <> 0      and air_system_delay = 0 and security_delay = 0 and late_aircraft_delay = 0 and weather_delay <> 0 Then 'Airline Delay, Weather Delay'
            when airline_delay       <> 0      and air_system_delay = 0 and security_delay = 0 and late_aircraft_delay <> 0 and weather_delay <> 0 Then 'Airline Delay, Late Aircraft Delay, Weather Delay'
            when airline_delay       <> 0      and air_system_delay = 0 and security_delay <> 0 and late_aircraft_delay = 0 and weather_delay <> 0 Then 'Security Delay, Airline Delay, Weather Delay'
            when late_aircraft_delay <> 0      and air_system_delay = 0 and security_delay = 0 and airline_delay = 0 and weather_delay = 0       Then 'Late Aircraft Delay'
            when late_aircraft_delay <> 0      and air_system_delay <> 0 and security_delay = 0 and airline_delay = 0 and weather_delay = 0       Then 'Air System Delay, Late Aircraft Delay'
            when late_aircraft_delay <> 0      and air_system_delay = 0 and security_delay = 0 and airline_delay = 0 and weather_delay <> 0       Then 'Late Aircraft Delay, Weather Delay'
            when late_aircraft_delay <> 0      and air_system_delay <> 0 and security_delay <> 0 and airline_delay = 0 and weather_delay = 0       Then 'Air System Delay, Security Delay, Late Aircraft Delay'
            when late_aircraft_delay <> 0      and air_system_delay = 0 and security_delay = 0 and airline_delay <> 0 and weather_delay <> 0       Then 'Late Aircraft Delay, Airline Delay, Weather Delay'
            when late_aircraft_delay <> 0      and air_system_delay <> 0 and security_delay = 0 and airline_delay = 0 and weather_delay <> 0       Then 'Air System Delay, Late Aircraft Delay, Weather Delay'
            when late_aircraft_delay <> 0      and air_system_delay <> 0 and security_delay = 0 and airline_delay <> 0 and weather_delay = 0       Then 'Air System Delay, Airline Delay, Late Aircraft Delay'
            when late_aircraft_delay <> 0      and air_system_delay <> 0 and security_delay = 0 and airline_delay <> 0 and weather_delay <> 0       Then 'Air System Delay, Airline Delay, Late Aircraft Delay, Weather Delay'
            when weather_delay       <> 0      and air_system_delay = 0 and security_delay = 0 and airline_delay = 0 and late_aircraft_delay = 0 Then 'Weather Delay'
            when weather_delay       <> 0      and air_system_delay <> 0 and security_delay = 0 and airline_delay = 0 and late_aircraft_delay = 0 Then 'Air System Delay, Weather Delay'
            when weather_delay       <> 0      and air_system_delay <> 0 and security_delay <> 0 and airline_delay = 0 and late_aircraft_delay = 0 Then 'Air System Delay, Weather Delay, Security Delay'
            when weather_delay       <> 0      and air_system_delay = 0 and security_delay <> 0 and airline_delay <> 0 and late_aircraft_delay = 0 Then 'Security Delay, Airline Delay, Weather Delay'
            when weather_delay       <> 0      and air_system_delay = 0 and security_delay <> 0 and airline_delay = 0 and late_aircraft_delay <> 0 Then 'Security Delay, Late Aircraft Delay, Weather Delay'
            when weather_delay       <> 0      and air_system_delay <> 0 and security_delay <> 0 and airline_delay <> 0 and late_aircraft_delay = 0 Then 'Air System Delay, Security Delay, Airline Delay, Weather Delay'
       End as Delay_Reasons
from t_a) 
group by airport) as a,
airports ap
where try_cast(a.airport as string) = try_cast(ap.iata_code as string)
order by airport;

Grant Select on Airport_Delay_Reasons_Info_V To Public;