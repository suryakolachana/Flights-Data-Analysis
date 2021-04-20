Create Or Replace view Public.Airport_Flight_Cancellations_Info_v As
with t_a as
(select distinct origin_airport as airport,
                case 
                when cancellation_reason =  'A' then 'Airline/Carrier'
                when cancellation_reason =  'B' then 'Weather'
                when cancellation_reason =  'C' then 'National Air System'
                when cancellation_reason =  'D' then 'Security'
                End as Cancellation_Reasons
 from flights
 where cancellation_reason <> ''
 order by origin_airport)
 select t_a.airport,
        ap.airport as Airport_Name,
        Ap.city||' - '||Ap.State||', '||Ap.Country as Airport_Location,
        listagg(distinct t_a.Cancellation_Reasons, ' | ') within group(order by t_a.Cancellation_Reasons asc) as airport_Flight_Cancellation_Reasons
 from t_a,
      airports ap
 where t_a.airport = ap.iata_code
 group by t_a.airport,
          ap.airport,
          Ap.city||' - '||Ap.State||', '||Ap.Country 
 order by t_a.airport;
 
Grant Select on Airport_Flight_Cancellations_Info_v To Public;